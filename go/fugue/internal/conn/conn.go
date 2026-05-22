// Package conn manages a single fugue WebSocket connection.
// It multiplexes gRPC streams over the connection and serialises writes.
package conn

import (
	"context"
	"encoding/binary"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"

	"github.com/coder/websocket"
	framev1 "github.com/fugue-rpc/fugue/grpcws/frame/v1"
	"github.com/fugue-rpc/fugue/frame"
	"github.com/fugue-rpc/fugue/internal/stream"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

// frameBufPool recycles the per-frame encode buffer to avoid a heap alloc on
// every WriteFrame call. Safe because ws.Write is synchronous — the buffer is
// not referenced after Write returns.
var frameBufPool = sync.Pool{
	New: func() any {
		b := make([]byte, 0, frame.HeaderSize+256)
		return &b
	},
}

// writeItem is a single encoded frame waiting to be sent by the writer goroutine.
// done is nil for fire-and-forget frames (MSG); non-nil for frames that require
// delivery confirmation before the caller unblocks (HEADER, END).
type writeItem struct {
	buf  *[]byte      // pooled; returned to frameBufPool after ws.Write
	done chan<- error // nil → fire-and-forget; non-nil → synchronous write
}

// Conn manages one WebSocket connection and the streams multiplexed over it.
type Conn struct {
	ws          *websocket.Conn
	streams     sync.Map // map[uint32]*stream.Stream
	highestID   uint32   // only written by Serve's single read loop, no lock needed
	writeQueue  chan writeItem
	connClosed  chan struct{} // closed when Serve is shutting down
	log         *slog.Logger

	// OnStream is called in a new goroutine for every incoming BEGIN frame.
	// If nil, incoming streams are accepted but no handler runs.
	OnStream func(s *stream.Stream)

	// Performance limits — set before calling Serve.
	RecvBufSize int // per-stream inbound buffer depth (0 → stream.DefaultRecvBufSize)
	MaxStreams  int // max concurrent streams (0 → unlimited)

	activeStreams         atomic.Int32
	estimatedPayloadSize atomic.Uint32 // EMA of recent MSG payload sizes for buffer pre-growth
}

func New(ws *websocket.Conn, log *slog.Logger) *Conn {
	if log == nil {
		log = slog.Default()
	}
	// Raise the WebSocket read limit to match our MaxPayloadSize (4 MiB payload
	// + 9-byte header). The coder/websocket default of 32 KiB would otherwise
	// reject legitimate large messages before our own size check fires.
	ws.SetReadLimit(int64(frame.MaxPayloadSize) + int64(frame.HeaderSize))
	return &Conn{ws: ws, log: log}
}

// Serve runs the read loop until the connection closes or ctx is cancelled.
// It starts a writer goroutine before entering the read loop and waits for
// it to drain before cancelling streams.
func (c *Conn) Serve(ctx context.Context) error {
	// Size the queue to 2×MaxStreams (minimum 128) so concurrent sends rarely
	// stall waiting for the writer goroutine to catch up.
	queueSize := 128
	if c.MaxStreams > 0 && 2*c.MaxStreams > queueSize {
		queueSize = 2 * c.MaxStreams
	}
	c.writeQueue = make(chan writeItem, queueSize)
	c.connClosed = make(chan struct{})

	writerDone := make(chan struct{})
	go func() {
		defer close(writerDone)
		// coalesceBuf accumulates multiple frames for a single ws.Write call.
		// Pooled separately; not returned to pool if it grew beyond 256 KiB.
		coalesceBuf := make([]byte, 0, 64*1024)
		var pending []writeItem

		flush := func() {
			if len(pending) == 0 {
				return
			}
			err := c.ws.Write(context.Background(), websocket.MessageBinary, coalesceBuf)
			for _, item := range pending {
				frameBufPool.Put(item.buf)
				if item.done != nil {
					item.done <- err
				}
			}
			// Reset for next batch; discard oversized backing arrays.
			if cap(coalesceBuf) > 256*1024 {
				coalesceBuf = make([]byte, 0, 64*1024)
			} else {
				coalesceBuf = coalesceBuf[:0]
			}
			pending = pending[:0]
		}

		for item := range c.writeQueue {
			coalesceBuf = append(coalesceBuf, *item.buf...)
			pending = append(pending, item)
			// Non-blocking drain: batch additional queued frames up to budget.
		drain:
			for len(coalesceBuf) < 64*1024 && len(pending) < 32 {
				select {
				case next, ok := <-c.writeQueue:
					if !ok {
						break drain
					}
					coalesceBuf = append(coalesceBuf, *next.buf...)
					pending = append(pending, next)
				default:
					break drain
				}
			}
			flush()
		}
	}()

	defer func() {
		// Signal WriteFrame callers that the connection is gone, then drain the
		// queue before cancelling streams so any buffered END frames can be flushed.
		close(c.connClosed)
		close(c.writeQueue)
		<-writerDone
		c.cancelAllStreams()
	}()

	for {
		_, msg, err := c.ws.Read(ctx)
		if err != nil {
			return err
		}
		frames, err := frame.DecodeAll(msg)
		if err != nil {
			_ = c.ws.Close(websocket.StatusProtocolError, "bad frame")
			return fmt.Errorf("conn: frame decode: %w", err)
		}
		for _, f := range frames {
			if err := c.dispatch(ctx, f); err != nil {
				return err
			}
		}
	}
}

func (c *Conn) dispatch(ctx context.Context, f frame.Frame) error {
	switch f.Type {
	case frame.TypeBEGIN:
		return c.handleBEGIN(ctx, f)
	case frame.TypeMSG:
		return c.handleMSG(f)
	case frame.TypeEND:
		return c.handleEND(f)
	case frame.TypeRESET:
		return c.handleRESET(f)
	default:
		_ = c.ws.Close(websocket.StatusProtocolError,
			fmt.Sprintf("unknown frame type 0x%02x", f.Type))
		return fmt.Errorf("conn: unknown frame type 0x%02x", f.Type)
	}
}

func (c *Conn) handleBEGIN(ctx context.Context, f frame.Frame) error {
	id := f.StreamID
	if id == 0 {
		_ = c.ws.Close(websocket.StatusProtocolError, "stream_id 0 is reserved")
		return fmt.Errorf("conn: stream_id 0 is reserved")
	}
	// highestID is ONLY read and written inside handleBEGIN, which is always
	// called from the single Serve read goroutine. No other goroutine touches
	// it, so no synchronization is needed.
	if id <= c.highestID {
		_ = c.ws.Close(websocket.StatusProtocolError, "non-monotonic stream_id")
		return fmt.Errorf("conn: stream_id %d not monotonically increasing (highest: %d)", id, c.highestID)
	}
	c.highestID = id

	// Decode BeginPayload before taking any resource slots.
	var bp framev1.BeginPayload
	if len(f.Payload) > 0 {
		if err := proto.Unmarshal(f.Payload, &bp); err != nil {
			_ = c.ws.Close(websocket.StatusProtocolError, "bad BeginPayload")
			return fmt.Errorf("conn: bad BeginPayload on stream %d: %w", id, err)
		}
	}

	// Check concurrent stream limit before allocating any resources.
	if c.MaxStreams > 0 && int(c.activeStreams.Add(1)) > c.MaxStreams {
		c.activeStreams.Add(-1)
		endPayload, _ := proto.Marshal(&framev1.EndPayload{
			StatusCode:    uint32(codes.ResourceExhausted),
			StatusMessage: "too many concurrent streams",
		})
		// Fire-and-forget: do not block the read loop waiting for delivery.
		c.enqueue(c.encodeFrame(frame.TypeEND, id, endPayload), nil)
		return nil
	}

	if len(bp.Metadata) == 0 {
		// Fast path: avoid allocating an empty map; gRPC treats nil MD identically.
		sCtx, cancel := context.WithCancel(metadata.NewIncomingContext(ctx, nil))
		s := stream.New(sCtx, cancel, id, bp.Method, c.RecvBufSize, c)
		c.streams.Store(id, s)
		c.startHandler(id, s)
		return nil
	}

	md := make(metadata.MD, len(bp.Metadata))
	for k, v := range bp.Metadata {
		md[k] = []string{v}
	}
	sCtx, cancel := context.WithCancel(metadata.NewIncomingContext(ctx, md))
	s := stream.New(sCtx, cancel, id, bp.Method, c.RecvBufSize, c)
	c.streams.Store(id, s)
	c.startHandler(id, s)
	return nil
}

func (c *Conn) startHandler(id uint32, s *stream.Stream) {
	go func() {
		defer func() {
			c.streams.Delete(id)
			if c.MaxStreams > 0 {
				c.activeStreams.Add(-1)
			}
		}()
		if c.OnStream != nil {
			c.OnStream(s)
		}
	}()
}

// handleMSG silently drops frames for unknown/closed streams. A MSG arriving
// after the handler returned is an in-flight race, not a protocol error.
// If the stream's inbound buffer is full, the stream is terminated with
// RESOURCE_EXHAUSTED to preserve stream independence.
func (c *Conn) handleMSG(f frame.Frame) error {
	s, ok := c.loadStream(f.StreamID)
	if !ok {
		return nil
	}
	if !s.Deliver(f.Payload) {
		_ = s.SendEnd(status.Error(codes.ResourceExhausted, "inbound message buffer full"))
		s.CloseRecv()
		s.Cancel()
		c.streams.Delete(f.StreamID)
	}
	return nil
}

// handleEND silently drops frames for unknown/closed streams for the same
// reason as handleMSG.
func (c *Conn) handleEND(f frame.Frame) error {
	s, ok := c.loadStream(f.StreamID)
	if !ok {
		return nil
	}
	s.CloseRecv()
	return nil
}

func (c *Conn) handleRESET(f frame.Frame) error {
	s, ok := c.loadStream(f.StreamID)
	if !ok {
		// RESET for unknown/closed stream is silently dropped per spec §4.4.
		return nil
	}
	s.CloseRecv()
	s.Cancel()
	c.streams.Delete(f.StreamID)
	return nil
}

func (c *Conn) loadStream(id uint32) (*stream.Stream, bool) {
	v, ok := c.streams.Load(id)
	if !ok {
		return nil, false
	}
	return v.(*stream.Stream), true
}

// WriteFrame implements stream.Sink.
//
// For TypeMSG frames the call is fire-and-forget: the frame is enqueued and
// WriteFrame returns immediately. If the connection has already closed, the
// frame is silently dropped (consistent with gRPC streaming semantics — there
// is no per-message delivery guarantee).
//
// For TypeHEADER and TypeEND frames WriteFrame blocks until the frame has been
// written to the wire (or the connection closes), preserving the invariant that
// SendHeader and SendEnd do not return before the remote peer can see the frame.
func (c *Conn) WriteFrame(typ uint8, streamID uint32, payload []byte) error {
	if uint32(len(payload)) > frame.MaxPayloadSize {
		return fmt.Errorf("frame: payload %d bytes exceeds MAX_PAYLOAD_SIZE", len(payload))
	}
	buf := c.encodeFrame(typ, streamID, payload)

	if typ == frame.TypeMSG {
		c.enqueue(buf, nil) // fire-and-forget; errors are silently dropped
		return nil
	}

	// HEADER and END: synchronous — block until the writer goroutine delivers.
	done := make(chan error, 1)
	if err := c.enqueue(buf, done); err != nil {
		return err
	}
	return <-done
}

// WriteMsgProto is the zero-copy fast path for MSG frames. It marshals msg
// directly into a pooled frame buffer, eliminating one heap allocation and one
// memcopy compared to the WriteFrame(TypeMSG) path.
//
// Recognised by stream.SendMsg via a type assertion (stream.protoMsgWriter).
func (c *Conn) WriteMsgProto(streamID uint32, msg proto.Message) error {
	bp := frameBufPool.Get().(*[]byte)
	b := (*bp)[:0]
	// Reserve the 9-byte header placeholder; fill it after marshalling so we
	// know the exact payload length.
	b = append(b, 0, 0, 0, 0, 0, 0, 0, 0, 0)
	// Pre-grow to estimated payload size to avoid reallocation on MarshalAppend.
	est := int(c.estimatedPayloadSize.Load())
	if est == 0 {
		est = 256
	}
	if cap(b) < frame.HeaderSize+est {
		grown := make([]byte, frame.HeaderSize, frame.HeaderSize+est)
		copy(grown, b[:frame.HeaderSize])
		b = grown
	}
	// Marshal proto bytes directly after the header — zero intermediate alloc
	// when the buffer has enough capacity.
	var err error
	b, err = proto.MarshalOptions{}.MarshalAppend(b, msg)
	if err != nil {
		frameBufPool.Put(bp)
		return err
	}
	*bp = b

	payloadLen := uint32(len(b) - frame.HeaderSize)
	// Backfill the 9-byte header now that payload length is known.
	b[0] = frame.TypeMSG
	binary.BigEndian.PutUint32(b[1:5], streamID)
	binary.BigEndian.PutUint32(b[5:9], payloadLen)

	// Update EMA: newEst ≈ (7×old + actual) / 8; clamped to [256, 4096].
	if payloadLen > 0 {
		for {
			old := c.estimatedPayloadSize.Load()
			newEst := (old*7 + payloadLen) / 8
			if newEst < 256 {
				newEst = 256
			} else if newEst > 4096 {
				newEst = 4096
			}
			if c.estimatedPayloadSize.CompareAndSwap(old, newEst) {
				break
			}
		}
	}

	c.enqueue(bp, nil)
	return nil
}

// encodeFrame allocates a pooled buffer and encodes the 9-byte header + payload.
func (c *Conn) encodeFrame(typ uint8, streamID uint32, payload []byte) *[]byte {
	bp := frameBufPool.Get().(*[]byte)
	b := (*bp)[:0]
	b = append(b, typ)
	b = binary.BigEndian.AppendUint32(b, streamID)
	b = binary.BigEndian.AppendUint32(b, uint32(len(payload)))
	b = append(b, payload...)
	*bp = b
	return bp
}

// enqueue sends buf to the write queue. If the connection is closing, the
// buffer is returned to the pool, done (if non-nil) receives an error, and
// the error is returned to the caller. For fire-and-forget callers (done==nil)
// the return value can be ignored.
//
// The recover() guard handles the rare race where the select picks the
// writeQueue send case in the same instant that the queue is closed by Serve's
// deferred cleanup — in that case Go panics on send-to-closed-channel.
func (c *Conn) enqueue(buf *[]byte, done chan<- error) (retErr error) {
	defer func() {
		if r := recover(); r != nil {
			frameBufPool.Put(buf)
			err := fmt.Errorf("fugue: connection closed")
			if done != nil {
				done <- err
			}
			retErr = err
		}
	}()
	select {
	case c.writeQueue <- writeItem{buf: buf, done: done}:
		return nil
	case <-c.connClosed:
		frameBufPool.Put(buf)
		err := fmt.Errorf("fugue: connection closed")
		if done != nil {
			done <- err
		}
		return err
	}
}

func (c *Conn) cancelAllStreams() {
	c.streams.Range(func(_, value any) bool {
		s := value.(*stream.Stream)
		s.CloseRecv() // safe: guarded by sync.Once inside Stream
		s.Cancel()
		return true
	})
}
