// Package conn manages a single grpcws WebSocket connection.
// It multiplexes gRPC streams over the connection and serialises writes.
package conn

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"

	"github.com/coder/websocket"
	framev1 "github.com/grpcws/wsgrpc/grpcws/frame/v1"
	"github.com/grpcws/wsgrpc/frame"
	"github.com/grpcws/wsgrpc/internal/stream"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

// Conn manages one WebSocket connection and the streams multiplexed over it.
type Conn struct {
	ws        *websocket.Conn
	streams   sync.Map // map[uint32]*stream.Stream
	highestID uint32   // only written by Serve's single read loop, no lock needed
	writeMu   sync.Mutex
	log       *slog.Logger

	// OnStream is called in a new goroutine for every incoming BEGIN frame.
	// If nil, incoming streams are accepted but no handler runs.
	OnStream func(s *stream.Stream)

	// Performance limits — set before calling Serve.
	RecvBufSize int // per-stream inbound buffer depth (0 → stream.DefaultRecvBufSize)
	MaxStreams  int // max concurrent streams (0 → unlimited)

	activeStreams atomic.Int32
}

func New(ws *websocket.Conn, log *slog.Logger) *Conn {
	if log == nil {
		log = slog.Default()
	}
	return &Conn{ws: ws, log: log}
}

// Serve runs the read loop until the connection closes or ctx is cancelled.
// It cancels all open streams when it returns.
func (c *Conn) Serve(ctx context.Context) error {
	defer c.cancelAllStreams()
	for {
		_, msg, err := c.ws.Read(ctx)
		if err != nil {
			return err
		}
		f, err := frame.Decode(msg)
		if err != nil {
			_ = c.ws.Close(websocket.StatusProtocolError, "bad frame")
			return fmt.Errorf("conn: frame decode: %w", err)
		}
		if err := c.dispatch(ctx, f); err != nil {
			return err
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
	// Serve is single-goroutine so highestID is safe without a lock.
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
		_ = c.WriteFrame(frame.TypeEND, id, endPayload)
		return nil
	}

	md := make(metadata.MD, len(bp.Metadata))
	for k, v := range bp.Metadata {
		md[k] = []string{v}
	}
	sCtx, cancel := context.WithCancel(metadata.NewIncomingContext(ctx, md))
	s := stream.New(sCtx, cancel, id, bp.Method, c.RecvBufSize, c)
	c.streams.Store(id, s)

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
	return nil
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

// WriteFrame implements stream.Sink. Encodes f and writes it to the WebSocket.
// Safe for concurrent use; writes are serialised under writeMu.
func (c *Conn) WriteFrame(typ uint8, streamID uint32, payload []byte) error {
	encoded, err := frame.Encode(frame.Frame{Type: typ, StreamID: streamID, Payload: payload})
	if err != nil {
		return err
	}
	c.writeMu.Lock()
	defer c.writeMu.Unlock()
	return c.ws.Write(context.Background(), websocket.MessageBinary, encoded)
}

func (c *Conn) cancelAllStreams() {
	c.streams.Range(func(_, value any) bool {
		s := value.(*stream.Stream)
		s.CloseRecv() // safe: guarded by sync.Once inside Stream
		s.Cancel()
		return true
	})
}
