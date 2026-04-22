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
	"github.com/grpcws/wsgrpc/frame"
	"github.com/grpcws/wsgrpc/internal/stream"
)

// Conn manages one WebSocket connection and the streams multiplexed over it.
type Conn struct {
	ws        *websocket.Conn
	streams   sync.Map     // map[uint32]*stream.Stream
	highestID atomic.Uint32
	writeMu   sync.Mutex
	log       *slog.Logger

	// OnStream is called in a new goroutine for every incoming BEGIN frame.
	// Week 3 replaces this with gRPC service dispatch.
	// If nil, incoming streams are accepted but no handler runs.
	OnStream func(s *stream.Stream)
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
	// Enforce monotonically increasing stream IDs (spec §5.1).
	for {
		highest := c.highestID.Load()
		if id <= highest {
			_ = c.ws.Close(websocket.StatusProtocolError, "non-monotonic stream_id")
			return fmt.Errorf("conn: stream_id %d not monotonically increasing (highest: %d)", id, highest)
		}
		if c.highestID.CompareAndSwap(highest, id) {
			break
		}
	}

	sCtx, cancel := context.WithCancel(ctx)
	s := stream.New(sCtx, cancel, id, c)
	c.streams.Store(id, s)

	go func() {
		defer c.streams.Delete(id)
		if c.OnStream != nil {
			c.OnStream(s)
		}
	}()
	return nil
}

func (c *Conn) handleMSG(f frame.Frame) error {
	s, ok := c.loadStream(f.StreamID)
	if !ok {
		_ = c.ws.Close(websocket.StatusProtocolError, "MSG for unknown stream")
		return fmt.Errorf("conn: MSG for unknown stream_id %d", f.StreamID)
	}
	s.Deliver(f.Payload)
	return nil
}

func (c *Conn) handleEND(f frame.Frame) error {
	s, ok := c.loadStream(f.StreamID)
	if !ok {
		_ = c.ws.Close(websocket.StatusProtocolError, "END for unknown stream")
		return fmt.Errorf("conn: END for unknown stream_id %d", f.StreamID)
	}
	s.CloseRecv() // signals EOF to the handler's RecvMsg
	return nil
}

func (c *Conn) handleRESET(f frame.Frame) error {
	s, ok := c.loadStream(f.StreamID)
	if !ok {
		// RESET for unknown stream is silently dropped per spec §4.4.
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
		s.CloseRecv()
		s.Cancel()
		return true
	})
}
