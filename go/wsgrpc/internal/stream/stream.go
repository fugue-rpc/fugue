package stream

import (
	"context"
	"errors"
	"io"
	"sync"

	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

// Frame type constants. Duplicated here to avoid importing the frame package
// before it exists; will be replaced by frame.TypeXxx constants in a later commit.
const (
	typHEADER = uint8(0x06)
	typMSG    = uint8(0x02)
	typEND    = uint8(0x03)
)

// Sink is the outbound write interface. Implemented by conn.Conn in production
// and by a recording double in tests.
type Sink interface {
	WriteFrame(typ uint8, streamID uint32, payload []byte) error
}

// Stream is the per-RPC stream object. It implements grpc.ServerStream.
type Stream struct {
	ctx        context.Context
	cancel     context.CancelFunc
	streamID   uint32
	sink       Sink
	RecvCh     chan []byte // fed by conn.Conn on MSG arrival; exported for test setup
	header     metadata.MD
	trailer    metadata.MD
	headerSent bool
	mu         sync.Mutex
}

var _ grpc.ServerStream = (*Stream)(nil) // compile-time interface check

func New(ctx context.Context, cancel context.CancelFunc, id uint32, sink Sink) *Stream {
	return &Stream{
		ctx:      ctx,
		cancel:   cancel,
		streamID: id,
		sink:     sink,
		RecvCh:   make(chan []byte, 64),
	}
}

// Deliver enqueues an inbound MSG payload. Called by conn.Conn on MSG frame arrival.
func (s *Stream) Deliver(payload []byte) { s.RecvCh <- payload }

// CloseRecv signals EOF to the handler. Called on client half-close (END) or RESET.
func (s *Stream) CloseRecv() { close(s.RecvCh) }

// Cancel cancels the stream context. Called on RESET or WebSocket close.
func (s *Stream) Cancel() { s.cancel() }

// Trailer returns the trailing metadata captured by SetTrailer.
// Called by the dispatch layer after the handler returns.
func (s *Stream) Trailer() metadata.MD {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.trailer.Copy()
}

// SendEnd writes an END frame encoding the gRPC status and captured trailers.
// Called by the dispatch layer after the handler returns.
// TODO: encode trailers into EndPayload proto once the frame package exists.
func (s *Stream) SendEnd(err error) error {
	st, _ := status.FromError(err)
	code := uint32(st.Code())
	payload := []byte{byte(code >> 24), byte(code >> 16), byte(code >> 8), byte(code)}
	return s.sink.WriteFrame(typEND, s.streamID, payload)
}

// --- grpc.ServerStream interface ---

func (s *Stream) Context() context.Context { return s.ctx }

func (s *Stream) SetHeader(md metadata.MD) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.headerSent {
		return errors.New("wsgrpc: headers already sent")
	}
	s.header = metadata.Join(s.header, md)
	return nil
}

func (s *Stream) SendHeader(md metadata.MD) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.headerSent {
		return errors.New("wsgrpc: headers already sent")
	}
	s.header = metadata.Join(s.header, md)
	return s.flushHeaderLocked()
}

func (s *Stream) SetTrailer(md metadata.MD) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.trailer = metadata.Join(s.trailer, md)
}

func (s *Stream) SendMsg(m any) error {
	s.mu.Lock()
	if !s.headerSent {
		if err := s.flushHeaderLocked(); err != nil {
			s.mu.Unlock()
			return err
		}
	}
	s.mu.Unlock()
	b, err := proto.Marshal(m.(proto.Message))
	if err != nil {
		return err
	}
	return s.sink.WriteFrame(typMSG, s.streamID, b)
}

func (s *Stream) RecvMsg(m any) error {
	payload, ok := <-s.RecvCh
	if !ok {
		return io.EOF
	}
	return proto.Unmarshal(payload, m.(proto.Message))
}

// flushHeaderLocked sends a HEADER frame. Called with s.mu held.
// TODO: encode s.header into HeaderPayload proto once the frame package exists.
func (s *Stream) flushHeaderLocked() error {
	if err := s.sink.WriteFrame(typHEADER, s.streamID, nil); err != nil {
		return err
	}
	s.headerSent = true
	return nil
}
