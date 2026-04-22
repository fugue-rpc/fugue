package stream

import (
	"context"
	"errors"
	"io"
	"sync"

	framev1 "github.com/grpcws/wsgrpc/grpcws/frame/v1"
	"github.com/grpcws/wsgrpc/frame"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

// Sink is the outbound write interface. Implemented by conn.Conn in production
// and by a recording double in tests.
type Sink interface {
	WriteFrame(typ uint8, streamID uint32, payload []byte) error
}

// Stream is the per-RPC stream object. It implements grpc.ServerStream.
type Stream struct {
	ctx          context.Context
	cancel       context.CancelFunc
	streamID     uint32
	sink         Sink
	RecvCh       chan []byte // fed by conn.Conn on MSG arrival; exported for test setup
	closeRecvOnce sync.Once
	header       metadata.MD
	trailer      metadata.MD
	headerSent   bool
	mu           sync.Mutex
}

var _ grpc.ServerStream = (*Stream)(nil) // compile-time interface check

func New(ctx context.Context, cancel context.CancelFunc, id uint32, sink Sink) *Stream {
	return &Stream{
		ctx:      ctx,
		cancel:   cancel,
		streamID: id,
		sink:     sink,
		RecvCh:   make(chan []byte, 16),
	}
}

// Deliver enqueues an inbound MSG payload. Called by conn.Conn on MSG frame arrival.
func (s *Stream) Deliver(payload []byte) { s.RecvCh <- payload }

// CloseRecv signals EOF to the handler. Called on client half-close (END), RESET, or
// WebSocket close. Safe to call multiple times.
func (s *Stream) CloseRecv() { s.closeRecvOnce.Do(func() { close(s.RecvCh) }) }

// Cancel cancels the stream context. Called on RESET or WebSocket close.
func (s *Stream) Cancel() { s.cancel() }

// Trailer returns the trailing metadata captured by SetTrailer.
func (s *Stream) Trailer() metadata.MD {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.trailer.Copy()
}

// SendEnd writes an END frame encoding the gRPC status and captured trailers.
func (s *Stream) SendEnd(err error) error {
	st, _ := status.FromError(err)
	trailers := make(map[string]string)
	for k, vs := range s.Trailer() {
		if len(vs) > 0 {
			trailers[k] = vs[0]
		}
	}
	payload, merr := proto.Marshal(&framev1.EndPayload{
		StatusCode:    uint32(st.Code()),
		StatusMessage: st.Message(),
		Trailers:      trailers,
	})
	if merr != nil {
		return merr
	}
	return s.sink.WriteFrame(frame.TypeEND, s.streamID, payload)
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
	return s.sink.WriteFrame(frame.TypeMSG, s.streamID, b)
}

func (s *Stream) RecvMsg(m any) error {
	payload, ok := <-s.RecvCh
	if !ok {
		return io.EOF
	}
	return proto.Unmarshal(payload, m.(proto.Message))
}

// flushHeaderLocked sends a HEADER frame carrying s.header as HeaderPayload.
// Called with s.mu held.
func (s *Stream) flushHeaderLocked() error {
	headers := make(map[string]string)
	for k, vs := range s.header {
		if len(vs) > 0 {
			headers[k] = vs[0]
		}
	}
	payload, err := proto.Marshal(&framev1.HeaderPayload{Headers: headers})
	if err != nil {
		return err
	}
	if err := s.sink.WriteFrame(frame.TypeHEADER, s.streamID, payload); err != nil {
		return err
	}
	s.headerSent = true
	return nil
}
