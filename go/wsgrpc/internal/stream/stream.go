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
	ctx           context.Context
	cancel        context.CancelFunc
	streamID      uint32
	method        string // full gRPC method path, e.g. "/echo.v1.Echo/Echo"
	sink          Sink
	RecvCh        chan []byte // fed by conn.Conn on MSG arrival; exported for test setup
	closeRecvOnce sync.Once
	sentEnd       sync.Once  // ensures exactly one END frame is sent per stream
	header        metadata.MD
	trailer       metadata.MD
	headerSent    bool
	mu            sync.Mutex
}

var _ grpc.ServerStream = (*Stream)(nil) // compile-time interface check

// DefaultRecvBufSize is the per-stream inbound message buffer used when no
// explicit size is configured. 64 slots × up to 4 MiB per message = 256 MiB
// worst-case burst capacity before the stream is terminated.
const DefaultRecvBufSize = 64

func New(ctx context.Context, cancel context.CancelFunc, id uint32, method string, recvBufSize int, sink Sink) *Stream {
	if recvBufSize <= 0 {
		recvBufSize = DefaultRecvBufSize
	}
	return &Stream{
		ctx:      ctx,
		cancel:   cancel,
		streamID: id,
		method:   method,
		sink:     sink,
		RecvCh:   make(chan []byte, recvBufSize),
	}
}

// Method returns the full gRPC method path from the BEGIN frame.
func (s *Stream) Method() string { return s.method }

// Deliver enqueues an inbound MSG payload. Returns false if the buffer is full,
// in which case the caller is responsible for terminating the stream.
// Non-blocking: never stalls the connection read loop.
func (s *Stream) Deliver(payload []byte) bool {
	select {
	case s.RecvCh <- payload:
		return true
	default:
		return false
	}
}

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
// Idempotent: only the first call sends a frame; subsequent calls are no-ops.
// This allows both the read loop (on buffer overflow) and the dispatch layer
// (after the handler returns) to call SendEnd without sending duplicate frames.
func (s *Stream) SendEnd(err error) error {
	var writeErr error
	s.sentEnd.Do(func() {
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
			writeErr = merr
			return
		}
		writeErr = s.sink.WriteFrame(frame.TypeEND, s.streamID, payload)
	})
	return writeErr
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
