// Package fugue implements a gRPC-over-WebSocket server.
// Handlers registered via RegisterService are called with standard
// grpc.ServerStream / context.Context — no handler changes required.
package fugue

import (
	"log/slog"
	"net/http"
	"sync"

	"github.com/coder/websocket"
	"github.com/fugue-rpc/fugue-go/internal/conn"
	"github.com/fugue-rpc/fugue-go/internal/stream"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

// Server accepts WebSocket connections and dispatches gRPC streams to
// registered service handlers. It implements http.Handler and
// grpc.ServiceRegistrar.
type Server struct {
	mu          sync.RWMutex
	methods     map[string]methodEntry
	origins     []string
	log         *slog.Logger
	recvBufSize int
	maxStreams   int
	codec       Codec // nil → default proto
}

type methodEntry struct {
	impl      any
	unary     grpc.MethodHandler  // non-nil for unary RPCs
	streaming grpc.StreamHandler  // non-nil for streaming RPCs
}

var (
	_ http.Handler        = (*Server)(nil)
	_ grpc.ServiceRegistrar = (*Server)(nil)
)

// Option configures a Server.
type Option func(*Server)

// WithOrigins sets the allowed WebSocket origins. When set, connections from
// other origins are rejected with 403. When not set, all origins are accepted
// (useful for local dev; not suitable for production).
func WithOrigins(origins ...string) Option {
	return func(s *Server) { s.origins = append(s.origins, origins...) }
}

// WithLogger sets the logger used for connection-level events.
func WithLogger(l *slog.Logger) Option {
	return func(s *Server) { s.log = l }
}

// WithStreamRecvBuffer sets the per-stream inbound message buffer depth.
// When a stream's buffer fills, it is terminated with RESOURCE_EXHAUSTED so
// slow handlers never block the connection read loop. Default: 64.
func WithStreamRecvBuffer(n int) Option {
	return func(s *Server) { s.recvBufSize = n }
}

// WithMaxConcurrentStreams limits the number of simultaneously open streams
// per connection. Streams beyond the limit receive an immediate END with
// RESOURCE_EXHAUSTED; the connection itself stays alive. Default: unlimited.
func WithMaxConcurrentStreams(n int) Option {
	return func(s *Server) { s.maxStreams = n }
}

// NewServer creates a new Server.
func NewServer(opts ...Option) *Server {
	s := &Server{
		methods: make(map[string]methodEntry),
		log:     slog.Default(),
	}
	for _, o := range opts {
		o(s)
	}
	return s
}

// RegisterService registers a service and its implementation.
// The desc and impl are the same types passed to a standard grpc.Server.
func (s *Server) RegisterService(desc *grpc.ServiceDesc, impl any) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, m := range desc.Methods {
		path := "/" + desc.ServiceName + "/" + m.MethodName
		s.methods[path] = methodEntry{impl: impl, unary: m.Handler}
	}
	for _, m := range desc.Streams {
		path := "/" + desc.ServiceName + "/" + m.StreamName
		s.methods[path] = methodEntry{impl: impl, streaming: m.Handler}
	}
}

// ServeHTTP upgrades the HTTP connection to WebSocket and serves gRPC streams.
func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	opts := &websocket.AcceptOptions{}
	if len(s.origins) > 0 {
		opts.OriginPatterns = s.origins
	} else {
		opts.InsecureSkipVerify = true
	}
	ws, err := websocket.Accept(w, r, opts)
	if err != nil {
		s.log.Debug("websocket accept failed", "err", err)
		return
	}
	c := conn.New(ws, s.log)
	c.OnStream = s.onStream
	c.RecvBufSize = s.recvBufSize
	c.MaxStreams = s.maxStreams
	if err := c.Serve(r.Context()); err != nil {
		s.log.Debug("conn closed", "err", err)
	}
}

func (s *Server) onStream(str *stream.Stream) {
	s.mu.RLock()
	entry, ok := s.methods[str.Method()]
	codec := s.codec
	s.mu.RUnlock()

	if !ok {
		_ = str.SendEnd(status.Errorf(codes.Unimplemented, "unknown method %s", str.Method()))
		return
	}

	if codec != nil {
		str.SetCodec(codec)
	}

	var err error
	if entry.unary != nil {
		err = s.dispatchUnary(str, entry, codec)
	} else {
		err = entry.streaming(entry.impl, str)
	}
	_ = str.SendEnd(err)
}

func (s *Server) dispatchUnary(str *stream.Stream, entry methodEntry, codec Codec) error {
	// Wait for the single request message.
	var payload []byte
	select {
	case p, ok := <-str.RecvCh:
		if !ok {
			return status.Error(codes.Internal, "client half-closed before sending request")
		}
		payload = p
	case <-str.Context().Done():
		return status.Error(codes.Canceled, "stream cancelled before request arrived")
	}

	var dec func(m any) error
	if codec != nil {
		dec = func(m any) error { return codec.Unmarshal(payload, m) }
	} else {
		dec = func(m any) error { return proto.Unmarshal(payload, m.(proto.Message)) }
	}
	resp, err := entry.unary(entry.impl, str.Context(), dec, nil)
	if err != nil {
		return err
	}
	return str.SendMsg(resp)
}
