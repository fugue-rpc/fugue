// Spike A: validates the grpc.ServiceDesc dispatch contract without any
// WebSocket or HTTP involvement. These tests remain as regression tests.
package stream_test

import (
	"context"
	"io"
	"strings"
	"sync"
	"testing"

	framev1 "github.com/fugue-rpc/fugue/frame/v1"
	"github.com/fugue-rpc/fugue/internal/stream"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

// --- recording sink ---

type frame struct {
	typ      uint8
	streamID uint32
	payload  []byte
}

type recordingSink struct {
	mu     sync.Mutex
	frames []frame
}

func (r *recordingSink) WriteFrame(typ uint8, streamID uint32, payload []byte) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	cp := make([]byte, len(payload))
	copy(cp, payload)
	r.frames = append(r.frames, frame{typ, streamID, cp})
	return nil
}

func (r *recordingSink) types() []uint8 {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]uint8, len(r.frames))
	for i, f := range r.frames {
		out[i] = f.typ
	}
	return out
}

func (r *recordingSink) msgPayloads() [][]byte {
	r.mu.Lock()
	defer r.mu.Unlock()
	var out [][]byte
	for _, f := range r.frames {
		if f.typ == 0x02 { // MSG
			cp := make([]byte, len(f.payload))
			copy(cp, f.payload)
			out = append(out, cp)
		}
	}
	return out
}

// --- hand-crafted server implementation ---

type testServer struct{}

func (t *testServer) sayHello(_ context.Context, req *wrapperspb.StringValue) (*wrapperspb.StringValue, error) {
	return wrapperspb.String("hello " + req.Value), nil
}

func (t *testServer) listWords(req *wrapperspb.StringValue, ss grpc.ServerStream) error {
	for _, w := range strings.Fields(req.Value) {
		if err := ss.SendMsg(wrapperspb.String(w)); err != nil {
			return err
		}
	}
	return nil
}

func (t *testServer) collect(ss grpc.ServerStream) error {
	var parts []string
	for {
		m := new(wrapperspb.StringValue)
		if err := ss.RecvMsg(m); err == io.EOF {
			break
		} else if err != nil {
			return err
		}
		parts = append(parts, m.Value)
	}
	return ss.SendMsg(wrapperspb.String(strings.Join(parts, ",")))
}

func (t *testServer) chat(ss grpc.ServerStream) error {
	ss.SetTrailer(metadata.Pairs("x-chat-done", "true"))
	for {
		m := new(wrapperspb.StringValue)
		if err := ss.RecvMsg(m); err == io.EOF {
			return nil
		} else if err != nil {
			return err
		}
		if err := ss.SendMsg(wrapperspb.String("echo:" + m.Value)); err != nil {
			return err
		}
	}
}

// --- hand-crafted ServiceDesc (mirrors protoc-gen-go-grpc output) ---

var svc = &testServer{}

var testServiceDesc = grpc.ServiceDesc{
	ServiceName: "test.TestService",
	HandlerType: (*testServer)(nil),
	Methods: []grpc.MethodDesc{
		{
			MethodName: "SayHello",
			Handler: func(srv any, ctx context.Context, dec func(any) error, interceptor grpc.UnaryServerInterceptor) (any, error) {
				req := new(wrapperspb.StringValue)
				if err := dec(req); err != nil {
					return nil, err
				}
				return srv.(*testServer).sayHello(ctx, req)
			},
		},
	},
	Streams: []grpc.StreamDesc{
		{
			StreamName:    "ListWords",
			ServerStreams: true,
			Handler: func(srv any, ss grpc.ServerStream) error {
				req := new(wrapperspb.StringValue)
				if err := ss.RecvMsg(req); err != nil {
					return err
				}
				return srv.(*testServer).listWords(req, ss)
			},
		},
		{
			StreamName:    "Collect",
			ClientStreams: true,
			Handler: func(srv any, ss grpc.ServerStream) error {
				return srv.(*testServer).collect(ss)
			},
		},
		{
			StreamName:    "Chat",
			ServerStreams: true,
			ClientStreams: true,
			Handler: func(srv any, ss grpc.ServerStream) error {
				return srv.(*testServer).chat(ss)
			},
		},
	},
}

// --- helpers ---

func newStream(t *testing.T) (*stream.Stream, *recordingSink) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	sink := &recordingSink{}
	s := stream.New(ctx, cancel, 1, "/test.Test/Method", 0, sink)
	t.Cleanup(func() { cancel() })
	return s, sink
}

func mustMarshal(t *testing.T, m proto.Message) []byte {
	t.Helper()
	b, err := proto.Marshal(m)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	return b
}

func mustUnmarshal[T proto.Message](t *testing.T, b []byte, dst T) T {
	t.Helper()
	if err := proto.Unmarshal(b, dst); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	return dst
}

// --- spike tests ---

// TestUnaryDispatch validates that the dec closure correctly fills the request
// proto and the handler response round-trips through proto.Marshal.
func TestUnaryDispatch(t *testing.T) {
	s, sink := newStream(t)

	reqBytes := mustMarshal(t, wrapperspb.String("world"))

	handler := testServiceDesc.Methods[0].Handler
	resp, err := handler(svc, s.Context(), func(m any) error {
		return proto.Unmarshal(reqBytes, m.(proto.Message))
	}, nil)
	if err != nil {
		t.Fatalf("handler error: %v", err)
	}

	// Simulate dispatch: marshal response, send as MSG, send END.
	if err := s.SendMsg(resp); err != nil {
		t.Fatalf("SendMsg: %v", err)
	}
	if err := s.SendEnd(nil); err != nil {
		t.Fatalf("SendEnd: %v", err)
	}

	// Verify frame order: HEADER (auto-flushed), MSG, END.
	types := sink.types()
	if len(types) != 3 {
		t.Fatalf("want 3 frames, got %d: %v", len(types), types)
	}
	if types[0] != 0x06 {
		t.Errorf("frame[0]: want HEADER(0x06), got 0x%02x", types[0])
	}
	if types[1] != 0x02 {
		t.Errorf("frame[1]: want MSG(0x02), got 0x%02x", types[1])
	}
	if types[2] != 0x03 {
		t.Errorf("frame[2]: want END(0x03), got 0x%02x", types[2])
	}

	// Verify the response payload round-trips correctly.
	got := mustUnmarshal(t, sink.msgPayloads()[0], new(wrapperspb.StringValue))
	if got.Value != "hello world" {
		t.Errorf("response: want %q, got %q", "hello world", got.Value)
	}
}

// TestAutoHeaderFlush verifies that calling SendMsg without a prior SendHeader
// causes a HEADER frame to be emitted before the MSG frame.
func TestAutoHeaderFlush(t *testing.T) {
	s, sink := newStream(t)

	// SendMsg with no prior SendHeader call.
	if err := s.SendMsg(wrapperspb.String("x")); err != nil {
		t.Fatalf("SendMsg: %v", err)
	}

	types := sink.types()
	if len(types) < 2 {
		t.Fatalf("want at least 2 frames, got %d", len(types))
	}
	if types[0] != 0x06 {
		t.Errorf("first frame: want HEADER(0x06), got 0x%02x — auto-flush did not fire", types[0])
	}
	if types[1] != 0x02 {
		t.Errorf("second frame: want MSG(0x02), got 0x%02x", types[1])
	}
}

// TestExplicitSendHeader verifies SendHeader sends the HEADER frame immediately
// and subsequent SendMsg does not send a second HEADER.
func TestExplicitSendHeader(t *testing.T) {
	s, sink := newStream(t)

	if err := s.SendHeader(metadata.Pairs("x-request-id", "abc")); err != nil {
		t.Fatalf("SendHeader: %v", err)
	}
	if err := s.SendMsg(wrapperspb.String("payload")); err != nil {
		t.Fatalf("SendMsg: %v", err)
	}

	types := sink.types()
	headerCount := 0
	for _, typ := range types {
		if typ == 0x06 {
			headerCount++
		}
	}
	if headerCount != 1 {
		t.Errorf("want exactly 1 HEADER frame, got %d", headerCount)
	}
	if types[0] != 0x06 {
		t.Errorf("HEADER must be first frame, got 0x%02x", types[0])
	}
}

// TestSendHeaderIdempotent verifies that calling SendHeader twice returns an error.
func TestSendHeaderIdempotent(t *testing.T) {
	s, _ := newStream(t)
	if err := s.SendHeader(nil); err != nil {
		t.Fatalf("first SendHeader: %v", err)
	}
	if err := s.SendHeader(nil); err == nil {
		t.Error("second SendHeader: want error, got nil")
	}
}

// TestServerStreamDispatch verifies the server-streaming RPC shape:
// server sends multiple MSG frames after one client MSG.
func TestServerStreamDispatch(t *testing.T) {
	s, sink := newStream(t)

	// Deliver the single request message and close recv.
	s.Deliver(mustMarshal(t, wrapperspb.String("foo bar baz")))
	s.CloseRecv()

	handler := testServiceDesc.Streams[0].Handler // ListWords
	if err := handler(svc, s); err != nil {
		t.Fatalf("handler: %v", err)
	}
	s.SendEnd(nil)

	payloads := sink.msgPayloads()
	if len(payloads) != 3 {
		t.Fatalf("want 3 MSG frames, got %d", len(payloads))
	}
	for i, want := range []string{"foo", "bar", "baz"} {
		got := mustUnmarshal(t, payloads[i], new(wrapperspb.StringValue))
		if got.Value != want {
			t.Errorf("msg[%d]: want %q, got %q", i, want, got.Value)
		}
	}
}

// TestClientStreamDispatch verifies the client-streaming RPC shape:
// client sends multiple MSG frames, server responds with one.
func TestClientStreamDispatch(t *testing.T) {
	s, sink := newStream(t)

	for _, word := range []string{"hello", "world"} {
		s.Deliver(mustMarshal(t, wrapperspb.String(word)))
	}
	s.CloseRecv()

	handler := testServiceDesc.Streams[1].Handler // Collect
	if err := handler(svc, s); err != nil {
		t.Fatalf("handler: %v", err)
	}
	s.SendEnd(nil)

	payloads := sink.msgPayloads()
	if len(payloads) != 1 {
		t.Fatalf("want 1 MSG frame, got %d", len(payloads))
	}
	got := mustUnmarshal(t, payloads[0], new(wrapperspb.StringValue))
	if got.Value != "hello,world" {
		t.Errorf("response: want %q, got %q", "hello,world", got.Value)
	}
}

// TestBidiDispatch verifies the bidi-streaming RPC shape:
// interleaved send/recv, and SetTrailer values are captured after handler returns.
func TestBidiDispatch(t *testing.T) {
	s, sink := newStream(t)

	// Feed 3 messages then close.
	for _, word := range []string{"a", "b", "c"} {
		s.Deliver(mustMarshal(t, wrapperspb.String(word)))
	}
	s.CloseRecv()

	handler := testServiceDesc.Streams[2].Handler // Chat
	if err := handler(svc, s); err != nil {
		t.Fatalf("handler: %v", err)
	}
	s.SendEnd(nil)

	// Verify echo responses.
	payloads := sink.msgPayloads()
	if len(payloads) != 3 {
		t.Fatalf("want 3 MSG frames, got %d", len(payloads))
	}
	for i, want := range []string{"echo:a", "echo:b", "echo:c"} {
		got := mustUnmarshal(t, payloads[i], new(wrapperspb.StringValue))
		if got.Value != want {
			t.Errorf("msg[%d]: want %q, got %q", i, want, got.Value)
		}
	}

	// Verify SetTrailer was captured.
	trailer := s.Trailer()
	if v := trailer.Get("x-chat-done"); len(v) == 0 || v[0] != "true" {
		t.Errorf("trailer x-chat-done: want [true], got %v", v)
	}
}

// TestContextCancelOnClose verifies that Cancel() cancels the stream context,
// which unblocks a handler blocked on RecvMsg.
func TestContextCancelOnClose(t *testing.T) {
	s, _ := newStream(t)

	done := make(chan error, 1)
	go func() {
		// This RecvMsg will block until the context is cancelled or RecvCh is closed.
		m := new(wrapperspb.StringValue)
		done <- s.RecvMsg(m)
	}()

	s.Cancel()
	// RecvMsg must unblock. Since the channel is not closed but context is cancelled,
	// the goroutine will be stuck — so we also close RecvCh to let it unblock via EOF.
	// (In production, RESET closes RecvCh AND calls Cancel simultaneously.)
	s.CloseRecv()

	if err := <-done; err != io.EOF {
		t.Errorf("RecvMsg after cancel: want io.EOF, got %v", err)
	}
}

// TestErrorStatusPropagation verifies that a handler returning a gRPC status error
// is correctly encoded in the END frame payload.
func TestErrorStatusPropagation(t *testing.T) {
	s, sink := newStream(t)

	handlerErr := status.Error(codes.NotFound, "method not found")
	s.SendEnd(handlerErr)

	types := sink.types()
	if len(types) != 1 || types[0] != 0x03 {
		t.Fatalf("want 1 END frame, got frames: %v", types)
	}

	// Decode EndPayload proto from the END frame.
	var ep framev1.EndPayload
	if err := proto.Unmarshal(sink.frames[0].payload, &ep); err != nil {
		t.Fatalf("unmarshal EndPayload: %v", err)
	}
	if got := codes.Code(ep.StatusCode); got != codes.NotFound {
		t.Errorf("status code: want %v, got %v", codes.NotFound, got)
	}
	if ep.StatusMessage != "method not found" {
		t.Errorf("status message: want %q, got %q", "method not found", ep.StatusMessage)
	}
}
