package fugue_test

import (
	"context"
	"io"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/coder/websocket"
	echov1 "github.com/fugue-rpc/fugue/echo/v1"
	framev1 "github.com/fugue-rpc/fugue/frame/v1"
	"github.com/fugue-rpc/fugue/frame"
	"github.com/fugue-rpc/fugue"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/protobuf/proto"
)

// --- echo implementation ---

type echoImpl struct{ echov1.UnimplementedEchoServer }

func (e *echoImpl) Echo(_ context.Context, req *echov1.Msg) (*echov1.Msg, error) {
	return &echov1.Msg{Value: req.Value}, nil
}

func (e *echoImpl) EchoStream(req *echov1.Msg, stream grpc.ServerStreamingServer[echov1.Msg]) error {
	for i := 0; i < 3; i++ {
		if err := stream.Send(&echov1.Msg{Value: req.Value}); err != nil {
			return err
		}
	}
	return nil
}

func (e *echoImpl) EchoCollect(stream grpc.ClientStreamingServer[echov1.Msg, echov1.Msg]) error {
	var parts []string
	for {
		msg, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}
		parts = append(parts, msg.Value)
	}
	return stream.SendAndClose(&echov1.Msg{Value: strings.Join(parts, ",")})
}

func (e *echoImpl) EchoBidi(stream grpc.BidiStreamingServer[echov1.Msg, echov1.Msg]) error {
	for {
		msg, err := stream.Recv()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}
		if err := stream.Send(&echov1.Msg{Value: msg.Value}); err != nil {
			return err
		}
	}
}

// --- test helpers ---

func newEchoServer(t testing.TB) *httptest.Server {
	t.Helper()
	srv := fugue.NewServer()
	echov1.RegisterEchoServer(srv, &echoImpl{})
	hs := httptest.NewServer(srv)
	t.Cleanup(hs.Close)
	return hs
}

func wsURL(srv *httptest.Server) string {
	return strings.Replace(srv.URL, "http://", "ws://", 1)
}

// testConn is a minimal WebSocket client that sends and receives fugue frames.
// frameBuf holds decoded frames that arrived in the same coalesced WebSocket
// message as a previously consumed frame; it is drained before the next Read.
type testConn struct {
	ws       *websocket.Conn
	writeMu  sync.Mutex
	frameBuf []frame.Frame
}

func dial(ctx context.Context, t testing.TB, url string) *testConn {
	t.Helper()
	ws, _, err := websocket.Dial(ctx, url, nil)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	t.Cleanup(func() { ws.CloseNow() })
	return &testConn{ws: ws}
}

func (c *testConn) send(ctx context.Context, t testing.TB, f frame.Frame) {
	t.Helper()
	b, err := frame.Encode(f)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	c.writeMu.Lock()
	defer c.writeMu.Unlock()
	if err := c.ws.Write(ctx, websocket.MessageBinary, b); err != nil {
		t.Fatalf("ws.Write: %v", err)
	}
}

func (c *testConn) sendBegin(ctx context.Context, t testing.TB, id uint32, method string) {
	t.Helper()
	payload, err := proto.Marshal(&framev1.BeginPayload{Method: method})
	if err != nil {
		t.Fatalf("marshal BeginPayload: %v", err)
	}
	c.send(ctx, t, frame.Frame{Type: frame.TypeBEGIN, StreamID: id, Payload: payload})
}

func (c *testConn) sendMsg(ctx context.Context, t testing.TB, id uint32, m proto.Message) {
	t.Helper()
	payload, err := proto.Marshal(m)
	if err != nil {
		t.Fatalf("marshal msg: %v", err)
	}
	c.send(ctx, t, frame.Frame{Type: frame.TypeMSG, StreamID: id, Payload: payload})
}

func (c *testConn) sendEnd(ctx context.Context, t testing.TB, id uint32) {
	t.Helper()
	c.send(ctx, t, frame.Frame{Type: frame.TypeEND, StreamID: id})
}

func (c *testConn) readFrame(ctx context.Context, t testing.TB) frame.Frame {
	t.Helper()
	// Drain any frames left over from the previous coalesced WebSocket message.
	for len(c.frameBuf) == 0 {
		_, msg, err := c.ws.Read(ctx)
		if err != nil {
			t.Fatalf("ws.Read: %v", err)
		}
		frames, err := frame.DecodeAll(msg)
		if err != nil {
			t.Fatalf("decode frames: %v", err)
		}
		c.frameBuf = frames
	}
	f := c.frameBuf[0]
	c.frameBuf = c.frameBuf[1:]
	return f
}

func (c *testConn) expectType(ctx context.Context, t testing.TB, want uint8) frame.Frame {
	t.Helper()
	f := c.readFrame(ctx, t)
	if f.Type != want {
		t.Errorf("frame type: want 0x%02x, got 0x%02x", want, f.Type)
	}
	return f
}

func (c *testConn) recvMsg(ctx context.Context, t testing.TB, id uint32, m proto.Message) {
	t.Helper()
	f := c.expectType(ctx, t, frame.TypeMSG)
	if f.StreamID != id {
		t.Errorf("MSG stream ID: want %d, got %d", id, f.StreamID)
	}
	if err := proto.Unmarshal(f.Payload, m); err != nil {
		t.Fatalf("unmarshal MSG: %v", err)
	}
}

// --- tests ---

func TestUnaryEcho(t *testing.T) {
	srv := newEchoServer(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	c := dial(ctx, t, wsURL(srv))
	c.sendBegin(ctx, t, 1, echov1.Echo_Echo_FullMethodName)
	c.sendMsg(ctx, t, 1, &echov1.Msg{Value: "hello"})
	c.sendEnd(ctx, t, 1)

	c.expectType(ctx, t, frame.TypeHEADER)

	resp := new(echov1.Msg)
	c.recvMsg(ctx, t, 1, resp)
	if resp.Value != "hello" {
		t.Errorf("echo: want %q, got %q", "hello", resp.Value)
	}

	c.expectType(ctx, t, frame.TypeEND)
}

func TestServerStreamEcho(t *testing.T) {
	srv := newEchoServer(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	c := dial(ctx, t, wsURL(srv))
	c.sendBegin(ctx, t, 1, echov1.Echo_EchoStream_FullMethodName)
	c.sendMsg(ctx, t, 1, &echov1.Msg{Value: "ping"})
	c.sendEnd(ctx, t, 1)

	c.expectType(ctx, t, frame.TypeHEADER)

	for i := 0; i < 3; i++ {
		resp := new(echov1.Msg)
		c.recvMsg(ctx, t, 1, resp)
		if resp.Value != "ping" {
			t.Errorf("stream[%d]: want %q, got %q", i, "ping", resp.Value)
		}
	}

	c.expectType(ctx, t, frame.TypeEND)
}

func TestClientStreamEcho(t *testing.T) {
	srv := newEchoServer(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	c := dial(ctx, t, wsURL(srv))
	c.sendBegin(ctx, t, 1, echov1.Echo_EchoCollect_FullMethodName)
	c.sendMsg(ctx, t, 1, &echov1.Msg{Value: "a"})
	c.sendMsg(ctx, t, 1, &echov1.Msg{Value: "b"})
	c.sendMsg(ctx, t, 1, &echov1.Msg{Value: "c"})
	c.sendEnd(ctx, t, 1)

	c.expectType(ctx, t, frame.TypeHEADER)

	resp := new(echov1.Msg)
	c.recvMsg(ctx, t, 1, resp)
	if resp.Value != "a,b,c" {
		t.Errorf("collect: want %q, got %q", "a,b,c", resp.Value)
	}

	c.expectType(ctx, t, frame.TypeEND)
}

func TestBidiStreamEcho(t *testing.T) {
	srv := newEchoServer(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	c := dial(ctx, t, wsURL(srv))
	c.sendBegin(ctx, t, 1, echov1.Echo_EchoBidi_FullMethodName)

	words := []string{"alpha", "beta", "gamma"}
	for _, w := range words {
		c.sendMsg(ctx, t, 1, &echov1.Msg{Value: w})
	}
	c.sendEnd(ctx, t, 1)

	c.expectType(ctx, t, frame.TypeHEADER)

	for _, want := range words {
		got := new(echov1.Msg)
		c.recvMsg(ctx, t, 1, got)
		if got.Value != want {
			t.Errorf("bidi echo: want %q, got %q", want, got.Value)
		}
	}

	c.expectType(ctx, t, frame.TypeEND)
}

func TestUnimplementedMethod(t *testing.T) {
	srv := newEchoServer(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	c := dial(ctx, t, wsURL(srv))
	c.sendBegin(ctx, t, 1, "/unknown.Service/NoSuchMethod")

	// Unimplemented handler calls SendEnd directly without any SendMsg, so no
	// HEADER is auto-flushed (spec §4.5: HEADER only required before MSG).
	// Expect exactly one END frame carrying UNIMPLEMENTED status.
	f := c.expectType(ctx, t, frame.TypeEND)
	var ep framev1.EndPayload
	if err := proto.Unmarshal(f.Payload, &ep); err != nil {
		t.Fatalf("unmarshal EndPayload: %v", err)
	}
	if codes.Code(ep.StatusCode) != codes.Unimplemented {
		t.Errorf("status: want Unimplemented, got %v", codes.Code(ep.StatusCode))
	}
}

// TestMaxConcurrentStreams verifies that WithMaxConcurrentStreams(N) rejects the
// (N+1)th stream with RESOURCE_EXHAUSTED while keeping the connection alive.
func TestMaxConcurrentStreams(t *testing.T) {
	const limit = 2

	s := fugue.NewServer(fugue.WithMaxConcurrentStreams(limit))
	echov1.RegisterEchoServer(s, &echoImpl{})
	hs := httptest.NewServer(s)
	t.Cleanup(hs.Close)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	c := dial(ctx, t, wsURL(hs))

	// Per-stream frame channels; writes happen before the goroutine starts so
	// no mutex is needed for the demuxer reads.
	streams := make(map[uint32]chan frame.Frame)
	for i := uint32(1); i <= limit+1; i++ {
		streams[i] = make(chan frame.Frame, 32)
	}

	go func() {
		for {
			_, msg, err := c.ws.Read(ctx)
			if err != nil {
				return
			}
			frames, err := frame.DecodeAll(msg)
			if err != nil {
				continue
			}
			for _, f := range frames {
				if ch := streams[f.StreamID]; ch != nil {
					ch <- f
				}
			}
		}
	}()

	expect := func(id uint32, want uint8) frame.Frame {
		t.Helper()
		select {
		case f := <-streams[id]:
			if f.Type != want {
				t.Errorf("stream %d: want 0x%02x, got 0x%02x", id, want, f.Type)
			}
			return f
		case <-time.After(3 * time.Second):
			t.Fatalf("stream %d: timeout waiting for 0x%02x", id, want)
		}
		return frame.Frame{}
	}

	// Open 'limit' bidi streams and send a message on each to confirm handlers
	// are running before attempting the over-limit stream.
	for i := uint32(1); i <= limit; i++ {
		c.sendBegin(ctx, t, i, echov1.Echo_EchoBidi_FullMethodName)
	}
	for i := uint32(1); i <= limit; i++ {
		c.sendMsg(ctx, t, i, &echov1.Msg{Value: "ping"})
	}
	for i := uint32(1); i <= limit; i++ {
		expect(i, frame.TypeHEADER)
		expect(i, frame.TypeMSG)
	}

	// Try to open one more stream — must be rejected immediately.
	overLimit := uint32(limit + 1)
	c.sendBegin(ctx, t, overLimit, echov1.Echo_EchoBidi_FullMethodName)

	f := expect(overLimit, frame.TypeEND)
	var ep framev1.EndPayload
	if err := proto.Unmarshal(f.Payload, &ep); err != nil {
		t.Fatalf("unmarshal EndPayload: %v", err)
	}
	if codes.Code(ep.StatusCode) != codes.ResourceExhausted {
		t.Errorf("status: want ResourceExhausted, got %v", codes.Code(ep.StatusCode))
	}

	// Close the open streams and verify the connection is still alive.
	for i := uint32(1); i <= limit; i++ {
		c.sendEnd(ctx, t, i)
	}
	for i := uint32(1); i <= limit; i++ {
		expect(i, frame.TypeEND)
	}
}

func TestConcurrent20Streams(t *testing.T) {
	const n = 20
	srv := newEchoServer(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	c := dial(ctx, t, wsURL(srv))

	// Register all receive channels keyed by stream ID.
	type chanMap struct {
		mu sync.Mutex
		m  map[uint32]chan frame.Frame
	}
	cm := &chanMap{m: make(map[uint32]chan frame.Frame)}
	for i := uint32(1); i <= n; i++ {
		ch := make(chan frame.Frame, 32)
		cm.mu.Lock()
		cm.m[i] = ch
		cm.mu.Unlock()
	}

	// Read loop demuxes frames to per-stream channels.
	done := make(chan struct{})
	go func() {
		defer close(done)
		for {
			_, msg, err := c.ws.Read(ctx)
			if err != nil {
				return
			}
			frames, err := frame.DecodeAll(msg)
			if err != nil {
				continue
			}
			for _, f := range frames {
				cm.mu.Lock()
				ch := cm.m[f.StreamID]
				cm.mu.Unlock()
				if ch != nil {
					ch <- f
				}
			}
		}
	}()

	// Send all BEGINs in ascending order (monotonicity requirement).
	for i := uint32(1); i <= n; i++ {
		c.sendBegin(ctx, t, i, echov1.Echo_Echo_FullMethodName)
	}

	// Each goroutine sends MSG+END and reads HEADER+MSG+END concurrently.
	var wg sync.WaitGroup
	for i := uint32(1); i <= n; i++ {
		i := i
		wg.Add(1)
		go func() {
			defer wg.Done()
			ch := cm.m[i]
			c.sendMsg(ctx, t, i, &echov1.Msg{Value: "ok"})
			c.sendEnd(ctx, t, i)

			expect := func(want uint8) {
				select {
				case f := <-ch:
					if f.Type != want {
						t.Errorf("stream %d: want 0x%02x, got 0x%02x", i, want, f.Type)
					}
				case <-time.After(5 * time.Second):
					t.Errorf("stream %d: timeout waiting for 0x%02x", i, want)
				}
			}
			expect(frame.TypeHEADER)
			expect(frame.TypeMSG)
			expect(frame.TypeEND)
		}()
	}
	wg.Wait()
}

// ── Benchmarks ────────────────────────────────────────────────────────────────

// BenchmarkUnaryEcho measures the full round-trip latency of a single
// sequential unary RPC: BEGIN+MSG+END → HEADER+MSG+END.
// One connection is reused across all iterations; stream IDs increment.
func BenchmarkUnaryEcho(b *testing.B) {
	srv := newEchoServer(b)
	ctx := context.Background()
	c := dial(ctx, b, wsURL(srv))

	payload, _ := proto.Marshal(&echov1.Msg{Value: "hello"})
	b.SetBytes(int64(len(payload)))
	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		id := uint32(i + 1)
		c.sendBegin(ctx, b, id, echov1.Echo_Echo_FullMethodName)
		c.sendMsg(ctx, b, id, &echov1.Msg{Value: "hello"})
		c.sendEnd(ctx, b, id)
		c.expectType(ctx, b, frame.TypeHEADER)
		c.expectType(ctx, b, frame.TypeMSG)
		c.expectType(ctx, b, frame.TypeEND)
	}
}

// BenchmarkUnaryEchoParallel measures unary throughput with GOMAXPROCS
// goroutines each driving their own connection.
func BenchmarkUnaryEchoParallel(b *testing.B) {
	srv := newEchoServer(b)
	b.ReportAllocs()
	b.ResetTimer()

	b.RunParallel(func(pb *testing.PB) {
		ctx := context.Background()
		c := dial(ctx, b, wsURL(srv))
		defer c.ws.CloseNow()
		var id uint32
		for pb.Next() {
			id++
			c.sendBegin(ctx, b, id, echov1.Echo_Echo_FullMethodName)
			c.sendMsg(ctx, b, id, &echov1.Msg{Value: "hello"})
			c.sendEnd(ctx, b, id)
			c.expectType(ctx, b, frame.TypeHEADER)
			c.expectType(ctx, b, frame.TypeMSG)
			c.expectType(ctx, b, frame.TypeEND)
		}
	})
}

// BenchmarkBidiEcho measures the round-trip latency of a single message on a
// persistent bidi stream: send one MSG, receive one MSG echo per iteration.
// The stream stays open across all b.N iterations so stream-open overhead is
// amortised. Compare with BenchmarkUnaryEcho to see the cost difference
// between unary and bidi stream reuse.
func BenchmarkBidiEcho(b *testing.B) {
	srv := newEchoServer(b)
	ctx := context.Background()

	ws, _, err := websocket.Dial(ctx, wsURL(srv), nil)
	if err != nil {
		b.Fatalf("dial: %v", err)
	}
	b.Cleanup(func() { ws.CloseNow() })

	sendRaw := func(f frame.Frame) {
		enc, _ := frame.Encode(f)
		if err := ws.Write(ctx, websocket.MessageBinary, enc); err != nil {
			b.Fatalf("ws.Write: %v", err)
		}
	}
	var bidiFrameBuf []frame.Frame
	recvFrame := func() frame.Frame {
		for len(bidiFrameBuf) == 0 {
			_, raw, err := ws.Read(ctx)
			if err != nil {
				b.Fatalf("ws.Read: %v", err)
			}
			frames, err := frame.DecodeAll(raw)
			if err != nil {
				b.Fatalf("frame.DecodeAll: %v", err)
			}
			bidiFrameBuf = frames
		}
		f := bidiFrameBuf[0]
		bidiFrameBuf = bidiFrameBuf[1:]
		return f
	}

	// Open bidi stream; consume the HEADER that arrives with the first echo.
	beginPayload, _ := proto.Marshal(&framev1.BeginPayload{Method: echov1.Echo_EchoBidi_FullMethodName})
	sendRaw(frame.Frame{Type: frame.TypeBEGIN, StreamID: 1, Payload: beginPayload})

	payload, _ := proto.Marshal(&echov1.Msg{Value: "x"})
	b.SetBytes(int64(len(payload)))
	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		sendRaw(frame.Frame{Type: frame.TypeMSG, StreamID: 1, Payload: payload})
		// First iteration also drains the HEADER the server sends before its
		// first reply; subsequent iterations see only MSG frames.
		if i == 0 {
			f := recvFrame()
			if f.Type == frame.TypeHEADER {
				recvFrame() // discard HEADER, read the actual MSG echo
			}
		} else {
			recvFrame() // MSG echo
		}
	}

	// Half-close so the server handler returns cleanly.
	sendRaw(frame.Frame{Type: frame.TypeEND, StreamID: 1})
}
