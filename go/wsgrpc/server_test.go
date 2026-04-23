package wsgrpc_test

import (
	"context"
	"io"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/coder/websocket"
	echov1 "github.com/grpcws/wsgrpc/echo/v1"
	framev1 "github.com/grpcws/wsgrpc/grpcws/frame/v1"
	"github.com/grpcws/wsgrpc/frame"
	"github.com/grpcws/wsgrpc"
	"google.golang.org/grpc"
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

func newEchoServer(t *testing.T) *httptest.Server {
	t.Helper()
	srv := wsgrpc.NewServer()
	echov1.RegisterEchoServer(srv, &echoImpl{})
	hs := httptest.NewServer(srv)
	t.Cleanup(hs.Close)
	return hs
}

func wsURL(srv *httptest.Server) string {
	return strings.Replace(srv.URL, "http://", "ws://", 1)
}

// testConn is a minimal WebSocket client that sends and receives grpcws frames.
type testConn struct {
	ws      *websocket.Conn
	writeMu sync.Mutex
}

func dial(ctx context.Context, t *testing.T, url string) *testConn {
	t.Helper()
	ws, _, err := websocket.Dial(ctx, url, nil)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	t.Cleanup(func() { ws.CloseNow() })
	return &testConn{ws: ws}
}

func (c *testConn) send(ctx context.Context, t *testing.T, f frame.Frame) {
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

func (c *testConn) sendBegin(ctx context.Context, t *testing.T, id uint32, method string) {
	t.Helper()
	payload, err := proto.Marshal(&framev1.BeginPayload{Method: method})
	if err != nil {
		t.Fatalf("marshal BeginPayload: %v", err)
	}
	c.send(ctx, t, frame.Frame{Type: frame.TypeBEGIN, StreamID: id, Payload: payload})
}

func (c *testConn) sendMsg(ctx context.Context, t *testing.T, id uint32, m proto.Message) {
	t.Helper()
	payload, err := proto.Marshal(m)
	if err != nil {
		t.Fatalf("marshal msg: %v", err)
	}
	c.send(ctx, t, frame.Frame{Type: frame.TypeMSG, StreamID: id, Payload: payload})
}

func (c *testConn) sendEnd(ctx context.Context, t *testing.T, id uint32) {
	t.Helper()
	c.send(ctx, t, frame.Frame{Type: frame.TypeEND, StreamID: id})
}

func (c *testConn) readFrame(ctx context.Context, t *testing.T) frame.Frame {
	t.Helper()
	_, msg, err := c.ws.Read(ctx)
	if err != nil {
		t.Fatalf("ws.Read: %v", err)
	}
	f, err := frame.Decode(msg)
	if err != nil {
		t.Fatalf("decode frame: %v", err)
	}
	return f
}

func (c *testConn) expectType(ctx context.Context, t *testing.T, want uint8) frame.Frame {
	t.Helper()
	f := c.readFrame(ctx, t)
	if f.Type != want {
		t.Errorf("frame type: want 0x%02x, got 0x%02x", want, f.Type)
	}
	return f
}

func (c *testConn) recvMsg(ctx context.Context, t *testing.T, id uint32, m proto.Message) {
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

	// Server should respond with HEADER (from auto-flush) then END with Unimplemented.
	// Actually for unimplemented, no HEADER is sent — server calls SendEnd directly.
	// But SendEnd uses WriteFrame directly, which never calls flushHeader.
	// So we just get END.
	f := c.readFrame(ctx, t)
	if f.Type != frame.TypeEND {
		// If a HEADER slipped through, skip it and check the next frame.
		if f.Type == frame.TypeHEADER {
			f = c.readFrame(ctx, t)
		}
		if f.Type != frame.TypeEND {
			t.Fatalf("want END for unknown method, got 0x%02x", f.Type)
		}
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
			f, err := frame.Decode(msg)
			if err != nil {
				continue
			}
			cm.mu.Lock()
			ch := cm.m[f.StreamID]
			cm.mu.Unlock()
			if ch != nil {
				ch <- f
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
