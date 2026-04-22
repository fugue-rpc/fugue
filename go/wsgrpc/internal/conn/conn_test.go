package conn_test

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/coder/websocket"
	"github.com/grpcws/wsgrpc/frame"
	"github.com/grpcws/wsgrpc/internal/conn"
	"github.com/grpcws/wsgrpc/internal/stream"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

// echoHandler reads all MSG frames on a stream and echoes the raw bytes back,
// then sends END. Used as OnStream in all tests.
func echoHandler(s *stream.Stream) {
	defer s.SendEnd(nil)
	for {
		m := new(wrapperspb.StringValue)
		if err := s.RecvMsg(m); err == io.EOF {
			return
		} else if err != nil {
			return
		}
		if err := s.SendMsg(m); err != nil {
			return
		}
	}
}

// newTestServer starts an httptest.Server that upgrades to WebSocket and runs
// conn.Serve with the provided OnStream handler.
func newTestServer(t *testing.T, onStream func(*stream.Stream)) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ws, err := websocket.Accept(w, r, &websocket.AcceptOptions{
			InsecureSkipVerify: true, // test only — no origin check needed
		})
		if err != nil {
			t.Logf("websocket.Accept: %v", err)
			return
		}
		c := conn.New(ws, nil)
		c.OnStream = onStream
		_ = c.Serve(r.Context())
	}))
	t.Cleanup(srv.Close)
	return srv
}

// wsURL converts an httptest.Server URL to a ws:// URL.
func wsURL(srv *httptest.Server) string {
	return strings.Replace(srv.URL, "http://", "ws://", 1)
}

// --- client-side helpers ---

// clientConn wraps a WebSocket connection with a simple frame-level demuxer.
// It runs a background read loop that dispatches incoming frames to per-stream channels.
type clientConn struct {
	ws      *websocket.Conn
	mu      sync.Mutex  // guards streams map
	writeMu sync.Mutex  // serialises ws.Write calls
	streams map[uint32]chan frame.Frame
}

func dialClient(ctx context.Context, t *testing.T, url string) *clientConn {
	t.Helper()
	ws, _, err := websocket.Dial(ctx, url, nil)
	if err != nil {
		t.Fatalf("websocket.Dial: %v", err)
	}
	c := &clientConn{ws: ws, streams: make(map[uint32]chan frame.Frame)}
	go c.readLoop(ctx)
	t.Cleanup(func() { ws.CloseNow() })
	return c
}

func (c *clientConn) register(id uint32) <-chan frame.Frame {
	ch := make(chan frame.Frame, 64)
	c.mu.Lock()
	c.streams[id] = ch
	c.mu.Unlock()
	return ch
}

func (c *clientConn) readLoop(ctx context.Context) {
	for {
		_, msg, err := c.ws.Read(ctx)
		if err != nil {
			// Drain all registered channels so goroutines waiting on them unblock.
			c.mu.Lock()
			for _, ch := range c.streams {
				close(ch)
			}
			c.streams = nil
			c.mu.Unlock()
			return
		}
		f, err := frame.Decode(msg)
		if err != nil {
			continue
		}
		c.mu.Lock()
		ch := c.streams[f.StreamID]
		c.mu.Unlock()
		if ch != nil {
			ch <- f
		}
	}
}

func (c *clientConn) send(ctx context.Context, t *testing.T, f frame.Frame) {
	t.Helper()
	encoded, err := frame.Encode(f)
	if err != nil {
		t.Fatalf("frame.Encode: %v", err)
	}
	c.writeMu.Lock()
	defer c.writeMu.Unlock()
	if err := c.ws.Write(ctx, websocket.MessageBinary, encoded); err != nil {
		t.Fatalf("ws.Write: %v", err)
	}
}

func (c *clientConn) expectFrameType(t *testing.T, ch <-chan frame.Frame, wantType uint8, timeout time.Duration) frame.Frame {
	t.Helper()
	select {
	case f, ok := <-ch:
		if !ok {
			t.Fatalf("channel closed before receiving frame type 0x%02x", wantType)
		}
		if f.Type != wantType {
			t.Errorf("frame type: want 0x%02x, got 0x%02x", wantType, f.Type)
		}
		return f
	case <-time.After(timeout):
		t.Fatalf("timeout waiting for frame type 0x%02x", wantType)
	}
	return frame.Frame{}
}

// --- tests ---

// TestServe20ConcurrentStreams is the Week 2 done criterion.
// 20 goroutines simultaneously open streams, exchange MSG frames,
// and verify clean shutdown. go test -race must pass.
func TestServe20ConcurrentStreams(t *testing.T) {
	const numStreams = 20
	srv := newTestServer(t, echoHandler)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	client := dialClient(ctx, t, wsURL(srv))

	// Register all stream channels and send BEGIN frames in strict ascending order.
	// Stream IDs must be monotonically increasing — we can't send them from racing
	// goroutines because the mutex doesn't guarantee ordering across goroutines.
	channels := make([]<-chan frame.Frame, numStreams+1)
	for i := 1; i <= numStreams; i++ {
		channels[i] = client.register(uint32(i))
		client.send(ctx, t, frame.Frame{Type: frame.TypeBEGIN, StreamID: uint32(i)})
	}

	// Once all streams are open, concurrently exchange messages on all of them.
	var wg sync.WaitGroup
	for i := 1; i <= numStreams; i++ {
		i := i
		wg.Add(1)
		go func() {
			defer wg.Done()
			id := uint32(i)
			ch := channels[i]
			text := fmt.Sprintf("stream-%d", i)
			msgPayload, _ := proto.Marshal(wrapperspb.String(text))

			// Send one message then half-close.
			client.send(ctx, t, frame.Frame{Type: frame.TypeMSG, StreamID: id, Payload: msgPayload})
			client.send(ctx, t, frame.Frame{Type: frame.TypeEND, StreamID: id})

			// Expect HEADER (auto-flushed by server before first SendMsg).
			client.expectFrameType(t, ch, frame.TypeHEADER, 5*time.Second)

			// Expect echoed MSG.
			msgFrame := client.expectFrameType(t, ch, frame.TypeMSG, 5*time.Second)
			got := new(wrapperspb.StringValue)
			if err := proto.Unmarshal(msgFrame.Payload, got); err != nil {
				t.Errorf("stream %d: unmarshal echo: %v", id, err)
			} else if got.Value != text {
				t.Errorf("stream %d: echo value: want %q, got %q", id, text, got.Value)
			}

			// Expect END from server.
			client.expectFrameType(t, ch, frame.TypeEND, 5*time.Second)
		}()
	}

	wg.Wait()
}

// TestStreamIDZeroIsProtocolError verifies that stream_id=0 closes the connection.
func TestStreamIDZeroIsProtocolError(t *testing.T) {
	srv := newTestServer(t, echoHandler)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	ws, _, err := websocket.Dial(ctx, wsURL(srv), nil)
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	defer ws.CloseNow()

	encoded, _ := frame.Encode(frame.Frame{Type: frame.TypeBEGIN, StreamID: 0})
	_ = ws.Write(ctx, websocket.MessageBinary, encoded)

	// The server must close the connection after a stream_id=0 BEGIN.
	_, _, err = ws.Read(ctx)
	if err == nil {
		t.Error("expected connection close after stream_id=0, got nil error")
	}
}

// TestNonMonotonicStreamIDIsProtocolError verifies that a BEGIN with a
// stream_id ≤ the highest seen closes the connection.
func TestNonMonotonicStreamIDIsProtocolError(t *testing.T) {
	srv := newTestServer(t, echoHandler)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	ws, _, err := websocket.Dial(ctx, wsURL(srv), nil)
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	defer ws.CloseNow()

	sendBegin := func(id uint32) {
		encoded, _ := frame.Encode(frame.Frame{Type: frame.TypeBEGIN, StreamID: id})
		_ = ws.Write(ctx, websocket.MessageBinary, encoded)
	}

	sendBegin(5) // accepted
	sendBegin(3) // non-monotonic — protocol error

	_, _, err = ws.Read(ctx)
	if err == nil {
		t.Error("expected connection close after non-monotonic stream_id, got nil error")
	}
}

// TestRESETForUnknownStreamIsDropped verifies that RESET for a closed/unseen
// stream is silently ignored (spec §4.4).
func TestRESETForUnknownStreamIsDropped(t *testing.T) {
	srv := newTestServer(t, echoHandler)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	client := dialClient(ctx, t, wsURL(srv))

	// Send RESET for a stream that was never opened.
	client.send(ctx, t, frame.Frame{Type: frame.TypeRESET, StreamID: 99})

	// Follow up with a valid stream to prove the connection is still alive.
	ch := client.register(1)
	client.send(ctx, t, frame.Frame{Type: frame.TypeBEGIN, StreamID: 1})

	payload, _ := proto.Marshal(wrapperspb.String("alive"))
	client.send(ctx, t, frame.Frame{Type: frame.TypeMSG, StreamID: 1, Payload: payload})
	client.send(ctx, t, frame.Frame{Type: frame.TypeEND, StreamID: 1})

	client.expectFrameType(t, ch, frame.TypeHEADER, 5*time.Second)
	client.expectFrameType(t, ch, frame.TypeMSG, 5*time.Second)
	client.expectFrameType(t, ch, frame.TypeEND, 5*time.Second)
}

// TestRESETCancelsStream verifies that a client RESET cancels the stream context
// and unblocks any handler blocked on RecvMsg.
func TestRESETCancelsStream(t *testing.T) {
	handlerBlocked := make(chan struct{})
	handlerDone := make(chan struct{})

	onStream := func(s *stream.Stream) {
		defer close(handlerDone)
		close(handlerBlocked) // signal that handler is running
		m := new(wrapperspb.StringValue)
		err := s.RecvMsg(m) // will block until RESET arrives
		if err == nil || err == io.EOF {
			// RESET closes RecvCh, so RecvMsg returns io.EOF
			return
		}
	}

	srv := newTestServer(t, onStream)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	client := dialClient(ctx, t, wsURL(srv))
	client.send(ctx, t, frame.Frame{Type: frame.TypeBEGIN, StreamID: 1})

	// Wait for handler to start.
	select {
	case <-handlerBlocked:
	case <-time.After(2 * time.Second):
		t.Fatal("handler did not start")
	}

	// Send RESET — handler's RecvMsg should unblock.
	client.send(ctx, t, frame.Frame{Type: frame.TypeRESET, StreamID: 1})

	select {
	case <-handlerDone:
	case <-time.After(2 * time.Second):
		t.Fatal("handler did not exit after RESET")
	}
}
