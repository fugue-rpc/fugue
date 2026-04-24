// stress hammers an echo server and reports throughput + latency.
//
// Modes:
//
//	grpcws (default) — grpcws WebSocket unary
//	  stress -addr ws://localhost:8080/wsgrpc/ -conns 10 -streams 10 -duration 30s
//
//	connect-h1 / connect-h2 — Connect protocol unary over HTTP/1.1 or HTTP/2 (h2c)
//	  stress -mode connect-h1 -connect-addr http://localhost:8090/echo.v1.Echo/Echo -conns 10 -streams 10
//
//	stream-server / stream-client / stream-bidi — grpcws streaming modes
//	  stress -mode stream-server -msgs-per-stream 100 -conns 10 -streams 10
//
//	connect-stream — Connect-ES server-streaming (comparison for stream-server only)
//	  stress -mode connect-stream -connect-stream-addr http://localhost:8090/echo.v1.Echo/EchoStreamN
//
// Profiling:
//
//	stress -cpuprofile cpu.pprof   # write CPU profile (open with: go tool pprof cpu.pprof)
//	stress -memprofile mem.pprof   # write heap profile after run
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"runtime/pprof"
	"sort"
	"sync"
	"syscall"
	"time"

	"github.com/coder/websocket"
	echov1 "github.com/grpcws/wsgrpc/echo/v1"
	framev1 "github.com/grpcws/wsgrpc/grpcws/frame/v1"
	"github.com/grpcws/wsgrpc/frame"
	"google.golang.org/protobuf/proto"
)

var (
	addr              = flag.String("addr", "ws://localhost:8080/wsgrpc/", "grpcws WebSocket address")
	numConns          = flag.Int("conns", 10, "number of connections (WebSocket conns or HTTP client instances)")
	numStreams        = flag.Int("streams", 10, "concurrent streams/goroutines per connection")
	duration          = flag.Duration("duration", 30*time.Second, "test duration")
	payloadSize       = flag.Int("payload", 0, "request payload size in bytes (0 = use default 'stress' message)")
	cpuProfile        = flag.String("cpuprofile", "", "write CPU profile to file")
	memProfile        = flag.String("memprofile", "", "write heap profile to file")
	mode              = flag.String("mode", "grpcws", "protocol mode: grpcws | connect-h1 | connect-h2 | stream-server | stream-client | stream-bidi | connect-stream")
	connectAddr       = flag.String("connect-addr", "http://localhost:8090/echo.v1.Echo/Echo", "Connect unary echo server URL")
	connectStreamAddr = flag.String("connect-stream-addr", "http://localhost:8090/echo.v1.Echo/EchoStreamN", "Connect server-streaming echo server URL")
	msgsPerStream     = flag.Int("msgs-per-stream", 100, "messages per stream for streaming modes")
)

// workerResult holds per-goroutine stats (no mutex needed — one goroutine owns it).
type workerResult struct {
	completed     int64
	errors        int64
	latencies     []int64 // per-stream e2e latency (ns)
	ttfmLatencies []int64 // time-to-first-message, for streaming modes (ns)
}

func main() {
	flag.Parse()

	if *cpuProfile != "" {
		f, err := os.Create(*cpuProfile)
		if err != nil {
			log.Fatalf("create cpu profile: %v", err)
		}
		defer f.Close()
		if err := pprof.StartCPUProfile(f); err != nil {
			log.Fatalf("start cpu profile: %v", err)
		}
		defer pprof.StopCPUProfile()
	}

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	deadline := time.Now().Add(*duration)
	runCtx, runCancel := context.WithDeadline(ctx, deadline)
	defer runCancel()

	switch *mode {
	case "connect-h1", "connect-h2":
		fmt.Printf("Mode:        %s\n", *mode)
		fmt.Printf("Endpoint:    %s\n", *connectAddr)
		fmt.Printf("Goroutines:  %d (%d conns × %d streams)  Duration: %s\n\n",
			*numConns**numStreams, *numConns, *numStreams, *duration)
		runConnectMode(runCtx)
	case "stream-server":
		runStreamServerMode(runCtx, ctx)
	case "stream-client":
		runStreamClientMode(runCtx, ctx)
	case "stream-bidi":
		runStreamBidiMode(runCtx, ctx)
	case "connect-stream":
		fmt.Printf("Mode:        connect-stream (Connect-ES server-streaming)\n")
		fmt.Printf("Endpoint:    %s\n", *connectStreamAddr)
		fmt.Printf("Goroutines:  %d (%d conns × %d streams)  Msgs/stream: %d  Duration: %s\n\n",
			*numConns**numStreams, *numConns, *numStreams, *msgsPerStream, *duration)
		runConnectStreamMode(runCtx)
	default:
		fmt.Printf("Mode:        grpcws\n")
		fmt.Printf("Connecting to %s\n", *addr)
		fmt.Printf("Connections: %d  Streams/conn: %d  Duration: %s\n\n",
			*numConns, *numStreams, *duration)
		runGrpcwsMode(runCtx, ctx)
	}

	if *memProfile != "" {
		f, err := os.Create(*memProfile)
		if err != nil {
			log.Fatalf("create mem profile: %v", err)
		}
		defer f.Close()
		if err := pprof.WriteHeapProfile(f); err != nil {
			log.Fatalf("write mem profile: %v", err)
		}
		fmt.Printf("\nHeap profile written to %s\n", *memProfile)
	}
}

// ── grpcws mode ───────────────────────────────────────────────────────────────

func runGrpcwsMode(runCtx, dialCtx context.Context) {
	workerResults := make([]workerResult, *numConns**numStreams)
	var wg sync.WaitGroup

	var reqPayload []byte
	if *payloadSize > 0 {
		reqPayload, _ = proto.Marshal(&echov1.Msg{Value: string(make([]byte, *payloadSize))})
		fmt.Printf("Payload size: %d bytes (proto-encoded: %d bytes)\n\n", *payloadSize, len(reqPayload))
	} else {
		reqPayload, _ = proto.Marshal(&echov1.Msg{Value: "stress"})
	}

	workerIdx := 0
	for c := 0; c < *numConns; c++ {
		conn, err := newConn(dialCtx, *addr)
		if err != nil {
			log.Printf("conn %d: dial failed: %v", c, err)
			continue
		}
		for s := 0; s < *numStreams; s++ {
			idx := workerIdx
			workerIdx++
			wg.Add(1)
			go func(conn *connState, res *workerResult) {
				defer wg.Done()
				res.latencies = make([]int64, 0, 4096)
				runWorker(runCtx, conn, reqPayload, res)
			}(conn, &workerResults[idx])
		}
	}

	wg.Wait()

	var totalCompleted, totalErrors int64
	var allLatencies []int64
	for i := range workerResults {
		totalCompleted += workerResults[i].completed
		totalErrors += workerResults[i].errors
		allLatencies = append(allLatencies, workerResults[i].latencies...)
	}
	printResults(totalCompleted, totalErrors, float64(totalCompleted)/duration.Seconds(), allLatencies)
}

// ── Per-connection state ──────────────────────────────────────────────────────

type connState struct {
	ws      *websocket.Conn
	nextID  uint32 // only incremented while holding writeMu
	writeMu sync.Mutex
	chMu    sync.RWMutex
	chans   map[uint32]chan frame.Frame
}

func newConn(ctx context.Context, addr string) (*connState, error) {
	ws, _, err := websocket.Dial(ctx, addr, nil)
	if err != nil {
		return nil, err
	}
	// Match the server's read limit so large payloads aren't rejected by the
	// WebSocket layer before the grpcws frame decoder can validate them.
	ws.SetReadLimit(4*1024*1024 + 9) // MaxPayloadSize + HeaderSize
	c := &connState{
		ws:    ws,
		chans: make(map[uint32]chan frame.Frame),
	}
	go c.readLoop(ctx)
	return c, nil
}

func (c *connState) readLoop(ctx context.Context) {
	defer func() {
		// Close all waiting channels so goroutines unblock.
		c.chMu.Lock()
		for _, ch := range c.chans {
			close(ch)
		}
		c.chans = nil
		c.chMu.Unlock()
		c.ws.CloseNow()
	}()
	for {
		_, msg, err := c.ws.Read(ctx)
		if err != nil {
			return
		}
		// One WebSocket message may contain multiple coalesced frames from the
		// server's write-queue batching. Decode all of them and route each to
		// its stream channel.
		frames, err := frame.DecodeAll(msg)
		if err != nil {
			continue
		}
		for _, f := range frames {
			c.chMu.RLock()
			ch := c.chans[f.StreamID]
			c.chMu.RUnlock()
			if ch != nil {
				select {
				case ch <- f:
				default: // drop if consumer is too slow (shouldn't happen for unary)
				}
			}
		}
	}
}

// beginStream allocates a new stream ID, registers its receive channel, and
// sends the BEGIN frame — all while holding writeMu. This guarantees that the
// server always sees BEGIN frames in strictly ascending stream-ID order, which
// is required by the protocol (§5.1). Any other write ordering would cause the
// server to close the connection with code 1002.
// chanCap sets the receive channel capacity; pass 8 for unary, msgsPerStream+8 for streaming.
func (c *connState) beginStream(beginPayload []byte, chanCap int) (uint32, <-chan frame.Frame, error) {
	ch := make(chan frame.Frame, chanCap)
	c.writeMu.Lock()
	c.nextID++
	id := c.nextID
	c.chMu.Lock()
	if c.chans != nil {
		c.chans[id] = ch
	}
	c.chMu.Unlock()
	b, _ := frame.Encode(frame.Frame{Type: frame.TypeBEGIN, StreamID: id, Payload: beginPayload})
	err := c.ws.Write(context.Background(), websocket.MessageBinary, b)
	c.writeMu.Unlock()
	return id, ch, err
}

func (c *connState) freeStream(id uint32) {
	c.chMu.Lock()
	if c.chans != nil {
		delete(c.chans, id)
	}
	c.chMu.Unlock()
}

func (c *connState) writeFrame(f frame.Frame) error {
	b, err := frame.Encode(f)
	if err != nil {
		return err
	}
	c.writeMu.Lock()
	defer c.writeMu.Unlock()
	return c.ws.Write(context.Background(), websocket.MessageBinary, b)
}

// ── Worker ────────────────────────────────────────────────────────────────────

func runWorker(ctx context.Context, conn *connState, reqPayload []byte, res *workerResult) {
	beginPayload, _ := proto.Marshal(&framev1.BeginPayload{
		Method: echov1.Echo_Echo_FullMethodName,
	})

	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		// beginStream atomically allocates ID + sends BEGIN (keeps ID order).
		id, ch, err := conn.beginStream(beginPayload, 8)
		start := time.Now()
		if err != nil {
			res.errors++
			conn.freeStream(id)
			return
		}
		if err := conn.writeFrame(frame.Frame{Type: frame.TypeMSG, StreamID: id, Payload: reqPayload}); err != nil {
			res.errors++
			conn.freeStream(id)
			return
		}
		if err := conn.writeFrame(frame.Frame{Type: frame.TypeEND, StreamID: id}); err != nil {
			res.errors++
			conn.freeStream(id)
			return
		}

		// Read HEADER + MSG + END.
		if !drainUnary(ctx, ch) {
			res.errors++
			conn.freeStream(id)
			return
		}

		res.latencies = append(res.latencies, time.Since(start).Nanoseconds())
		res.completed++
		conn.freeStream(id)
	}
}

// drainUnary reads HEADER + MSG + END from ch, returning false on error or ctx done.
func drainUnary(ctx context.Context, ch <-chan frame.Frame) bool {
	// We expect exactly 3 frames: HEADER, MSG, END (in that order).
	for _, want := range []uint8{frame.TypeHEADER, frame.TypeMSG, frame.TypeEND} {
		select {
		case <-ctx.Done():
			return false
		case f, ok := <-ch:
			if !ok {
				return false
			}
			_ = want
			if want == frame.TypeEND {
				// Check for gRPC error status.
				var ep framev1.EndPayload
				if err := proto.Unmarshal(f.Payload, &ep); err == nil && ep.StatusCode != 0 {
					return false
				}
			}
		}
	}
	return true
}

// ── Shared output ─────────────────────────────────────────────────────────────

func printResults(totalCompleted, totalErrors int64, rps float64, allLatencies []int64) {
	fmt.Printf("── Results ───────────────────────────────────────────────\n")
	fmt.Printf("  Total RPCs:  %d\n", totalCompleted)
	fmt.Printf("  Throughput:  %.0f RPC/s\n", rps)
	if totalErrors > 0 {
		fmt.Printf("  Errors:      %d (%.1f%%)\n", totalErrors,
			100*float64(totalErrors)/float64(totalCompleted+totalErrors))
	} else {
		fmt.Printf("  Errors:      0\n")
	}
	if len(allLatencies) > 0 {
		sort.Slice(allLatencies, func(i, j int) bool { return allLatencies[i] < allLatencies[j] })
		n := len(allLatencies)
		fmt.Printf("  Latency:\n")
		fmt.Printf("    mean  %s\n", fmtNs(mean(allLatencies)))
		fmt.Printf("    p50   %s\n", fmtNs(allLatencies[n*50/100]))
		fmt.Printf("    p90   %s\n", fmtNs(allLatencies[n*90/100]))
		fmt.Printf("    p95   %s\n", fmtNs(allLatencies[n*95/100]))
		fmt.Printf("    p99   %s\n", fmtNs(allLatencies[n*99/100]))
		fmt.Printf("    max   %s\n", fmtNs(allLatencies[n-1]))
	}
}

// ── Formatting helpers ────────────────────────────────────────────────────────

func fmtNs(ns int64) string {
	d := time.Duration(ns)
	switch {
	case d < time.Microsecond:
		return fmt.Sprintf("%.1fns", float64(ns))
	case d < time.Millisecond:
		return fmt.Sprintf("%.1fµs", float64(ns)/1e3)
	default:
		return fmt.Sprintf("%.2fms", float64(ns)/1e6)
	}
}

func mean(ns []int64) int64 {
	if len(ns) == 0 {
		return 0
	}
	var sum int64
	for _, v := range ns {
		sum += v
	}
	return sum / int64(len(ns))
}
