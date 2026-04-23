// stress hammers the grpcws echo server and reports throughput + latency.
//
// Usage:
//
//	stress -addr ws://localhost:8080/wsgrpc/ -conns 10 -streams 10 -duration 30s
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
	"sync/atomic"
	"syscall"
	"time"

	"github.com/coder/websocket"
	echov1 "github.com/grpcws/wsgrpc/echo/v1"
	framev1 "github.com/grpcws/wsgrpc/grpcws/frame/v1"
	"github.com/grpcws/wsgrpc/frame"
	"google.golang.org/protobuf/proto"
)

var (
	addr       = flag.String("addr", "ws://localhost:8080/wsgrpc/", "echo server WebSocket address")
	numConns   = flag.Int("conns", 10, "number of WebSocket connections")
	numStreams  = flag.Int("streams", 10, "concurrent streams per connection")
	duration   = flag.Duration("duration", 30*time.Second, "test duration")
	cpuProfile = flag.String("cpuprofile", "", "write CPU profile to file")
	memProfile = flag.String("memprofile", "", "write heap profile to file")
)

// workerResult holds per-goroutine stats (no mutex needed — one goroutine owns it).
type workerResult struct {
	completed int64
	errors    int64
	latencies []int64 // nanoseconds
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

	fmt.Printf("Connecting to %s\n", *addr)
	fmt.Printf("Connections: %d  Streams/conn: %d  Duration: %s\n\n",
		*numConns, *numStreams, *duration)

	workerResults := make([]workerResult, *numConns**numStreams)
	var wg sync.WaitGroup

	// Shared request payload — proto-encoded Msg.
	reqPayload, _ := proto.Marshal(&echov1.Msg{Value: "stress"})

	workerIdx := 0
	for c := 0; c < *numConns; c++ {
		conn, err := newConn(ctx, *addr)
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

	// Merge results.
	var totalCompleted, totalErrors int64
	var allLatencies []int64
	for i := range workerResults {
		totalCompleted += workerResults[i].completed
		totalErrors += workerResults[i].errors
		allLatencies = append(allLatencies, workerResults[i].latencies...)
	}

	elapsed := *duration
	rps := float64(totalCompleted) / elapsed.Seconds()

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

// ── Per-connection state ──────────────────────────────────────────────────────

type connState struct {
	ws      *websocket.Conn
	nextID  atomic.Uint32
	writeMu sync.Mutex
	chMu    sync.RWMutex
	chans   map[uint32]chan frame.Frame
}

func newConn(ctx context.Context, addr string) (*connState, error) {
	ws, _, err := websocket.Dial(ctx, addr, nil)
	if err != nil {
		return nil, err
	}
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
		f, err := frame.Decode(msg)
		if err != nil {
			continue
		}
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

func (c *connState) allocStream() (uint32, <-chan frame.Frame) {
	id := c.nextID.Add(1)
	ch := make(chan frame.Frame, 8)
	c.chMu.Lock()
	if c.chans != nil {
		c.chans[id] = ch
	}
	c.chMu.Unlock()
	return id, ch
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

		id, ch := conn.allocStream()
		start := time.Now()

		if err := conn.writeFrame(frame.Frame{Type: frame.TypeBEGIN, StreamID: id, Payload: beginPayload}); err != nil {
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
