package main

// Streaming benchmark workers for all four fugue RPC kinds plus the
// Connect-ES server-streaming comparison mode.
//
// Connect-ES cannot do client-streaming or bidi-streaming from a browser
// (the Fetch API buffers the entire request body before sending). That
// structural limitation is exactly why fugue exists; the connect-stream mode
// demonstrates the server-streaming comparison where both protocols compete.

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"log"
	"net/http"
	"sort"
	"sync"
	"time"

	echov1 "github.com/fugue-rpc/fugue-go/echo/v1"
	framev1 "github.com/fugue-rpc/fugue-go/fugue/frame/v1"
	"github.com/fugue-rpc/fugue-go/frame"
	"google.golang.org/protobuf/proto"
)

// streamResult aggregates results from all streaming workers.
type streamResult struct {
	totalCompleted int64
	totalErrors    int64
	allLatencies   []int64
	allTTFM        []int64
}

// ── fugue server-streaming ───────────────────────────────────────────────────

func runStreamServerMode(runCtx, dialCtx context.Context) {
	fmt.Printf("Mode:        stream-server (fugue)\n")
	fmt.Printf("Connecting to %s\n", *addr)
	fmt.Printf("Connections: %d  Streams/conn: %d  Msgs/stream: %d  Duration: %s\n\n",
		*numConns, *numStreams, *msgsPerStream, *duration)

	beginPayload, _ := proto.Marshal(&framev1.BeginPayload{
		Method: echov1.Echo_EchoStreamN_FullMethodName,
	})
	reqPayload, _ := proto.Marshal(&echov1.StreamNReq{
		Value: "stress",
		Count: int32(*msgsPerStream),
	})

	sr := runGrpcwsStreamWorkers(runCtx, dialCtx, func(ctx context.Context, conn *connState, res *workerResult) {
		runStreamServerWorker(ctx, conn, beginPayload, reqPayload, res)
	})
	printStreamResults(sr)
}

func runStreamServerWorker(ctx context.Context, conn *connState, beginPayload, reqPayload []byte, res *workerResult) {
	for {
		if ctx.Err() != nil {
			return
		}
		id, ch, err := conn.beginStream(beginPayload, *msgsPerStream+8)
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

		var ttfm int64
		if !drainServerStream(ctx, ch, *msgsPerStream, start, &ttfm) {
			res.errors++
			conn.freeStream(id)
			return
		}

		res.latencies = append(res.latencies, time.Since(start).Nanoseconds())
		if ttfm > 0 {
			res.ttfmLatencies = append(res.ttfmLatencies, ttfm)
		}
		res.completed++
		conn.freeStream(id)
	}
}

// drainServerStream reads HEADER + n×MSG + END from ch.
// Sets *ttfm (nanoseconds from start) when the first MSG arrives.
func drainServerStream(ctx context.Context, ch <-chan frame.Frame, n int, start time.Time, ttfm *int64) bool {
	// HEADER
	select {
	case <-ctx.Done():
		return false
	case f, ok := <-ch:
		if !ok || f.Type != frame.TypeHEADER {
			return false
		}
	}
	// N×MSG
	for i := 0; i < n; i++ {
		select {
		case <-ctx.Done():
			return false
		case f, ok := <-ch:
			if !ok || f.Type != frame.TypeMSG {
				return false
			}
			if i == 0 {
				*ttfm = time.Since(start).Nanoseconds()
			}
		}
	}
	// END
	select {
	case <-ctx.Done():
		return false
	case f, ok := <-ch:
		if !ok || f.Type != frame.TypeEND {
			return false
		}
		var ep framev1.EndPayload
		if err := proto.Unmarshal(f.Payload, &ep); err == nil && ep.StatusCode != 0 {
			return false
		}
		return true
	}
}

// ── fugue client-streaming ───────────────────────────────────────────────────

func runStreamClientMode(runCtx, dialCtx context.Context) {
	fmt.Printf("Mode:        stream-client (fugue)\n")
	fmt.Printf("Connecting to %s\n", *addr)
	fmt.Printf("Connections: %d  Streams/conn: %d  Msgs/stream: %d  Duration: %s\n\n",
		*numConns, *numStreams, *msgsPerStream, *duration)

	beginPayload, _ := proto.Marshal(&framev1.BeginPayload{
		Method: echov1.Echo_EchoCollect_FullMethodName,
	})
	msgPayload, _ := proto.Marshal(&echov1.Msg{Value: "stress"})

	sr := runGrpcwsStreamWorkers(runCtx, dialCtx, func(ctx context.Context, conn *connState, res *workerResult) {
		runStreamClientWorker(ctx, conn, beginPayload, msgPayload, res)
	})
	printStreamResults(sr)
}

func runStreamClientWorker(ctx context.Context, conn *connState, beginPayload, msgPayload []byte, res *workerResult) {
	for {
		if ctx.Err() != nil {
			return
		}
		id, ch, err := conn.beginStream(beginPayload, 8)
		start := time.Now()
		if err != nil {
			res.errors++
			conn.freeStream(id)
			return
		}
		for i := 0; i < *msgsPerStream; i++ {
			if err := conn.writeFrame(frame.Frame{Type: frame.TypeMSG, StreamID: id, Payload: msgPayload}); err != nil {
				res.errors++
				conn.freeStream(id)
				return
			}
		}
		if err := conn.writeFrame(frame.Frame{Type: frame.TypeEND, StreamID: id}); err != nil {
			res.errors++
			conn.freeStream(id)
			return
		}
		// Server replies with HEADER + MSG + END (one concatenated response).
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

// ── fugue bidi-streaming ─────────────────────────────────────────────────────

func runStreamBidiMode(runCtx, dialCtx context.Context) {
	fmt.Printf("Mode:        stream-bidi (fugue)\n")
	fmt.Printf("Connecting to %s\n", *addr)
	fmt.Printf("Connections: %d  Streams/conn: %d  Msgs/stream: %d  Duration: %s\n\n",
		*numConns, *numStreams, *msgsPerStream, *duration)

	beginPayload, _ := proto.Marshal(&framev1.BeginPayload{
		Method: echov1.Echo_EchoBidi_FullMethodName,
	})
	msgPayload, _ := proto.Marshal(&echov1.Msg{Value: "stress"})

	sr := runGrpcwsStreamWorkers(runCtx, dialCtx, func(ctx context.Context, conn *connState, res *workerResult) {
		runStreamBidiWorker(ctx, conn, beginPayload, msgPayload, res)
	})
	printStreamResults(sr)
}

func runStreamBidiWorker(ctx context.Context, conn *connState, beginPayload, msgPayload []byte, res *workerResult) {
	for {
		if ctx.Err() != nil {
			return
		}
		id, ch, err := conn.beginStream(beginPayload, *msgsPerStream+8)
		start := time.Now()
		if err != nil {
			res.errors++
			conn.freeStream(id)
			return
		}
		// Send N messages then half-close.
		for i := 0; i < *msgsPerStream; i++ {
			if err := conn.writeFrame(frame.Frame{Type: frame.TypeMSG, StreamID: id, Payload: msgPayload}); err != nil {
				res.errors++
				conn.freeStream(id)
				return
			}
		}
		if err := conn.writeFrame(frame.Frame{Type: frame.TypeEND, StreamID: id}); err != nil {
			res.errors++
			conn.freeStream(id)
			return
		}
		// Drain HEADER + N×MSG + END.
		var ttfm int64
		if !drainServerStream(ctx, ch, *msgsPerStream, start, &ttfm) {
			res.errors++
			conn.freeStream(id)
			return
		}
		res.latencies = append(res.latencies, time.Since(start).Nanoseconds())
		if ttfm > 0 {
			res.ttfmLatencies = append(res.ttfmLatencies, ttfm)
		}
		res.completed++
		conn.freeStream(id)
	}
}

// ── Connect-ES server-streaming ───────────────────────────────────────────────
//
// Connect-ES cannot do client-streaming or bidi from a browser — the Fetch API
// buffers the full request body before the server sees any bytes. This mode
// provides the fair comparison point for stream-server only.

func runConnectStreamMode(ctx context.Context) {
	reqBytes, _ := proto.Marshal(&echov1.StreamNReq{
		Value: "stress",
		Count: int32(*msgsPerStream),
	})
	// Prepend the 5-byte Connect envelope header: flags=0x00 (data), then length.
	var env [5]byte
	binary.BigEndian.PutUint32(env[1:], uint32(len(reqBytes)))
	reqBody := append(env[:], reqBytes...)

	clients := make([]*http.Client, *numConns)
	for i := range clients {
		clients[i] = newHTTPClient("connect-h1")
	}

	workerResults := make([]workerResult, *numConns**numStreams)
	var wg sync.WaitGroup

	idx := 0
	for c := range *numConns {
		for range *numStreams {
			i := idx
			idx++
			cl := clients[c]
			wg.Add(1)
			go func(res *workerResult) {
				defer wg.Done()
				res.latencies = make([]int64, 0, 1024)
				res.ttfmLatencies = make([]int64, 0, 1024)
				runConnectStreamWorker(ctx, cl, reqBody, res)
			}(&workerResults[i])
		}
	}
	wg.Wait()

	var sr streamResult
	for i := range workerResults {
		sr.totalCompleted += workerResults[i].completed
		sr.totalErrors += workerResults[i].errors
		sr.allLatencies = append(sr.allLatencies, workerResults[i].latencies...)
		sr.allTTFM = append(sr.allTTFM, workerResults[i].ttfmLatencies...)
	}
	printStreamResults(sr)
}

func runConnectStreamWorker(ctx context.Context, client *http.Client, reqBody []byte, res *workerResult) {
	for {
		if ctx.Err() != nil {
			return
		}

		req, err := http.NewRequestWithContext(ctx, http.MethodPost, *connectStreamAddr, bytes.NewReader(reqBody))
		if err != nil {
			res.errors++
			return
		}
		req.Header.Set("Content-Type", "application/connect+proto")

		start := time.Now()
		resp, err := client.Do(req)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			res.errors++
			continue
		}
		if resp.StatusCode != http.StatusOK {
			io.Copy(io.Discard, resp.Body)
			resp.Body.Close()
			res.errors++
			continue
		}

		// Parse Connect streaming frames: [flags(1)][length(4 BE)][data...]
		// flags=0x00 is a data frame; flags=0x02 is an end-stream (trailers) frame.
		var msgCount int
		var firstMsg bool
		var ttfm int64
		var readErr bool
		var hdr [5]byte
		for {
			if _, err := io.ReadFull(resp.Body, hdr[:]); err != nil {
				readErr = true
				break
			}
			flags := hdr[0]
			length := binary.BigEndian.Uint32(hdr[1:])
			if length > 0 {
				if _, err := io.CopyN(io.Discard, resp.Body, int64(length)); err != nil {
					readErr = true
					break
				}
			}
			if flags&0x02 != 0 {
				break // end-stream frame (trailers)
			}
			msgCount++
			if !firstMsg {
				firstMsg = true
				ttfm = time.Since(start).Nanoseconds()
			}
		}
		resp.Body.Close()

		if readErr || msgCount != *msgsPerStream {
			res.errors++
			continue
		}

		res.latencies = append(res.latencies, time.Since(start).Nanoseconds())
		if ttfm > 0 {
			res.ttfmLatencies = append(res.ttfmLatencies, ttfm)
		}
		res.completed++
	}
}

// ── Shared worker launcher ────────────────────────────────────────────────────

func runGrpcwsStreamWorkers(runCtx, dialCtx context.Context, workerFn func(context.Context, *connState, *workerResult)) streamResult {
	workerResults := make([]workerResult, *numConns**numStreams)
	var wg sync.WaitGroup

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
				res.latencies = make([]int64, 0, 1024)
				res.ttfmLatencies = make([]int64, 0, 1024)
				workerFn(runCtx, conn, res)
			}(conn, &workerResults[idx])
		}
	}
	wg.Wait()

	var sr streamResult
	for i := range workerResults {
		sr.totalCompleted += workerResults[i].completed
		sr.totalErrors += workerResults[i].errors
		sr.allLatencies = append(sr.allLatencies, workerResults[i].latencies...)
		sr.allTTFM = append(sr.allTTFM, workerResults[i].ttfmLatencies...)
	}
	return sr
}

// ── Output ────────────────────────────────────────────────────────────────────

func printStreamResults(sr streamResult) {
	rps := float64(sr.totalCompleted) / duration.Seconds()
	printResults(sr.totalCompleted, sr.totalErrors, rps, sr.allLatencies)

	if len(sr.allTTFM) > 0 {
		sort.Slice(sr.allTTFM, func(i, j int) bool { return sr.allTTFM[i] < sr.allTTFM[j] })
		n := len(sr.allTTFM)
		fmt.Printf("  TTFM (time-to-first-message):\n")
		fmt.Printf("    p50   %s\n", fmtNs(sr.allTTFM[n*50/100]))
		fmt.Printf("    p99   %s\n", fmtNs(sr.allTTFM[n*99/100]))
	}

	totalMsgs := float64(sr.totalCompleted) * float64(*msgsPerStream)
	fmt.Printf("  Messages/s: %.0f\n", totalMsgs/duration.Seconds())
}
