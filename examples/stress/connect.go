package main

// Connect-protocol HTTP worker for cross-library benchmarking.
//
// Implements the Connect unary wire format:
//   POST <url>
//   Content-Type: application/proto
//   Body: raw proto bytes (no envelope framing for unary)
//
// The 5-byte envelope (application/connect+proto) is for streaming only;
// unary RPCs use the simpler application/proto content type.

import (
	"bytes"
	"context"
	"crypto/tls"
	"fmt"
	"io"
	"net"
	"net/http"
	"sync"
	"time"

	echov1 "github.com/wsgrpc/wsgrpc/echo/v1"
	"golang.org/x/net/http2"
	"google.golang.org/protobuf/proto"
)

func newHTTPClient(mode string) *http.Client {
	if mode == "connect-h2" {
		return &http.Client{
			Transport: &http2.Transport{
				AllowHTTP: true,
				DialTLSContext: func(ctx context.Context, network, addr string, _ *tls.Config) (net.Conn, error) {
					return (&net.Dialer{}).DialContext(ctx, network, addr)
				},
			},
		}
	}
	// HTTP/1.1 — per-conn client holds numStreams keep-alive connections.
	return &http.Client{
		Transport: &http.Transport{
			MaxIdleConns:        *numStreams,
			MaxIdleConnsPerHost: *numStreams,
			DisableCompression:  true,
		},
	}
}

func runConnectMode(ctx context.Context) {
	workerResults := make([]workerResult, *numConns**numStreams)
	var wg sync.WaitGroup

	var body []byte
	if *payloadSize > 0 {
		body, _ = proto.Marshal(&echov1.Msg{Value: string(make([]byte, *payloadSize))})
		fmt.Printf("Payload size: %d bytes (proto-encoded: %d bytes)\n\n", *payloadSize, len(body))
	} else {
		body, _ = proto.Marshal(&echov1.Msg{Value: "stress"})
	}

	// For H1: single pooled client with MaxIdleConnsPerHost = total goroutines.
	// For H2: one client per "conn" so each gets its own TCP connection,
	//         matching the grpcws topology of numConns connections.
	clients := make([]*http.Client, *numConns)
	for i := range clients {
		clients[i] = newHTTPClient(*mode)
	}

	idx := 0
	for c := range *numConns {
		for range *numStreams {
			i := idx
			idx++
			cl := clients[c]
			wg.Add(1)
			go func(res *workerResult) {
				defer wg.Done()
				res.latencies = make([]int64, 0, 4096)
				runConnectWorker(ctx, cl, body, res)
			}(&workerResults[i])
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

	elapsed := *duration
	rps := float64(totalCompleted) / elapsed.Seconds()

	printResults(totalCompleted, totalErrors, rps, allLatencies)
}

func runConnectWorker(ctx context.Context, client *http.Client, body []byte, res *workerResult) {
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		start := time.Now()

		req, err := http.NewRequestWithContext(ctx, http.MethodPost, *connectAddr, bytes.NewReader(body))
		if err != nil {
			res.errors++
			return
		}
		req.Header.Set("Content-Type", "application/proto")

		resp, err := client.Do(req)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			res.errors++
			continue
		}
		io.Copy(io.Discard, resp.Body)
		resp.Body.Close()

		if resp.StatusCode != http.StatusOK {
			res.errors++
			continue
		}

		res.latencies = append(res.latencies, time.Since(start).Nanoseconds())
		res.completed++
	}
}
