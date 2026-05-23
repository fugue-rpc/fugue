# @fugue-rpc/react — Performance Results

Platform: Windows 11, i7-11700F @ 2.50 GHz, Node.js v22, Go 1.26  
Echo server: `examples/echo-server` (Go, loopback :8080)  
Test harness: vitest + jsdom + `@testing-library/react`  
Date: 2026-05-23

---

## React hook throughput

These numbers reflect the full React hook stack: jsdom render, `act()` scheduling,
signal state transitions, and `waitFor` polling — on top of the WebSocket transport.

| Hook | Test | Result |
|------|------|--------|
| `useUnary` | 50 sequential calls, single hook instance | **13 RPC/s** |
| `useServerStream` | 10 concurrent streams, 5 messages each | **541 msg/s** |
| `useBidiStream` | 5 concurrent bidi streams, 5 messages each | **330 msg/s** |

The `useUnary` sequential figure is dominated by React's synchronous re-render
cycle (idle → loading → success → reset per call), not the transport. The
underlying transport delivers ~64,000 unary RPC/s at 100 concurrent streams
when called directly — see `stress-results.txt`.

---

## Transport-level comparison vs leading HTTP-based alternative

The underlying `@fugue-rpc/transport` was benchmarked against the leading
HTTP/1.1-based browser RPC library using the Go stress tool
(`examples/stress/`). Full numbers: `comparison-results.txt`.

### Unary RPC throughput

| Concurrency | fugue (WebSocket) | Leading alternative (HTTP/1.1) | Ratio |
|-------------|-------------------|-------------------------------|-------|
| 1 stream | ~8,000 RPC/s | ~7,900 RPC/s | 1.0× |
| 100 streams | 60,164 RPC/s | 59,942 RPC/s | 1.0× |
| 1,000 streams | **76,037 RPC/s** | 40,658 RPC/s | **1.9×** |

At moderate concurrency both libraries are equivalent. At 1,000 concurrent
streams, WebSocket frame-level multiplexing keeps TCP connections at O(connections)
while HTTP/1.1 keep-alive requires O(connections × streams) sockets. The OS TCP
stack on Windows starts failing at this scale: the leading alternative's p99
latency spiked to **10,470 ms**; fugue's p99 was **73 ms**.

### Latency at 100 concurrent streams

| Metric | fugue | Leading alternative |
|--------|-------|---------------------|
| mean | 1.40 ms | 1.67 ms |
| p50 | 1.01 ms | 1.01 ms |
| p90 | 3.03 ms | 4.43 ms |
| p99 | 7.96 ms | 10.02 ms |

### Server-streaming throughput

| Metric | fugue | Leading alternative | Ratio |
|--------|-------|---------------------|-------|
| Streams/s | 6,996 | 292 | **24×** |
| Messages/s | 699,603 | 29,150 | **24×** |
| Time-to-first-message p50 | 8 ms | 76 ms | **9.5× lower** |

HTTP/1.1 server-streaming serialises frames over a single response body.
WebSocket sends each frame independently with no head-of-line stall between
streams sharing the same connection.

---

## Capability gap

The leading alternative uses the browser Fetch API, which buffers the entire
request body before sending. This makes client-streaming and bidirectional
streaming impossible from a browser.

| RPC kind | fugue (browser) | Leading alternative (browser) |
|----------|-----------------|-------------------------------|
| Unary | ✓ | ✓ |
| Server-streaming | ✓ | ✓ |
| Client-streaming | ✓ | ✗ not possible |
| Bidirectional | ✓ | ✗ not possible |

fugue uses a single long-lived WebSocket connection, which allows true
full-duplex streaming. Client-streaming (`useBidiStream` with `halfClose()`)
and bidirectional streaming have no equivalent in any leading browser RPC
library.

---

## Notes

- All transport benchmarks run Go client → Go server on Windows loopback (no
  network hop). Linux/epoll would be faster; CGO I/O overhead accounts for ~25%
  of CPU on Windows.
- React hook numbers include jsdom, `act()`, and vitest process overhead on top
  of the transport. Production browser performance will differ.
- For high-frequency streams, bypass the hook layer and use
  `transport.openStream()` directly — see `@fugue-rpc/transport` README.
