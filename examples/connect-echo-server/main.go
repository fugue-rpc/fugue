// connect-echo-server implements the Echo service using the Connect protocol.
// It serves on :8090 and supports HTTP/1.1 and HTTP/2 (h2c, cleartext).
//
// Endpoints:
//
//	POST /echo.v1.Echo/Echo          – unary echo
//	POST /echo.v1.Echo/EchoStream    – server-streaming echo (5 messages)
package main

import (
	"context"
	"log/slog"
	"net/http"

	"connectrpc.com/connect"
	echov1 "github.com/wsgrpc/wsgrpc/echo/v1"
	"golang.org/x/net/http2"
	"golang.org/x/net/http2/h2c"
)

func main() {
	mux := http.NewServeMux()

	mux.Handle("/echo.v1.Echo/Echo", connect.NewUnaryHandler(
		"/echo.v1.Echo/Echo",
		func(_ context.Context, req *connect.Request[echov1.Msg]) (*connect.Response[echov1.Msg], error) {
			return connect.NewResponse(&echov1.Msg{Value: req.Msg.Value}), nil
		},
	))

	mux.Handle("/echo.v1.Echo/EchoStream", connect.NewServerStreamHandler(
		"/echo.v1.Echo/EchoStream",
		func(_ context.Context, req *connect.Request[echov1.Msg], stream *connect.ServerStream[echov1.Msg]) error {
			for range 5 {
				if err := stream.Send(&echov1.Msg{Value: req.Msg.Value}); err != nil {
					return err
				}
			}
			return nil
		},
	))

	mux.Handle("/echo.v1.Echo/EchoStreamN", connect.NewServerStreamHandler(
		"/echo.v1.Echo/EchoStreamN",
		func(_ context.Context, req *connect.Request[echov1.StreamNReq], stream *connect.ServerStream[echov1.Msg]) error {
			n := int(req.Msg.Count)
			if n <= 0 {
				n = 1
			}
			for range n {
				if err := stream.Send(&echov1.Msg{Value: req.Msg.Value}); err != nil {
					return err
				}
			}
			return nil
		},
	))

	// h2c wraps the mux so the same listener speaks HTTP/1.1 and HTTP/2.
	handler := h2c.NewHandler(mux, &http2.Server{})

	slog.Info("connect echo server listening", "addr", ":8090",
		"protocols", "HTTP/1.1 + HTTP/2 (h2c)")
	if err := http.ListenAndServe(":8090", handler); err != nil {
		slog.Error("server exited", "err", err)
	}
}
