package main

import (
	"context"
	"io"
	"log/slog"
	"net/http"
	_ "net/http/pprof" // registers /debug/pprof/ on the default mux
	"strings"

	echov1 "github.com/grpcws/wsgrpc/echo/v1"
	"github.com/grpcws/wsgrpc"
	"google.golang.org/grpc"
)

func main() {
	srv := wsgrpc.NewServer(wsgrpc.WithLogger(slog.Default()))
	echov1.RegisterEchoServer(srv, &echoImpl{})

	// pprof on a dedicated port so it never contends with gRPC traffic.
	go func() {
		slog.Info("pprof listening", "addr", ":6060", "path", "/debug/pprof/")
		if err := http.ListenAndServe(":6060", nil); err != nil {
			slog.Error("pprof server exited", "err", err)
		}
	}()

	// gRPC-over-WebSocket on its own mux so pprof routes stay separate.
	mux := http.NewServeMux()
	mux.Handle("/wsgrpc/", srv)

	slog.Info("echo server listening", "addr", ":8080", "path", "/wsgrpc/")
	if err := http.ListenAndServe(":8080", mux); err != nil {
		slog.Error("server exited", "err", err)
	}
}

type echoImpl struct{ echov1.UnimplementedEchoServer }

func (e *echoImpl) Echo(_ context.Context, req *echov1.Msg) (*echov1.Msg, error) {
	return &echov1.Msg{Value: req.Value}, nil
}

func (e *echoImpl) EchoStream(req *echov1.Msg, stream grpc.ServerStreamingServer[echov1.Msg]) error {
	for i := 0; i < 5; i++ {
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

func (e *echoImpl) EchoStreamN(req *echov1.StreamNReq, stream grpc.ServerStreamingServer[echov1.Msg]) error {
	n := int(req.Count)
	if n <= 0 {
		n = 1
	}
	for i := 0; i < n; i++ {
		if err := stream.Send(&echov1.Msg{Value: req.Value}); err != nil {
			return err
		}
	}
	return nil
}
