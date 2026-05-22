module github.com/fugue-rpc/stress

go 1.26.1

require (
	github.com/coder/websocket v1.8.14
	github.com/fugue-rpc/fugue v0.0.0
	golang.org/x/net v0.49.0
	google.golang.org/protobuf v1.36.11
)

require (
	golang.org/x/sys v0.40.0 // indirect
	golang.org/x/text v0.33.0 // indirect
	google.golang.org/genproto/googleapis/rpc v0.0.0-20260120221211-b8f7ae30c516 // indirect
	google.golang.org/grpc v1.80.0 // indirect
)

replace github.com/fugue-rpc/fugue => ../../wsgrpc-go
