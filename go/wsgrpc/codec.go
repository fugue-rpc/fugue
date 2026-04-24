package wsgrpc

// Codec is the interface for pluggable message serialisation on user-defined
// message types. Implement this to use vtprotobuf or another marshaller.
// Name returns a human-readable identifier used in log messages and debug output.
//
// The default codec (used when WithCodec is not called) marshals and unmarshals
// via google.golang.org/protobuf/proto.
type Codec interface {
	Marshal(v any) ([]byte, error)
	Unmarshal(data []byte, v any) error
	Name() string
}

// WithCodec sets the Codec used to marshal and unmarshal user messages.
// The codec is applied per-stream for SendMsg and RecvMsg calls.
// The default proto codec is used when WithCodec is not called.
func WithCodec(c Codec) Option {
	return func(s *Server) { s.codec = c }
}
