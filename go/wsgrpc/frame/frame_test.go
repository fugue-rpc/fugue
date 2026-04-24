package frame_test

import (
	"bytes"
	"testing"

	"github.com/wsgrpc/wsgrpc/frame"
)

// allTypes lists every defined frame type so we can verify round-trips for each.
var allTypes = []struct {
	name string
	typ  uint8
}{
	{"BEGIN", frame.TypeBEGIN},
	{"MSG", frame.TypeMSG},
	{"END", frame.TypeEND},
	{"RESET", frame.TypeRESET},
	{"HEADER", frame.TypeHEADER},
}

// TestRoundTrip encodes then decodes a frame for every type and verifies
// the fields are preserved exactly.
func TestRoundTrip(t *testing.T) {
	payload := []byte("hello grpcws")

	for _, tc := range allTypes {
		t.Run(tc.name, func(t *testing.T) {
			orig := frame.Frame{Type: tc.typ, StreamID: 42, Payload: payload}

			encoded, err := frame.Encode(orig)
			if err != nil {
				t.Fatalf("Encode: %v", err)
			}
			if len(encoded) != frame.HeaderSize+len(payload) {
				t.Fatalf("encoded length: want %d, got %d", frame.HeaderSize+len(payload), len(encoded))
			}

			got, err := frame.Decode(encoded)
			if err != nil {
				t.Fatalf("Decode: %v", err)
			}

			if got.Type != orig.Type {
				t.Errorf("Type: want 0x%02x, got 0x%02x", orig.Type, got.Type)
			}
			if got.StreamID != orig.StreamID {
				t.Errorf("StreamID: want %d, got %d", orig.StreamID, got.StreamID)
			}
			if !bytes.Equal(got.Payload, orig.Payload) {
				t.Errorf("Payload: want %q, got %q", orig.Payload, got.Payload)
			}
		})
	}
}

func TestEmptyPayload(t *testing.T) {
	orig := frame.Frame{Type: frame.TypeHEADER, StreamID: 1, Payload: nil}
	encoded, err := frame.Encode(orig)
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}
	if len(encoded) != frame.HeaderSize {
		t.Fatalf("empty payload: want %d bytes, got %d", frame.HeaderSize, len(encoded))
	}
	got, err := frame.Decode(encoded)
	if err != nil {
		t.Fatalf("Decode: %v", err)
	}
	if len(got.Payload) != 0 {
		t.Errorf("Payload: want empty, got %q", got.Payload)
	}
}

// TestHeaderByteLayout verifies the exact byte positions described in the wire format spec.
func TestHeaderByteLayout(t *testing.T) {
	f := frame.Frame{Type: frame.TypeMSG, StreamID: 0x01020304, Payload: []byte{0xAB, 0xCD}}
	encoded, _ := frame.Encode(f)

	// Byte 0: type
	if encoded[0] != frame.TypeMSG {
		t.Errorf("byte[0] type: want 0x%02x, got 0x%02x", frame.TypeMSG, encoded[0])
	}
	// Bytes 1-4: stream_id big-endian
	if encoded[1] != 0x01 || encoded[2] != 0x02 || encoded[3] != 0x03 || encoded[4] != 0x04 {
		t.Errorf("bytes[1:5] stream_id: want [01 02 03 04], got [%02x %02x %02x %02x]",
			encoded[1], encoded[2], encoded[3], encoded[4])
	}
	// Bytes 5-8: payload_length big-endian (2 bytes payload → 0x00000002)
	if encoded[5] != 0x00 || encoded[6] != 0x00 || encoded[7] != 0x00 || encoded[8] != 0x02 {
		t.Errorf("bytes[5:9] payload_length: want [00 00 00 02], got [%02x %02x %02x %02x]",
			encoded[5], encoded[6], encoded[7], encoded[8])
	}
	// Bytes 9-10: payload
	if encoded[9] != 0xAB || encoded[10] != 0xCD {
		t.Errorf("bytes[9:11] payload: want [ab cd], got [%02x %02x]", encoded[9], encoded[10])
	}
}

func TestMaxStreamID(t *testing.T) {
	f := frame.Frame{Type: frame.TypeMSG, StreamID: 0xFFFFFFFF, Payload: []byte("x")}
	encoded, err := frame.Encode(f)
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}
	got, err := frame.Decode(encoded)
	if err != nil {
		t.Fatalf("Decode: %v", err)
	}
	if got.StreamID != 0xFFFFFFFF {
		t.Errorf("StreamID: want 0xFFFFFFFF, got 0x%08X", got.StreamID)
	}
}

// TestDecodeExtraTrailingBytes verifies Decode ignores bytes beyond the declared payload.
func TestDecodeExtraTrailingBytes(t *testing.T) {
	orig := frame.Frame{Type: frame.TypeMSG, StreamID: 1, Payload: []byte("abc")}
	encoded, _ := frame.Encode(orig)
	withTrailing := append(encoded, 0xFF, 0xFF)

	got, err := frame.Decode(withTrailing)
	if err != nil {
		t.Fatalf("Decode with trailing bytes: %v", err)
	}
	if !bytes.Equal(got.Payload, orig.Payload) {
		t.Errorf("Payload: want %q, got %q", orig.Payload, got.Payload)
	}
}

func TestEncodePayloadTooLarge(t *testing.T) {
	oversized := make([]byte, frame.MaxPayloadSize+1)
	_, err := frame.Encode(frame.Frame{Type: frame.TypeMSG, StreamID: 1, Payload: oversized})
	if err == nil {
		t.Error("Encode with oversized payload: want error, got nil")
	}
}

func TestDecodeShortHeader(t *testing.T) {
	_, err := frame.Decode([]byte{0x02, 0x00, 0x00}) // only 3 bytes
	if err != frame.ErrShortHeader {
		t.Errorf("want ErrShortHeader, got %v", err)
	}
}

func TestDecodePayloadTooLarge(t *testing.T) {
	// Craft a header declaring MaxPayloadSize+1 bytes.
	buf := make([]byte, frame.HeaderSize)
	buf[0] = frame.TypeMSG
	// stream_id = 1
	buf[4] = 1
	// payload_length = MaxPayloadSize + 1
	size := uint32(frame.MaxPayloadSize + 1)
	buf[5] = byte(size >> 24)
	buf[6] = byte(size >> 16)
	buf[7] = byte(size >> 8)
	buf[8] = byte(size)

	_, err := frame.Decode(buf)
	if err != frame.ErrPayloadTooLarge {
		t.Errorf("want ErrPayloadTooLarge, got %v", err)
	}
}

func TestDecodeShortPayload(t *testing.T) {
	// Header declares 10 bytes but we only provide 5.
	buf := make([]byte, frame.HeaderSize+5)
	buf[0] = frame.TypeMSG
	buf[5] = 0
	buf[6] = 0
	buf[7] = 0
	buf[8] = 10 // declares 10 bytes
	// only 5 bytes of payload follow

	_, err := frame.Decode(buf)
	if err != frame.ErrShortPayload {
		t.Errorf("want ErrShortPayload, got %v", err)
	}
}

// TestMaxPayloadSizeAtLimit verifies that exactly MaxPayloadSize bytes is accepted.
func TestMaxPayloadSizeAtLimit(t *testing.T) {
	payload := make([]byte, frame.MaxPayloadSize)
	payload[0] = 0xDE
	payload[frame.MaxPayloadSize-1] = 0xAD

	encoded, err := frame.Encode(frame.Frame{Type: frame.TypeMSG, StreamID: 1, Payload: payload})
	if err != nil {
		t.Fatalf("Encode at limit: %v", err)
	}
	got, err := frame.Decode(encoded)
	if err != nil {
		t.Fatalf("Decode at limit: %v", err)
	}
	if got.Payload[0] != 0xDE || got.Payload[frame.MaxPayloadSize-1] != 0xAD {
		t.Error("payload boundary bytes corrupted")
	}
}
