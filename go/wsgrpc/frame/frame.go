// Package frame implements the grpcws binary framing protocol.
// See docs/wire-format.md for the full specification.
package frame

import (
	"encoding/binary"
	"errors"
	"fmt"
)

// Frame type constants as defined in the wire format spec.
const (
	TypeBEGIN  = uint8(0x01)
	TypeMSG    = uint8(0x02)
	TypeEND    = uint8(0x03)
	TypeRESET  = uint8(0x04)
	TypeHEADER = uint8(0x06)

	HeaderSize     = 9
	MaxPayloadSize = 4 * 1024 * 1024 // 4 MiB — see wire-format.md §2
)

// Sentinel errors returned by Decode.
var (
	ErrShortHeader    = errors.New("frame: buffer too short for 9-byte header")
	ErrPayloadTooLarge = errors.New("frame: payload length exceeds MAX_PAYLOAD_SIZE")
	ErrShortPayload   = errors.New("frame: buffer shorter than declared payload length")
)

// Frame is a decoded grpcws frame.
type Frame struct {
	Type     uint8
	StreamID uint32
	Payload  []byte
}

// Encode serialises f into the 9-byte header + payload wire format.
// Returns an error only if len(f.Payload) > MaxPayloadSize.
func Encode(f Frame) ([]byte, error) {
	if len(f.Payload) > MaxPayloadSize {
		return nil, fmt.Errorf("frame: payload %d bytes exceeds MAX_PAYLOAD_SIZE (%d)",
			len(f.Payload), MaxPayloadSize)
	}
	buf := make([]byte, HeaderSize+len(f.Payload))
	buf[0] = f.Type
	binary.BigEndian.PutUint32(buf[1:5], f.StreamID)
	binary.BigEndian.PutUint32(buf[5:9], uint32(len(f.Payload)))
	copy(buf[HeaderSize:], f.Payload)
	return buf, nil
}

// Decode parses a complete frame from buf.
// buf must contain exactly the header plus the payload declared in the header.
// Extra trailing bytes are not consumed and do not cause an error.
func Decode(buf []byte) (Frame, error) {
	if len(buf) < HeaderSize {
		return Frame{}, ErrShortHeader
	}
	payloadLen := binary.BigEndian.Uint32(buf[5:9])
	if payloadLen > MaxPayloadSize {
		return Frame{}, ErrPayloadTooLarge
	}
	need := HeaderSize + int(payloadLen)
	if len(buf) < need {
		return Frame{}, ErrShortPayload
	}
	payload := make([]byte, payloadLen)
	copy(payload, buf[HeaderSize:need])
	return Frame{
		Type:     buf[0],
		StreamID: binary.BigEndian.Uint32(buf[1:5]),
		Payload:  payload,
	}, nil
}
