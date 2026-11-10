package crush

import (
	"bytes"
	"encoding/binary"
	"fmt"
)

// Decode sniffs the encoding of src and decodes it into a Wrapper:
// JSON when the payload starts with '{', Ceph binary when the first
// word is the CRUSH magic, the text grammar otherwise. It is the single
// format-detection point for the package; callers that already know the
// format should call DecodeJson, DecodeBinary or DecodeText directly.
func Decode(src []byte) (m *Wrapper, err error) {
	// The text lexer panics on malformed input when no errHandler is
	// installed; surface it as an error instead of crashing callers.
	defer func() {
		if r := recover(); r != nil {
			m, err = nil, fmt.Errorf("crush: decode crush map: %v", r)
		}
	}()
	m = NewWrapper()
	switch {
	case bytes.HasPrefix(bytes.TrimLeft(src, " \t\n\r#"), []byte("{")):
		if err := m.DecodeJson(src); err != nil {
			return nil, fmt.Errorf("crush: decode json: %w", err)
		}
	case len(src) >= 4 && binary.LittleEndian.Uint32(src[:4]) == Magic:
		if err := m.DecodeBinary(src); err != nil {
			return nil, fmt.Errorf("crush: decode binary: %w", err)
		}
	default:
		if err := m.DecodeText(src); err != nil {
			return nil, fmt.Errorf("crush: decode text: %w", err)
		}
	}
	return m, nil
}
