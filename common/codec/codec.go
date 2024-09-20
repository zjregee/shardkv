package codec

import (
	"fmt"
)

const (
	encGroupSize = 8
	encMarker    = byte(0xFF)
	encPad       = byte(0x0)
)

var pads = make([]byte, encGroupSize)

// EncodeBytes guarantees the encoded value is in ascending order for comparison.
// Refer:
// - https://github.com/talent-plan/tinykv/blob/course/kv/util/codec/codec.go#L27
// - https://github.com/facebook/mysql-5.6/wiki/MyRocks-record-format#memcomparable-format
func EncodeBytes(data []byte) []byte {
	dLen := len(data)
	result := make([]byte, 0, (dLen/encGroupSize+1)*(encGroupSize+1)+8)
	for idx := 0; idx <= dLen; idx += encGroupSize {
		remain := dLen - idx
		padCount := 0
		if remain >= encGroupSize {
			result = append(result, data[idx:idx+encGroupSize]...)
		} else {
			padCount = encGroupSize - remain
			result = append(result, data[idx:]...)
			result = append(result, pads[:padCount]...)
		}
		marker := encMarker - byte(padCount)
		result = append(result, marker)
	}
	return result
}

// DecodeBytes decodes bytes which is encoded by EncodeBytes before, returns the leftover bytes and decoded value if no error.
// Refer: https://github.com/talent-plan/tinykv/blob/course/kv/util/codec/codec.go#L52
func DecodeBytes(b []byte) ([]byte, []byte, error) {
	data := make([]byte, 0, len(b))
	for {
		if len(b) < encGroupSize+1 {
			return nil, nil, fmt.Errorf("insufficient bytes to decode value: %d", len(b))
		}
		groupBytes := b[:encGroupSize+1]
		group := groupBytes[:encGroupSize]
		marker := groupBytes[encGroupSize]
		padCount := encMarker - marker
		if padCount > encGroupSize {
			return nil, nil, fmt.Errorf("invalid marker byte, group bytes %q", groupBytes)
		}
		realGroupSize := encGroupSize - padCount
		data = append(data, group[:realGroupSize]...)
		b = b[encGroupSize+1:]
		if padCount != 0 {
			for _, v := range group[realGroupSize:] {
				if v != encPad {
					return nil, nil, fmt.Errorf("invalid padding byte, group bytes %q", groupBytes)
				}
			}
			break
		}
	}
	return b, data, nil
}
