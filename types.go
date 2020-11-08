package goldilocks

import (
	"encoding/binary"
	"errors"
	"fmt"
	"math"

	"github.com/jackc/pgio"
)

const (
	textFormat   = 0
	binaryFormat = 1
)

// PostgreSQL oids for builtin types
const (
	int8OID   = 20
	int2OID   = 21
	int4OID   = 23
	textOID   = 25
	float4OID = 700
	float8OID = 701
)

func readString(dst *string) (int16, valueReaderFunc) {
	return textFormat, func(buf []byte) error {
		if buf == nil {
			return errors.New("NULL cannot be converted to string")
		}
		*dst = string(buf)
		return nil
	}
}

func writeString(buf []byte, src string) ([]byte, uint32, int16) {
	buf = append(buf, src...)
	return buf, 0, textFormat
}

func readInt16(dst *int16) (int16, valueReaderFunc) {
	return binaryFormat, func(buf []byte) error {
		if buf == nil {
			return errors.New("NULL cannot be converted to int16")
		}
		if len(buf) != 2 {
			return fmt.Errorf("int16 requires data length of 2, got %d", len(buf))
		}
		*dst = int16(binary.BigEndian.Uint16(buf))
		return nil
	}
}

func writeInt16(buf []byte, src int16) ([]byte, uint32, int16) {
	return pgio.AppendInt16(buf, src), int2OID, binaryFormat
}

func readInt32(dst *int32) (int16, valueReaderFunc) {
	return binaryFormat, func(buf []byte) error {
		if buf == nil {
			return errors.New("NULL cannot be converted to int32")
		}
		if len(buf) != 4 {
			return fmt.Errorf("int32 requires data length of 4, got %d", len(buf))
		}
		*dst = int32(binary.BigEndian.Uint32(buf))
		return nil
	}
}

func writeInt32(buf []byte, src int32) ([]byte, uint32, int16) {
	return pgio.AppendInt32(buf, src), int4OID, binaryFormat
}

func readInt64(dst *int64) (int16, valueReaderFunc) {
	return binaryFormat, func(buf []byte) error {
		if buf == nil {
			return errors.New("NULL cannot be converted to int64")
		}
		if len(buf) != 8 {
			return fmt.Errorf("int64 requires data length of 8, got %d", len(buf))
		}
		*dst = int64(binary.BigEndian.Uint64(buf))
		return nil
	}
}

func writeInt64(buf []byte, src int64) ([]byte, uint32, int16) {
	return pgio.AppendInt64(buf, src), int8OID, binaryFormat
}

func readFloat32(dst *float32) (int16, valueReaderFunc) {
	return binaryFormat, func(buf []byte) error {
		if buf == nil {
			return errors.New("NULL cannot be converted to float32")
		}
		if len(buf) != 4 {
			return fmt.Errorf("float32 requires data length of 4, got %d", len(buf))
		}
		*dst = math.Float32frombits(binary.BigEndian.Uint32(buf))
		return nil
	}
}

func writeFloat32(buf []byte, src float32) ([]byte, uint32, int16) {
	return pgio.AppendUint32(buf, math.Float32bits(src)), float4OID, binaryFormat
}

func readFloat64(dst *float64) (int16, valueReaderFunc) {
	return binaryFormat, func(buf []byte) error {
		if buf == nil {
			return errors.New("NULL cannot be converted to float64")
		}
		if len(buf) != 8 {
			return fmt.Errorf("float64 requires data length of 8, got %d", len(buf))
		}
		*dst = math.Float64frombits(binary.BigEndian.Uint64(buf))
		return nil
	}
}

func writeFloat64(buf []byte, src float64) ([]byte, uint32, int16) {
	return pgio.AppendUint64(buf, math.Float64bits(src)), float8OID, binaryFormat
}
