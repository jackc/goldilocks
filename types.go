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

type NullString struct {
	Value string
	Valid bool
}

func (n NullString) EncodeParam(buf []byte) ([]byte, uint32, int16) {
	if n.Valid {
		return writeString(buf, n.Value)
	}
	return nil, 0, textFormat
}

func (*NullString) ResultFormat() int16 {
	return textFormat
}

func (n *NullString) DecodeResult(buf []byte) error {
	if buf == nil {
		*n = NullString{Valid: false}
		return nil
	}

	n.Valid = true
	return readNotNullString(buf, &n.Value)
}

func readString(dst *string) (int16, valueReaderFunc) {
	return textFormat, func(buf []byte) error {
		if buf == nil {
			return errors.New("NULL cannot be converted to string")
		}
		return readNotNullString(buf, dst)
	}
}

func readNotNullString(buf []byte, dst *string) error {
	*dst = string(buf)
	return nil
}

func writeString(buf []byte, src string) ([]byte, uint32, int16) {
	buf = append(buf, src...)
	return buf, 0, textFormat
}

type NullInt16 struct {
	Value int16
	Valid bool
}

func (n NullInt16) EncodeParam(buf []byte) ([]byte, uint32, int16) {
	if n.Valid {
		return writeInt16(buf, n.Value)
	}
	return nil, 0, binaryFormat
}

func (*NullInt16) ResultFormat() int16 {
	return binaryFormat
}

func (n *NullInt16) DecodeResult(buf []byte) error {
	if buf == nil {
		*n = NullInt16{Valid: false}
		return nil
	}

	n.Valid = true
	return readNotNullInt16(buf, &n.Value)
}

func readInt16(dst *int16) (int16, valueReaderFunc) {
	return binaryFormat, func(buf []byte) error {
		if buf == nil {
			return errors.New("NULL cannot be converted to int16")
		}
		return readNotNullInt16(buf, dst)
	}
}

func readNotNullInt16(buf []byte, dst *int16) error {
	if len(buf) != 2 {
		return fmt.Errorf("int16 requires data length of 2, got %d", len(buf))
	}
	*dst = int16(binary.BigEndian.Uint16(buf))
	return nil
}

func writeInt16(buf []byte, src int16) ([]byte, uint32, int16) {
	return pgio.AppendInt16(buf, src), int2OID, binaryFormat
}

type NullInt32 struct {
	Value int32
	Valid bool
}

func (n NullInt32) EncodeParam(buf []byte) ([]byte, uint32, int16) {
	if n.Valid {
		return writeInt32(buf, n.Value)
	}
	return nil, 0, binaryFormat
}

func (*NullInt32) ResultFormat() int16 {
	return binaryFormat
}

func (n *NullInt32) DecodeResult(buf []byte) error {
	if buf == nil {
		*n = NullInt32{Valid: false}
		return nil
	}

	n.Valid = true
	return readNotNullInt32(buf, &n.Value)
}

func readInt32(dst *int32) (int16, valueReaderFunc) {
	return binaryFormat, func(buf []byte) error {
		if buf == nil {
			return errors.New("NULL cannot be converted to int32")
		}
		return readNotNullInt32(buf, dst)
	}
}

func readNotNullInt32(buf []byte, dst *int32) error {
	if len(buf) != 4 {
		return fmt.Errorf("int32 requires data length of 4, got %d", len(buf))
	}
	*dst = int32(binary.BigEndian.Uint32(buf))
	return nil
}

func writeInt32(buf []byte, src int32) ([]byte, uint32, int16) {
	return pgio.AppendInt32(buf, src), int4OID, binaryFormat
}

type NullInt64 struct {
	Value int64
	Valid bool
}

func (n NullInt64) EncodeParam(buf []byte) ([]byte, uint32, int16) {
	if n.Valid {
		return writeInt64(buf, n.Value)
	}
	return nil, 0, binaryFormat
}

func (*NullInt64) ResultFormat() int16 {
	return binaryFormat
}

func (n *NullInt64) DecodeResult(buf []byte) error {
	if buf == nil {
		*n = NullInt64{Valid: false}
		return nil
	}

	n.Valid = true
	return readNotNullInt64(buf, &n.Value)
}

func readInt64(dst *int64) (int16, valueReaderFunc) {
	return binaryFormat, func(buf []byte) error {
		if buf == nil {
			return errors.New("NULL cannot be converted to int64")
		}
		return readNotNullInt64(buf, dst)
	}
}

func readNotNullInt64(buf []byte, dst *int64) error {
	if len(buf) != 8 {
		return fmt.Errorf("int64 requires data length of 8, got %d", len(buf))
	}
	*dst = int64(binary.BigEndian.Uint64(buf))
	return nil
}

func writeInt64(buf []byte, src int64) ([]byte, uint32, int16) {
	return pgio.AppendInt64(buf, src), int8OID, binaryFormat
}

type NullFloat32 struct {
	Value float32
	Valid bool
}

func (n NullFloat32) EncodeParam(buf []byte) ([]byte, uint32, int16) {
	if n.Valid {
		return writeFloat32(buf, n.Value)
	}
	return nil, 0, binaryFormat
}

func (*NullFloat32) ResultFormat() int16 {
	return binaryFormat
}

func (n *NullFloat32) DecodeResult(buf []byte) error {
	if buf == nil {
		*n = NullFloat32{Valid: false}
		return nil
	}

	n.Valid = true
	return readNotNullFloat32(buf, &n.Value)
}

func readFloat32(dst *float32) (int16, valueReaderFunc) {
	return binaryFormat, func(buf []byte) error {
		if buf == nil {
			return errors.New("NULL cannot be converted to float32")
		}
		return readNotNullFloat32(buf, dst)
	}
}

func readNotNullFloat32(buf []byte, dst *float32) error {
	if len(buf) != 4 {
		return fmt.Errorf("float32 requires data length of 4, got %d", len(buf))
	}
	*dst = math.Float32frombits(binary.BigEndian.Uint32(buf))
	return nil
}

func writeFloat32(buf []byte, src float32) ([]byte, uint32, int16) {
	return pgio.AppendUint32(buf, math.Float32bits(src)), float4OID, binaryFormat
}

type NullFloat64 struct {
	Value float64
	Valid bool
}

func (n NullFloat64) EncodeParam(buf []byte) ([]byte, uint32, int16) {
	if n.Valid {
		return writeFloat64(buf, n.Value)
	}
	return nil, 0, binaryFormat
}

func (*NullFloat64) ResultFormat() int16 {
	return binaryFormat
}

func (n *NullFloat64) DecodeResult(buf []byte) error {
	if buf == nil {
		*n = NullFloat64{Valid: false}
		return nil
	}

	n.Valid = true
	return readNotNullFloat64(buf, &n.Value)
}

func readFloat64(dst *float64) (int16, valueReaderFunc) {
	return binaryFormat, func(buf []byte) error {
		if buf == nil {
			return errors.New("NULL cannot be converted to float64")
		}
		return readNotNullFloat64(buf, dst)
	}
}

func readNotNullFloat64(buf []byte, dst *float64) error {
	if len(buf) != 8 {
		return fmt.Errorf("float64 requires data length of 8, got %d", len(buf))
	}
	*dst = math.Float64frombits(binary.BigEndian.Uint64(buf))
	return nil
}

func writeFloat64(buf []byte, src float64) ([]byte, uint32, int16) {
	return pgio.AppendUint64(buf, math.Float64bits(src)), float8OID, binaryFormat
}
