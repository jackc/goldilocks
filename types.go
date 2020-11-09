package goldilocks

import (
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"time"

	"github.com/jackc/pgio"
)

const (
	textFormat   = 0
	binaryFormat = 1
)

// PostgreSQL oids for builtin types
const (
	boolOID   = 16
	int8OID   = 20
	int2OID   = 21
	int4OID   = 23
	textOID   = 25
	float4OID = 700
	float8OID = 701
	dateOID   = 1082
)

type nilSkip struct{}

func (nilSkip) ResultFormat() int16 {
	return textFormat
}

func (nilSkip) DecodeResult(buf []byte) error {
	return nil
}

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

type notNullString string

func (*notNullString) ResultFormat() int16 {
	return textFormat
}

func (nn *notNullString) DecodeResult(buf []byte) error {
	if buf == nil {
		return errors.New("NULL cannot be converted to string")
	}
	return readNotNullString(buf, (*string)(nn))
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

type notNullInt16 int16

func (*notNullInt16) ResultFormat() int16 {
	return binaryFormat
}

func (nn *notNullInt16) DecodeResult(buf []byte) error {
	if buf == nil {
		return errors.New("NULL cannot be converted to int16")
	}
	return readNotNullInt16(buf, (*int16)(nn))
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

type notNullInt32 int32

func (*notNullInt32) ResultFormat() int16 {
	return binaryFormat
}

func (nn *notNullInt32) DecodeResult(buf []byte) error {
	if buf == nil {
		return errors.New("NULL cannot be converted to int32")
	}
	return readNotNullInt32(buf, (*int32)(nn))
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

type notNullInt64 int64

func (*notNullInt64) ResultFormat() int16 {
	return binaryFormat
}

func (nn *notNullInt64) DecodeResult(buf []byte) error {
	if buf == nil {
		return errors.New("NULL cannot be converted to int64")
	}
	return readNotNullInt64(buf, (*int64)(nn))
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

type notNullFloat32 float32

func (*notNullFloat32) ResultFormat() int16 {
	return binaryFormat
}

func (nn *notNullFloat32) DecodeResult(buf []byte) error {
	if buf == nil {
		return errors.New("NULL cannot be converted to float32")
	}
	return readNotNullFloat32(buf, (*float32)(nn))
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

type notNullFloat64 float64

func (*notNullFloat64) ResultFormat() int16 {
	return binaryFormat
}

func (nn *notNullFloat64) DecodeResult(buf []byte) error {
	if buf == nil {
		return errors.New("NULL cannot be converted to float64")
	}
	return readNotNullFloat64(buf, (*float64)(nn))
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

type NullBool struct {
	Value bool
	Valid bool
}

func (n NullBool) EncodeParam(buf []byte) ([]byte, uint32, int16) {
	if n.Valid {
		return writeBool(buf, n.Value)
	}
	return nil, 0, binaryFormat
}

func (*NullBool) ResultFormat() int16 {
	return binaryFormat
}

func (n *NullBool) DecodeResult(buf []byte) error {
	if buf == nil {
		*n = NullBool{Valid: false}
		return nil
	}

	n.Valid = true
	return readNotNullBool(buf, &n.Value)
}

type notNullBool bool

func (*notNullBool) ResultFormat() int16 {
	return binaryFormat
}

func (nn *notNullBool) DecodeResult(buf []byte) error {
	if buf == nil {
		return errors.New("NULL cannot be converted to bool")
	}
	return readNotNullBool(buf, (*bool)(nn))
}

func readBool(dst *bool) (int16, valueReaderFunc) {
	return binaryFormat, func(buf []byte) error {
		if buf == nil {
			return errors.New("NULL cannot be converted to bool")
		}
		return readNotNullBool(buf, dst)
	}
}

func readNotNullBool(buf []byte, dst *bool) error {
	if len(buf) != 1 {
		return fmt.Errorf("bool requires data length of 1, got %d", len(buf))
	}
	*dst = buf[0] == 1
	return nil
}

func writeBool(buf []byte, src bool) ([]byte, uint32, int16) {
	var b byte
	if src {
		b = 1
	}
	return append(buf, b), boolOID, binaryFormat
}

type NullDate struct {
	Value time.Time
	Valid bool
}

func (n NullDate) EncodeParam(buf []byte) ([]byte, uint32, int16) {
	if n.Valid {
		return writeDate(buf, n.Value)
	}
	return nil, 0, binaryFormat
}

func (*NullDate) ResultFormat() int16 {
	return binaryFormat
}

func (n *NullDate) DecodeResult(buf []byte) error {
	if buf == nil {
		*n = NullDate{Valid: false}
		return nil
	}

	n.Valid = true
	return readNotNullDate(buf, &n.Value)
}

// DateNegativeInfinity represents the PostgreSQL date value -Infinity. It is less than all dates the PostgreSQL date
// type can represent.
var DateNegativeInfinity = Date(time.Date(-9999999, 1, 1, 0, 0, 0, 0, time.UTC))

// DateInfinity represents the PostgreSQL date value Infinity. It is greater than all dates the PostgreSQL date type
// can represent.
var DateInfinity = Date(time.Date(9999999, 1, 1, 0, 0, 0, 0, time.UTC))

const (
	negativeInfinityDayOffset = -2147483648
	infinityDayOffset         = 2147483647
)

type Date time.Time

func (nn Date) EncodeParam(buf []byte) ([]byte, uint32, int16) {
	return writeDate(buf, time.Time(nn))
}

func (*Date) ResultFormat() int16 {
	return binaryFormat
}

func (nn *Date) DecodeResult(buf []byte) error {
	if buf == nil {
		return errors.New("NULL cannot be converted to time.Time")
	}
	return readNotNullDate(buf, (*time.Time)(nn))
}

func readDate(dst *time.Time) (int16, valueReaderFunc) {
	return binaryFormat, func(buf []byte) error {
		if buf == nil {
			return errors.New("NULL cannot be converted to time.Time")
		}
		return readNotNullDate(buf, dst)
	}
}

func readNotNullDate(buf []byte, dst *time.Time) error {
	if len(buf) != 4 {
		return fmt.Errorf("date requires data length of 4, got %d", len(buf))
	}

	dayOffset := int32(binary.BigEndian.Uint32(buf))

	switch dayOffset {
	case infinityDayOffset:
		*dst = time.Time(DateInfinity)
	case negativeInfinityDayOffset:
		*dst = time.Time(DateNegativeInfinity)
	default:
		*dst = time.Date(2000, 1, int(1+dayOffset), 0, 0, 0, 0, time.UTC)
	}
	return nil
}

func writeDate(buf []byte, src time.Time) ([]byte, uint32, int16) {
	tUnix := time.Date(src.Year(), src.Month(), src.Day(), 0, 0, 0, 0, time.UTC).Unix()
	dateEpoch := time.Date(2000, 1, 1, 0, 0, 0, 0, time.UTC).Unix()

	var daysSinceDateEpoch int32
	switch Date(src) {
	case DateInfinity:
		daysSinceDateEpoch = infinityDayOffset
	case DateNegativeInfinity:
		daysSinceDateEpoch = negativeInfinityDayOffset
	default:
		secSinceDateEpoch := tUnix - dateEpoch
		daysSinceDateEpoch = int32(secSinceDateEpoch / 86400)
	}

	return pgio.AppendInt32(buf, daysSinceDateEpoch), dateOID, binaryFormat
}
