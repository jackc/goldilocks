package goldilocks

import (
	"context"
	"fmt"

	"github.com/jackc/pgconn"
)

type Conn struct {
	pgconn *pgconn.PgConn

	paramValuesBuf []byte

	paramValues  [][]byte
	paramOIDs    []uint32
	paramFormats []int16

	resultFormats    []int16
	valueReaderFuncs []valueReaderFunc
}

type valueReaderFunc func([]byte) error

func (c *Conn) Query(
	ctx context.Context,
	sql string,
	args []interface{},
	results []interface{},
	rowFunc func() error,
) (int64, error) {
	err := c.prepareParams(args)
	if err != nil {
		return 0, err
	}

	err = c.prepareResults(results)
	if err != nil {
		return 0, err
	}

	rr := c.pgconn.ExecParams(ctx, sql, c.paramValues, c.paramOIDs, c.paramFormats, c.resultFormats)
	defer rr.Close()

	var rowCount int64
	for rr.NextRow() {
		rowCount++

		values := rr.Values()
		for i := range c.valueReaderFuncs {
			err := c.valueReaderFuncs[i](values[i])
			if err != nil {
				return rowCount, err
			}
		}

		err := rowFunc()
		if err != nil {
			return rowCount, err
		}
	}

	_, err = rr.Close()
	if err != nil {
		return rowCount, err
	}

	// Release
	if len(c.paramValuesBuf)+512 < cap(c.paramValuesBuf)/2 {
		c.paramValuesBuf = nil
	}

	return rowCount, nil
}

func (c *Conn) prepareParams(args []interface{}) error {
	if len(args) == 0 {
		c.paramValues = c.paramValues[0:0]
		c.paramOIDs = c.paramOIDs[0:0]
		c.paramFormats = c.paramFormats[0:0]
		return nil
	}

	// If working buffers are too small or too large create new buffers and allow old ones to be GCed.
	maxParamCap := len(args) * 2
	if maxParamCap < 32 {
		maxParamCap = 32
	}
	if cap(c.paramValues) < len(args) || maxParamCap < cap(args) {
		newCap := len(args)
		if len(args) < 32 {
			newCap = 32
		}
		c.paramValues = make([][]byte, len(args), newCap)
		c.paramOIDs = make([]uint32, len(args), newCap)
		c.paramFormats = make([]int16, len(args), newCap)
	} else {
		c.paramValues = c.paramValues[0:len(args)]
		c.paramOIDs = c.paramOIDs[0:len(args)]
		c.paramFormats = c.paramFormats[0:len(args)]
	}

	c.paramValuesBuf = c.paramValuesBuf[0:0]

	for i := range args {
		var value []byte
		var oid uint32
		var format int16

		switch arg := args[i].(type) {
		case string:
			value, oid, format = writeString(c.paramValuesBuf, arg)
		case int16:
			value, oid, format = writeInt16(c.paramValuesBuf, arg)
		case int32:
			value, oid, format = writeInt32(c.paramValuesBuf, arg)
		case int64:
			value, oid, format = writeInt64(c.paramValuesBuf, arg)
		case float32:
			value, oid, format = writeFloat32(c.paramValuesBuf, arg)
		case float64:
			value, oid, format = writeFloat64(c.paramValuesBuf, arg)
		default:
			return fmt.Errorf("args[%d] is unsupported type %T", i, args[i])
		}

		if value == nil {
			c.paramValues[i] = nil
		} else {
			c.paramValues[i] = value[len(c.paramValuesBuf):]
			c.paramValuesBuf = value
		}

		c.paramOIDs[i] = oid
		c.paramFormats[i] = format
	}

	return nil
}

func (c *Conn) prepareResults(results []interface{}) error {
	if len(results) == 0 {
		c.resultFormats = c.resultFormats[0:0]
		c.valueReaderFuncs = c.valueReaderFuncs[0:0]
		return nil
	}

	// If working buffers are too small or too large create new buffers and allow old ones to be GCed.
	maxResultsCap := len(results) * 2
	if maxResultsCap < 64 {
		maxResultsCap = 64
	}
	if cap(c.resultFormats) < len(results) || maxResultsCap < cap(results) {
		newCap := len(results)
		if len(results) < 64 {
			newCap = 64
		}
		c.resultFormats = make([]int16, len(results), newCap)
		c.valueReaderFuncs = make([]valueReaderFunc, len(results), newCap)
	} else {
		c.resultFormats = c.resultFormats[0:len(results)]
		c.valueReaderFuncs = c.valueReaderFuncs[0:len(results)]
	}

	for i := range results {
		var format int16
		var fn valueReaderFunc
		switch arg := results[i].(type) {
		case *string:
			format, fn = readString(arg)
		case *int16:
			format, fn = readInt16(arg)
		case *int32:
			format, fn = readInt32(arg)
		case *int64:
			format, fn = readInt64(arg)
		case *float32:
			format, fn = readFloat32(arg)
		case *float64:
			format, fn = readFloat64(arg)
		default:
			return fmt.Errorf("results[%d] is unsupported type %T", i, results[i])
		}

		c.resultFormats[i] = format
		c.valueReaderFuncs[i] = fn
	}

	return nil
}
