package goldilocks

import (
	"context"
	"fmt"

	"github.com/jackc/pgconn"
)

type DB struct {
}

type Conn struct {
}

func Query(
	ctx context.Context,
	conn *pgconn.PgConn,
	sql string,
	args []interface{},
	results []interface{},
	rowFunc func() error,
) (int64, error) {
	paramValues, paramOIDs, paramFormats, err := prepareParams(args)
	if err != nil {
		return 0, err
	}

	resultFormats, valueReaderFuncs, err := prepareResults(results)
	if err != nil {
		return 0, err
	}

	rr := conn.ExecParams(ctx, sql, paramValues, paramOIDs, paramFormats, resultFormats)
	defer rr.Close()

	var rowCount int64
	for rr.NextRow() {
		rowCount++

		values := rr.Values()
		for i := range valueReaderFuncs {
			err := valueReaderFuncs[i](values[i])
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

	return rowCount, nil
}

type valueReaderFunc func([]byte) error

func prepareParams(args []interface{}) ([][]byte, []uint32, []int16, error) {
	if len(args) == 0 {
		return nil, nil, nil, nil
	}
	paramValues := make([][]byte, len(args))
	paramOIDs := make([]uint32, len(args))
	paramFormats := make([]int16, len(args))

	for i := range args {
		switch arg := args[i].(type) {
		case string:
			paramValues[i], paramOIDs[i], paramFormats[i] = writeString(arg)
		case int16:
			paramValues[i], paramOIDs[i], paramFormats[i] = writeInt16(arg)
		case int32:
			paramValues[i], paramOIDs[i], paramFormats[i] = writeInt32(arg)
		case int64:
			paramValues[i], paramOIDs[i], paramFormats[i] = writeInt64(arg)
		case float32:
			paramValues[i], paramOIDs[i], paramFormats[i] = writeFloat32(arg)
		case float64:
			paramValues[i], paramOIDs[i], paramFormats[i] = writeFloat64(arg)
		default:
			return nil, nil, nil, fmt.Errorf("args[%d] is unsupported type %T", i, args[i])
		}
	}

	return paramValues, paramOIDs, paramFormats, nil
}

func prepareResults(results []interface{}) ([]int16, []valueReaderFunc, error) {
	if len(results) == 0 {
		return nil, nil, nil
	}
	resultFormats := make([]int16, len(results))
	valueReaderFuncs := make([]valueReaderFunc, len(results))

	for i := range results {
		switch arg := results[i].(type) {
		case *string:
			resultFormats[i], valueReaderFuncs[i] = readString(arg)
		case *int16:
			resultFormats[i], valueReaderFuncs[i] = readInt16(arg)
		case *int32:
			resultFormats[i], valueReaderFuncs[i] = readInt32(arg)
		case *int64:
			resultFormats[i], valueReaderFuncs[i] = readInt64(arg)
		case *float32:
			resultFormats[i], valueReaderFuncs[i] = readFloat32(arg)
		case *float64:
			resultFormats[i], valueReaderFuncs[i] = readFloat64(arg)

		default:
			return nil, nil, fmt.Errorf("results[%d] is unsupported type %T", i, results[i])
		}
	}

	return resultFormats, valueReaderFuncs, nil
}
