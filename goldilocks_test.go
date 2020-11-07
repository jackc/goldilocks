package goldilocks_test

import (
	"context"
	"os"
	"testing"

	"github.com/jackc/goldilocks"
	"github.com/stretchr/testify/require"
)

func TestConnQuery(t *testing.T) {
	t.Parallel()

	db, err := goldilocks.NewDB(os.Getenv("GOLDILOCKS_TEST_CONN_STRING"))
	require.NoError(t, err)
	defer db.Close()

	var rowCount int64
	var numbers []int32
	var n int32
	err = db.Do(context.Background(), func(conn *goldilocks.Conn) error {
		var err error
		rowCount, err = conn.Query(
			context.Background(),
			"select n from generate_series(1, 5) n",
			nil,
			[]interface{}{&n},
			func() error {
				numbers = append(numbers, n)
				return nil
			},
		)
		if err != nil {
			return err
		}

		return nil
	})
	require.NoError(t, err)
	require.EqualValues(t, 5, rowCount)
	require.Equal(t, []int32{1, 2, 3, 4, 5}, numbers)
}

func TestConnQueryBuiltinTypes(t *testing.T) {
	t.Parallel()

	db, err := goldilocks.NewDB(os.Getenv("GOLDILOCKS_TEST_CONN_STRING"))
	require.NoError(t, err)
	defer db.Close()

	var rowCount int64
	var s string
	var i16 int16
	var i32 int32
	var i64 int64
	var f32 float32
	var f64 float64
	err = db.Do(context.Background(), func(conn *goldilocks.Conn) error {
		var err error
		rowCount, err = conn.Query(
			context.Background(),
			"select $1, $2, $3, $4, $5, $6",
			[]interface{}{"foo", int16(1), int32(2), int64(3), float32(1.23), float64(4.56)},
			[]interface{}{&s, &i16, &i32, &i64, &f32, &f64},
			func() error {
				return nil
			},
		)
		if err != nil {
			return err
		}

		return nil
	})
	require.NoError(t, err)
	require.EqualValues(t, 1, rowCount)
	require.Equal(t, "foo", s)
	require.Equal(t, int16(1), i16)
	require.Equal(t, int32(2), i32)
	require.Equal(t, int64(3), i64)
	require.Equal(t, float32(1.23), f32)
	require.Equal(t, float64(4.56), f64)
}
