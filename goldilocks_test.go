package goldilocks_test

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/jackc/goldilocks"
	"github.com/jackc/pgconn"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func closeConn(t testing.TB, conn *pgconn.PgConn) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	require.NoError(t, conn.Close(ctx))
	select {
	case <-conn.CleanupDone():
	case <-time.After(5 * time.Second):
		t.Fatal("Connection cleanup exceeded maximum time")
	}
}

// Do a simple query to ensure the connection is still usable
func ensureConnValid(t *testing.T, pgConn *pgconn.PgConn) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	result := pgConn.ExecParams(ctx, "select generate_series(1,$1)", [][]byte{[]byte("3")}, nil, nil, nil).Read()
	cancel()

	require.Nil(t, result.Err)
	assert.Equal(t, 3, len(result.Rows))
	assert.Equal(t, "1", string(result.Rows[0][0]))
	assert.Equal(t, "2", string(result.Rows[1][0]))
	assert.Equal(t, "3", string(result.Rows[2][0]))
}

func TestConnQuery(t *testing.T) {
	t.Parallel()

	pgConn, err := pgconn.Connect(context.Background(), os.Getenv("GOLDILOCKS_TEST_CONN_STRING"))
	require.NoError(t, err)
	defer closeConn(t, pgConn)

	conn := &goldilocks.Conn{PgConn: pgConn}

	var numbers []int32
	var n int32
	rowCount, err := conn.Query(
		context.Background(),
		"select n from generate_series(1, 5) n",
		nil,
		[]interface{}{&n},
		func() error {
			numbers = append(numbers, n)
			return nil
		},
	)
	require.NoError(t, err)
	require.EqualValues(t, 5, rowCount)
	require.Equal(t, []int32{1, 2, 3, 4, 5}, numbers)

	ensureConnValid(t, pgConn)
}

func TestQuery(t *testing.T) {
	t.Parallel()

	pgConn, err := pgconn.Connect(context.Background(), os.Getenv("GOLDILOCKS_TEST_CONN_STRING"))
	require.NoError(t, err)
	defer closeConn(t, pgConn)

	conn := &goldilocks.Conn{PgConn: pgConn}

	var numbers []int32
	var n int32
	rowCount, err := conn.Query(
		context.Background(),
		"select n from generate_series(1, 5) n",
		nil,
		[]interface{}{&n},
		func() error {
			numbers = append(numbers, n)
			return nil
		},
	)
	require.NoError(t, err)
	require.EqualValues(t, 5, rowCount)
	require.Equal(t, []int32{1, 2, 3, 4, 5}, numbers)

	ensureConnValid(t, pgConn)
}

func TestQueryBuiltinTypes(t *testing.T) {
	t.Parallel()

	pgConn, err := pgconn.Connect(context.Background(), os.Getenv("GOLDILOCKS_TEST_CONN_STRING"))
	require.NoError(t, err)
	defer closeConn(t, pgConn)

	conn := &goldilocks.Conn{PgConn: pgConn}

	var s string
	var i16 int16
	var i32 int32
	var i64 int64
	var f32 float32
	var f64 float64
	rowCount, err := conn.Query(
		context.Background(),
		"select $1, $2, $3, $4, $5, $6",
		[]interface{}{"foo", int16(1), int32(2), int64(3), float32(1.23), float64(4.56)},
		[]interface{}{&s, &i16, &i32, &i64, &f32, &f64},
		func() error {
			return nil
		},
	)
	require.NoError(t, err)
	require.EqualValues(t, 1, rowCount)
	require.Equal(t, "foo", s)
	require.Equal(t, int16(1), i16)
	require.Equal(t, int32(2), i32)
	require.Equal(t, int64(3), i64)
	require.Equal(t, float32(1.23), f32)
	require.Equal(t, float64(4.56), f64)

	ensureConnValid(t, pgConn)
}

func TestQueryBuiltinResultTypes(t *testing.T) {
	t.Parallel()

	pgConn, err := pgconn.Connect(context.Background(), os.Getenv("GOLDILOCKS_TEST_CONN_STRING"))
	require.NoError(t, err)
	defer closeConn(t, pgConn)

	conn := &goldilocks.Conn{PgConn: pgConn}

	var s string
	var i16 int16
	var i32 int32
	var i64 int64
	var f32 float32
	var f64 float64
	rowCount, err := conn.Query(
		context.Background(),
		"select 'foo'::text, 1::smallint, 2::integer, 3::bigint, 1.23::float4, 4.56::float8",
		nil,
		[]interface{}{&s, &i16, &i32, &i64, &f32, &f64},
		func() error {
			return nil
		},
	)
	require.NoError(t, err)
	require.EqualValues(t, 1, rowCount)
	require.Equal(t, "foo", s)
	require.Equal(t, int16(1), i16)
	require.Equal(t, int32(2), i32)
	require.Equal(t, int64(3), i64)
	require.Equal(t, float32(1.23), f32)
	require.Equal(t, float64(4.56), f64)

	ensureConnValid(t, pgConn)
}
