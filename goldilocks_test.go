package goldilocks_test

import (
	"context"
	"math/rand"
	"testing"

	"github.com/jackc/goldilocks"
	"github.com/stretchr/testify/require"
)

func testQuery(t *testing.T, db goldilocks.StdDB) {
	var numbers []int32
	var n int32
	rowCount, err := db.Query(
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
}

func testQueryBuiltinTypes(t *testing.T, db goldilocks.StdDB) {
	for i := 0; i < 100; i++ {
		var s string
		var i16 int16
		var i32 int32
		var i64 int64
		var f32 float32
		var f64 float64

		args := []interface{}{"foo", int16(1), int32(2), int64(3), float32(1.23), float64(4.56)}
		results := []interface{}{&s, &i16, &i32, &i64, &f32, &f64}

		// Shuffle order of arguments.
		for j := 0; j < 10; j++ {
			a := rand.Intn(len(args))
			b := rand.Intn(len(args))
			args[a], args[b] = args[b], args[a]
			results[a], results[b] = results[b], results[a]
		}

		rowCount, err := db.Query(
			context.Background(),
			"select $1, $2, $3, $4, $5, $6",
			args,
			results,
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
	}
}

func testExec(t *testing.T, db goldilocks.StdDB) {
	rowsAffected, err := db.Exec(context.Background(), "create temporary table goldilocks (a text)")
	require.NoError(t, err)
	require.EqualValues(t, 0, rowsAffected)

	rowsAffected, err = db.Exec(context.Background(), "insert into goldilocks (a) values($1)", "foo")
	require.NoError(t, err)
	require.EqualValues(t, 1, rowsAffected)

	rowsAffected, err = db.Exec(context.Background(), "insert into goldilocks (a) values($1), ($2)", "foo", "bar")
	require.NoError(t, err)
	require.EqualValues(t, 2, rowsAffected)

	rowsAffected, err = db.Exec(context.Background(), "update goldilocks set a = $1", "baz")
	require.NoError(t, err)
	require.EqualValues(t, 3, rowsAffected)

	rowsAffected, err = db.Exec(context.Background(), "delete from goldilocks")
	require.NoError(t, err)
	require.EqualValues(t, 3, rowsAffected)

	rowsAffected, err = db.Exec(context.Background(), "drop table goldilocks")
	require.NoError(t, err)
	require.EqualValues(t, 0, rowsAffected)
}
