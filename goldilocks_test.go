package goldilocks_test

import (
	"context"
	"math/rand"
	"testing"
	"time"

	"github.com/jackc/goldilocks"
	"github.com/stretchr/testify/require"
)

func testStdDB(t *testing.T, db goldilocks.StdDB) {
	t.Run("testQuery", func(t *testing.T) { testQuery(t, db) })
	t.Run("testQueryGoBuiltinTypes", func(t *testing.T) { testQueryGoBuiltinTypes(t, db) })
	t.Run("testQuerySkipsNilResults", func(t *testing.T) { testQuerySkipsNilResults(t, db) })
	t.Run("testExec", func(t *testing.T) { testExec(t, db) })
	t.Run("testQueryParamEncodersAndResultDecoders", func(t *testing.T) { testQueryParamEncodersAndResultDecoders(t, db) })
}

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

func testQueryGoBuiltinTypes(t *testing.T, db goldilocks.StdDB) {
	var n int
	if testing.Short() {
		n = 10
	} else {
		n = 100
	}

	for i := 0; i < n; i++ {
		var s string
		var i16 int16
		var i32 int32
		var i64 int64
		var f32 float32
		var f64 float64
		var b bool
		var date time.Time

		args := []interface{}{"foo", int16(1), int32(2), int64(3), float32(1.23), float64(4.56), true, goldilocks.Date(time.Date(2020, 11, 9, 0, 0, 0, 0, time.UTC))}
		results := []interface{}{&s, &i16, &i32, &i64, &f32, &f64, &b, (*goldilocks.Date)(&date)}

		// Shuffle order of arguments.
		for j := 0; j < 10; j++ {
			a := rand.Intn(len(args))
			b := rand.Intn(len(args))
			args[a], args[b] = args[b], args[a]
			results[a], results[b] = results[b], results[a]
		}

		rowCount, err := db.Query(
			context.Background(),
			"select $1, $2, $3, $4, $5, $6, $7, $8",
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
		require.Equal(t, true, b)
		require.True(t, time.Date(2020, 11, 9, 0, 0, 0, 0, time.UTC).Equal(date))
	}
}

func testQueryParamEncodersAndResultDecoders(t *testing.T, db goldilocks.StdDB) {
	var n int
	if testing.Short() {
		n = 10
	} else {
		n = 100
	}

	for i := 0; i < n; i++ {
		str := goldilocks.NullString{"foo", true}
		resStr := goldilocks.NullString{}
		nullStr := goldilocks.NullString{}
		nullResStr := goldilocks.NullString{}

		i16 := goldilocks.NullInt16{42, true}
		resI16 := goldilocks.NullInt16{}
		nullI16 := goldilocks.NullInt16{}
		nullResI16 := goldilocks.NullInt16{}

		i32 := goldilocks.NullInt32{43, true}
		resI32 := goldilocks.NullInt32{}
		nullI32 := goldilocks.NullInt32{}
		nullResI32 := goldilocks.NullInt32{}

		i64 := goldilocks.NullInt64{44, true}
		resI64 := goldilocks.NullInt64{}
		nullI64 := goldilocks.NullInt64{}
		nullResI64 := goldilocks.NullInt64{}

		f32 := goldilocks.NullFloat32{43, true}
		resF32 := goldilocks.NullFloat32{}
		nullF32 := goldilocks.NullFloat32{}
		nullResF32 := goldilocks.NullFloat32{}

		f64 := goldilocks.NullFloat64{44, true}
		resF64 := goldilocks.NullFloat64{}
		nullF64 := goldilocks.NullFloat64{}
		nullResF64 := goldilocks.NullFloat64{}

		b := goldilocks.NullBool{true, true}
		resB := goldilocks.NullBool{}
		nullB := goldilocks.NullBool{}
		nullResB := goldilocks.NullBool{}

		date := goldilocks.NullDate{time.Date(2020, 11, 9, 0, 0, 0, 0, time.UTC), true}
		resDate := goldilocks.NullDate{}
		nullDate := goldilocks.NullDate{}
		nullResDate := goldilocks.NullDate{}

		args := []interface{}{str, nullStr, i16, nullI16, i32, nullI32, i64, nullI64, f32, nullF32, f64, nullF64, b, nullB, date, nullDate}
		results := []interface{}{&resStr, &nullResStr, &resI16, &nullResI16, &resI32, &nullResI32, &resI64, &nullResI64, &resF32, &nullResF32, &resF64, &nullResF64, &resB, &nullResB, &resDate, &nullResDate}

		// Shuffle order of arguments.
		for j := 0; j < 10; j++ {
			a := rand.Intn(len(args))
			b := rand.Intn(len(args))
			args[a], args[b] = args[b], args[a]
			results[a], results[b] = results[b], results[a]
		}

		rowCount, err := db.Query(
			context.Background(),
			"select $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16",
			args,
			results,
			func() error {
				return nil
			},
		)
		require.NoError(t, err)
		require.EqualValues(t, 1, rowCount)
		require.Equal(t, str, resStr)
		require.Equal(t, nullStr, nullResStr)
		require.Equal(t, i16, resI16)
		require.Equal(t, nullI16, nullResI16)
		require.Equal(t, i32, resI32)
		require.Equal(t, nullI32, nullResI32)
		require.Equal(t, i64, resI64)
		require.Equal(t, nullI64, nullResI64)
		require.Equal(t, f32, resF32)
		require.Equal(t, nullF32, nullResF32)
		require.Equal(t, f64, resF64)
		require.Equal(t, nullF64, nullResF64)
		require.True(t, date.Value.Equal(resDate.Value))
		require.Equal(t, date.Valid, resDate.Valid)
		require.Equal(t, nullDate, nullResDate)
	}
}

func testQuerySkipsNilResults(t *testing.T, db goldilocks.StdDB) {
	var a, c int32
	rowCount, err := db.Query(
		context.Background(),
		"select 1, 2, 3",
		nil,
		[]interface{}{&a, nil, &c},
		func() error { return nil },
	)
	require.NoError(t, err)
	require.EqualValues(t, 1, rowCount)
	require.EqualValues(t, 1, a)
	require.EqualValues(t, 3, c)
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
