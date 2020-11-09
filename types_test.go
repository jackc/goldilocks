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

func TestDate(t *testing.T) {
	t.Parallel()

	pgConn, err := pgconn.Connect(context.Background(), os.Getenv("GOLDILOCKS_TEST_CONN_STRING"))
	require.NoError(t, err)
	defer closePgConn(t, pgConn)
	db := goldilocks.NewConn(pgConn)

	for _, tt := range []struct {
		date time.Time
	}{
		{time.Date(1900, 1, 1, 0, 0, 0, 0, time.UTC)},
		{time.Date(1970, 1, 1, 0, 0, 0, 0, time.UTC)},
		{time.Date(1999, 12, 31, 0, 0, 0, 0, time.UTC)},
		{time.Date(2000, 1, 1, 0, 0, 0, 0, time.UTC)},
		{time.Date(2000, 1, 2, 0, 0, 0, 0, time.UTC)},
		{time.Date(2200, 1, 1, 0, 0, 0, 0, time.UTC)},
	} {
		var date time.Time

		_, err := db.Query(
			context.Background(),
			"select $1",
			[]interface{}{goldilocks.Date(tt.date)},
			[]interface{}{(*goldilocks.Date)(&date)},
			func() error { return nil },
		)
		require.NoError(t, err)
		require.True(t, tt.date.Equal(date))
	}

	ensurePgConnValid(t, pgConn)
}

func TestDateInfinity(t *testing.T) {
	t.Parallel()

	pgConn, err := pgconn.Connect(context.Background(), os.Getenv("GOLDILOCKS_TEST_CONN_STRING"))
	require.NoError(t, err)
	defer closePgConn(t, pgConn)
	db := goldilocks.NewConn(pgConn)

	var inf time.Time
	var ninf time.Time

	// Decode
	_, err = db.Query(
		context.Background(),
		"select 'infinity'::date, '-infinity'::date",
		nil,
		[]interface{}{(*goldilocks.Date)(&inf), (*goldilocks.Date)(&ninf)},
		func() error { return nil },
	)
	require.NoError(t, err)
	assert.True(t, inf.Equal(time.Time(goldilocks.DateInfinity)))
	assert.True(t, ninf.Equal(time.Time(goldilocks.DateNegativeInfinity)))

	// Encode
	var b1, b2 bool
	_, err = db.Query(
		context.Background(),
		"select $1 = 'infinity'::date, $2 = '-infinity'::date",
		[]interface{}{goldilocks.DateInfinity, goldilocks.DateNegativeInfinity},
		[]interface{}{&b1, &b2},
		func() error { return nil },
	)
	require.NoError(t, err)
	assert.True(t, b1)
	assert.True(t, b2)

	ensurePgConnValid(t, pgConn)
}

func TestTime(t *testing.T) {
	t.Parallel()

	pgConn, err := pgconn.Connect(context.Background(), os.Getenv("GOLDILOCKS_TEST_CONN_STRING"))
	require.NoError(t, err)
	defer closePgConn(t, pgConn)
	db := goldilocks.NewConn(pgConn)

	for _, tt := range []struct {
		time time.Time
	}{
		{time.Date(1900, 1, 1, 0, 0, 0, 0, time.UTC)},
		{time.Date(1970, 1, 1, 0, 0, 0, 0, time.UTC)},
		{time.Date(1999, 12, 31, 0, 0, 0, 0, time.UTC)},
		{time.Date(2000, 1, 1, 0, 0, 0, 0, time.UTC)},
		{time.Date(2000, 1, 2, 0, 0, 0, 0, time.UTC)},
		{time.Date(2200, 1, 1, 0, 0, 0, 0, time.UTC)},
	} {
		var _time time.Time

		_, err := db.Query(
			context.Background(),
			"select $1",
			[]interface{}{tt.time},
			[]interface{}{&_time},
			func() error { return nil },
		)
		require.NoError(t, err)
		require.True(t, tt.time.Equal(_time))
	}

	ensurePgConnValid(t, pgConn)
}

func TestTimeInfinity(t *testing.T) {
	t.Parallel()

	pgConn, err := pgconn.Connect(context.Background(), os.Getenv("GOLDILOCKS_TEST_CONN_STRING"))
	require.NoError(t, err)
	defer closePgConn(t, pgConn)
	db := goldilocks.NewConn(pgConn)

	var inf time.Time
	var ninf time.Time

	// Decode
	_, err = db.Query(
		context.Background(),
		"select 'infinity'::timestamptz, '-infinity'::timestamptz",
		nil,
		[]interface{}{&inf, &ninf},
		func() error { return nil },
	)
	require.NoError(t, err)
	assert.True(t, inf.Equal(goldilocks.TimeInfinity))
	assert.True(t, ninf.Equal(goldilocks.TimeNegativeInfinity))

	// Encode
	var b1, b2 bool
	_, err = db.Query(
		context.Background(),
		"select $1 = 'infinity'::timestamptz, $2 = '-infinity'::timestamptz",
		[]interface{}{goldilocks.TimeInfinity, goldilocks.TimeNegativeInfinity},
		[]interface{}{&b1, &b2},
		func() error { return nil },
	)
	require.NoError(t, err)
	assert.True(t, b1)
	assert.True(t, b2)

	ensurePgConnValid(t, pgConn)
}
