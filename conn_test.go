package goldilocks_test

import (
	"context"
	"errors"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/jackc/goldilocks"
	"github.com/jackc/pgconn"
	"github.com/jackc/pgerrcode"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func closePgConn(t testing.TB, conn *pgconn.PgConn) {
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
func ensurePgConnValid(t *testing.T, pgConn *pgconn.PgConn) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	result := pgConn.ExecParams(ctx, "select generate_series(1,$1)", [][]byte{[]byte("3")}, nil, nil, nil).Read()
	cancel()

	require.Nil(t, result.Err)
	assert.Equal(t, 3, len(result.Rows))
	assert.Equal(t, "1", string(result.Rows[0][0]))
	assert.Equal(t, "2", string(result.Rows[1][0]))
	assert.Equal(t, "3", string(result.Rows[2][0]))
}

func TestConnStd(t *testing.T) {
	pgConn, err := pgconn.Connect(context.Background(), os.Getenv("GOLDILOCKS_TEST_CONN_STRING"))
	require.NoError(t, err)
	defer closePgConn(t, pgConn)
	db := goldilocks.NewConn(pgConn)

	testStdDB(t, db)

	ensurePgConnValid(t, pgConn)
}

func TestConnBeginCommit(t *testing.T) {
	t.Parallel()

	pgConn, err := pgconn.Connect(context.Background(), os.Getenv("GOLDILOCKS_TEST_CONN_STRING"))
	require.NoError(t, err)
	defer closePgConn(t, pgConn)
	db := goldilocks.NewConn(pgConn)

	_, err = db.Exec(context.Background(), "create temporary table goldilocks (a text)")
	require.NoError(t, err)

	_, err = db.Exec(context.Background(), "insert into goldilocks (a) values($1)", "foo")
	require.NoError(t, err)

	err = db.Begin(context.Background(), func(db goldilocks.StdDB) error {
		_, err := db.Exec(context.Background(), "delete from goldilocks")
		return err
	})
	require.NoError(t, err)

	rowsAffected, err := db.Exec(context.Background(), "select * from goldilocks")
	require.NoError(t, err)
	require.EqualValues(t, 0, rowsAffected)

	ensurePgConnValid(t, pgConn)
}

func TestConnBeginFuncReturnsError(t *testing.T) {
	t.Parallel()

	pgConn, err := pgconn.Connect(context.Background(), os.Getenv("GOLDILOCKS_TEST_CONN_STRING"))
	require.NoError(t, err)
	defer closePgConn(t, pgConn)
	db := goldilocks.NewConn(pgConn)

	_, err = db.Exec(context.Background(), "create temporary table goldilocks (a text)")
	require.NoError(t, err)

	_, err = db.Exec(context.Background(), "insert into goldilocks (a) values($1)", "foo")
	require.NoError(t, err)

	err = db.Begin(context.Background(), func(db goldilocks.StdDB) error {
		_, err := db.Exec(context.Background(), "delete from goldilocks")
		require.NoError(t, err)
		return fmt.Errorf("some error")
	})
	require.EqualError(t, err, "some error")

	rowsAffected, err := db.Exec(context.Background(), "select * from goldilocks")
	require.NoError(t, err)
	require.EqualValues(t, 1, rowsAffected)

	ensurePgConnValid(t, pgConn)
}

func TestConnBeginBrokenTxIsRolledBack(t *testing.T) {
	t.Parallel()

	pgConn, err := pgconn.Connect(context.Background(), os.Getenv("GOLDILOCKS_TEST_CONN_STRING"))
	require.NoError(t, err)
	defer closePgConn(t, pgConn)
	db := goldilocks.NewConn(pgConn)

	_, err = db.Exec(context.Background(), "create temporary table goldilocks (a text)")
	require.NoError(t, err)

	_, err = db.Exec(context.Background(), "insert into goldilocks (a) values($1)", "foo")
	require.NoError(t, err)

	err = db.Begin(context.Background(), func(db goldilocks.StdDB) error {
		_, err := db.Exec(context.Background(), "delete from goldilocks")
		require.NoError(t, err)

		_, err = db.Exec(context.Background(), "select 1 / 0")
		require.Error(t, err)
		var pgErr *pgconn.PgError
		require.True(t, errors.As(err, &pgErr))
		require.Equal(t, pgerrcode.DivisionByZero, pgErr.Code)

		return nil
	})
	require.EqualError(t, err, "rolled back failed transaction")

	rowsAffected, err := db.Exec(context.Background(), "select * from goldilocks")
	require.NoError(t, err)
	require.EqualValues(t, 1, rowsAffected)

	ensurePgConnValid(t, pgConn)
}
