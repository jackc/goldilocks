package goldilocks_test

import (
	"context"
	"os"
	"testing"

	"github.com/jackc/goldilocks"
	"github.com/stretchr/testify/require"
)

func TestPoolAcquire(t *testing.T) {
	t.Parallel()

	db, err := goldilocks.NewPool(os.Getenv("GOLDILOCKS_TEST_CONN_STRING"))
	require.NoError(t, err)
	defer db.Close()

	var rowCount int64
	var numbers []int32
	var n int32
	err = db.Acquire(context.Background(), func(db *goldilocks.Conn) error {
		var err error
		rowCount, err = db.Query(
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

func TestPoolQuery(t *testing.T) {
	t.Parallel()

	db, err := goldilocks.NewPool(os.Getenv("GOLDILOCKS_TEST_CONN_STRING"))
	require.NoError(t, err)
	defer db.Close()

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

func TestPoolExec(t *testing.T) {
	t.Parallel()

	db, err := goldilocks.NewPool(os.Getenv("GOLDILOCKS_TEST_CONN_STRING"))
	require.NoError(t, err)
	defer db.Close()

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
}

func TestPoolBeginCommit(t *testing.T) {
	t.Parallel()

	db, err := goldilocks.NewPool(os.Getenv("GOLDILOCKS_TEST_CONN_STRING"))
	require.NoError(t, err)
	defer db.Close()

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

}
