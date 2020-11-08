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

	pool, err := goldilocks.NewPool(os.Getenv("GOLDILOCKS_TEST_CONN_STRING"))
	require.NoError(t, err)
	defer pool.Close()

	var rowCount int64
	var numbers []int32
	var n int32
	err = pool.Acquire(context.Background(), func(conn *goldilocks.Conn) error {
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

func TestPoolQuery(t *testing.T) {
	t.Parallel()

	pool, err := goldilocks.NewPool(os.Getenv("GOLDILOCKS_TEST_CONN_STRING"))
	require.NoError(t, err)
	defer pool.Close()

	var numbers []int32
	var n int32
	rowCount, err := pool.Query(
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

	pool, err := goldilocks.NewPool(os.Getenv("GOLDILOCKS_TEST_CONN_STRING"))
	require.NoError(t, err)
	defer pool.Close()

	rowsAffected, err := pool.Exec(context.Background(), "create temporary table goldilocks (a text)")
	require.NoError(t, err)
	require.EqualValues(t, 0, rowsAffected)

	rowsAffected, err = pool.Exec(context.Background(), "insert into goldilocks (a) values($1)", "foo")
	require.NoError(t, err)
	require.EqualValues(t, 1, rowsAffected)

	rowsAffected, err = pool.Exec(context.Background(), "insert into goldilocks (a) values($1), ($2)", "foo", "bar")
	require.NoError(t, err)
	require.EqualValues(t, 2, rowsAffected)

	rowsAffected, err = pool.Exec(context.Background(), "update goldilocks set a = $1", "baz")
	require.NoError(t, err)
	require.EqualValues(t, 3, rowsAffected)

	rowsAffected, err = pool.Exec(context.Background(), "delete from goldilocks")
	require.NoError(t, err)
	require.EqualValues(t, 3, rowsAffected)
}

func TestPoolBeginCommit(t *testing.T) {
	t.Parallel()

	pool, err := goldilocks.NewPool(os.Getenv("GOLDILOCKS_TEST_CONN_STRING"))
	require.NoError(t, err)
	defer pool.Close()

	_, err = pool.Exec(context.Background(), "create temporary table goldilocks (a text)")
	require.NoError(t, err)

	_, err = pool.Exec(context.Background(), "insert into goldilocks (a) values($1)", "foo")
	require.NoError(t, err)

	err = pool.Begin(context.Background(), func(conn goldilocks.StdDB) error {
		_, err := conn.Exec(context.Background(), "delete from goldilocks")
		return err
	})
	require.NoError(t, err)

	rowsAffected, err := pool.Exec(context.Background(), "select * from goldilocks")
	require.NoError(t, err)
	require.EqualValues(t, 0, rowsAffected)

}
