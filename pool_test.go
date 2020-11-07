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
