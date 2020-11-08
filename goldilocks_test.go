package goldilocks_test

import (
	"context"
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

}
