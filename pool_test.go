package goldilocks_test

import (
	"context"
	"os"
	"sync"
	"testing"

	"github.com/jackc/goldilocks"
	"github.com/stretchr/testify/require"
)

func TestPoolAcquire(t *testing.T) {
	t.Parallel()

	db, err := goldilocks.NewPool(os.Getenv("GOLDILOCKS_TEST_CONN_STRING"))
	require.NoError(t, err)
	defer db.Close()

	err = db.Acquire(context.Background(), func(db *goldilocks.Conn) error {
		testQuery(t, db)
		return nil
	})
	require.NoError(t, err)
}

func TestPoolStdDB(t *testing.T) {
	db, err := goldilocks.NewPool(os.Getenv("GOLDILOCKS_TEST_CONN_STRING"))
	require.NoError(t, err)
	defer db.Close()

	testStdDB(t, db)
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

func TestPoolStress(t *testing.T) {
	db, err := goldilocks.NewPool(os.Getenv("GOLDILOCKS_TEST_CONN_STRING"))
	require.NoError(t, err)
	defer db.Close()

	n := 100
	wg := &sync.WaitGroup{}

	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			err := db.Acquire(context.Background(), func(db *goldilocks.Conn) error {
				testExec(t, db)
				return nil
			})
			require.NoError(t, err)

			testQuery(t, db)
			testQuerySkipsNilResults(t, db)
			testQueryBuiltinTypes(t, db)
		}()
	}

	wg.Wait()
}
