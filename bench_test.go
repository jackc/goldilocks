package goldilocks_test

import (
	"context"
	"fmt"
	"os"
	"strconv"
	"strings"
	"testing"

	"github.com/jackc/goldilocks"
	"github.com/jackc/pgconn"
	"github.com/stretchr/testify/require"
)

func getSelectRowsCounts(b *testing.B) []int64 {
	var rowCounts []int64
	{
		s := os.Getenv("GOLDILOCKS_BENCH_SELECT_ROWS_COUNTS")
		if s != "" {
			for _, p := range strings.Split(s, " ") {
				n, err := strconv.ParseInt(p, 10, 64)
				if err != nil {
					b.Fatalf("Bad GOLDILOCKS_BENCH_SELECT_ROWS_COUNTS value: %v", err)
				}
				rowCounts = append(rowCounts, n)
			}
		}
	}

	if len(rowCounts) == 0 {
		rowCounts = []int64{1, 10, 100, 1000}
	}

	return rowCounts
}

func BenchmarkSelectRowsInts(b *testing.B) {

	pgConn, err := pgconn.Connect(context.Background(), os.Getenv("GOLDILOCKS_TEST_CONN_STRING"))
	require.NoError(b, err)
	defer closePgConn(b, pgConn)
	db := goldilocks.NewConn(pgConn)

	rowCounts := getSelectRowsCounts(b)

	for _, rowCount := range rowCounts {
		b.Run(fmt.Sprintf("%d rows", rowCount), func(b *testing.B) {

			var n1, n2, n3, n4, n5, n6, n7, n8, n9, n10 int64

			for i := 0; i < b.N; i++ {
				_, err := db.Query(
					context.Background(),
					"select n, n+1, n+2, n+3, n+4, n+5, n+6, n+7, n+8, n+9 from generate_series(100001, 100000 + $1) n",
					[]interface{}{rowCount},
					[]interface{}{&n1, &n2, &n3, &n4, &n5, &n6, &n7, &n8, &n9, &n10},
					func() error { return nil },
				)
				if err != nil {
					b.Fatal(err)
				}
			}

		})
	}
}
