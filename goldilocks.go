package goldilocks

import "context"

// StdDB is the common interface that Pool, Conn, and transaction contexts support.
type StdDB interface {
	Query(ctx context.Context, sql string, args []interface{}, results []interface{}, rowFunc func() error) (rowsAffected int64, err error)
	Exec(ctx context.Context, sql string, args ...interface{}) (rowsAffected int64, err error)
	Begin(ctx context.Context, f func(StdDB) error) error
}
