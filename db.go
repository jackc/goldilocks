package goldilocks

import (
	"context"
	"runtime"
	"strconv"
	"time"

	"github.com/jackc/pgconn"
	"github.com/jackc/puddle"
	errors "golang.org/x/xerrors"
)

var defaultMaxConns = int32(4)
var defaultMinConns = int32(0)
var defaultMaxConnLifetime = time.Hour
var defaultMaxConnIdleTime = time.Minute * 5
var defaultHealthCheckPeriod = time.Minute

type DB struct {
	p                 *puddle.Pool
	config            *Config
	minConns          int32
	maxConnLifetime   time.Duration
	maxConnIdleTime   time.Duration
	healthCheckPeriod time.Duration
	closeChan         chan struct{}
}

// Config is the configuration struct for creating a DB. It must be created by ParseConfig and then it can be
// modified. A manually initialized Config will cause ConnectConfig to panic.
type Config struct {
	pgconn.Config

	// MaxConnLifetime is the duration since creation after which a connection will be automatically closed.
	MaxConnLifetime time.Duration

	// MaxConnIdleTime is the duration after which an idle connection will be automatically closed by the health check.
	MaxConnIdleTime time.Duration

	// MaxConns is the maximum size of the connection pool.
	MaxConns int32

	// MinConns is the minimum size of the connection pool. The health check will increase the number of connections to this
	// amount if it had dropped below.
	MinConns int32

	// HealthCheckPeriod is the duration between checks of the health of idle connections.
	HealthCheckPeriod time.Duration

	createdByParseConfig bool // Used to enforce created by ParseConfig rule.
}

// NewDB creates a new DB from connStr. See ParseConfig for information on connString format.
func NewDB(connString string) (*DB, error) {
	config, err := ParseConfig(connString)
	if err != nil {
		return nil, err
	}

	return NewDBConfig(config)
}

// NewDBConfig creates a new DB from config. config must have been created by ParseConfig.
func NewDBConfig(config *Config) (*DB, error) {
	// Default values are set in ParseConfig. Enforce initial creation by ParseConfig rather than setting defaults from
	// zero values.
	if !config.createdByParseConfig {
		panic("config must be created by ParseConfig")
	}

	p := &DB{
		config:            config,
		minConns:          config.MinConns,
		maxConnLifetime:   config.MaxConnLifetime,
		maxConnIdleTime:   config.MaxConnIdleTime,
		healthCheckPeriod: config.HealthCheckPeriod,
		closeChan:         make(chan struct{}),
	}

	p.p = puddle.NewPool(
		func(ctx context.Context) (interface{}, error) {
			pgConn, err := pgconn.ConnectConfig(ctx, &config.Config)
			if err != nil {
				return nil, err
			}

			conn := &Conn{pgconn: pgConn}

			return conn, nil
		},
		func(value interface{}) {
			ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
			conn := value.(*Conn)
			conn.pgconn.Close(ctx)
			select {
			case <-conn.pgconn.CleanupDone():
			case <-ctx.Done():
			}
			cancel()
		},
		config.MaxConns,
	)

	go p.backgroundHealthCheck()

	return p, nil
}

// ParseConfig builds a Config from connString. It parses connString with the same behavior as pgconn.ParseConfig with the
// addition of the following variables:
//
// pool_max_conns: integer greater than 0
// pool_min_conns: integer 0 or greater
// pool_max_conn_lifetime: duration string
// pool_max_conn_idle_time: duration string
// pool_health_check_period: duration string
//
// See Config for definitions of these arguments.
//
//   # Example DSN
//   user=jack password=secret host=pg.example.com port=5432 dbname=mydb sslmode=verify-ca pool_max_conns=10
//
//   # Example URL
//   postgres://jack:secret@pg.example.com:5432/mydb?sslmode=verify-ca&pool_max_conns=10
func ParseConfig(connString string) (*Config, error) {
	pgconnConfig, err := pgconn.ParseConfig(connString)
	if err != nil {
		return nil, err
	}

	config := &Config{
		Config:               *pgconnConfig,
		createdByParseConfig: true,
	}

	if s, ok := config.Config.RuntimeParams["pool_max_conns"]; ok {
		delete(config.Config.RuntimeParams, "pool_max_conns")
		n, err := strconv.ParseInt(s, 10, 32)
		if err != nil {
			return nil, errors.Errorf("cannot parse pool_max_conns: %w", err)
		}
		if n < 1 {
			return nil, errors.Errorf("pool_max_conns too small: %d", n)
		}
		config.MaxConns = int32(n)
	} else {
		config.MaxConns = defaultMaxConns
		if numCPU := int32(runtime.NumCPU()); numCPU > config.MaxConns {
			config.MaxConns = numCPU
		}
	}

	if s, ok := config.Config.RuntimeParams["pool_min_conns"]; ok {
		delete(config.Config.RuntimeParams, "pool_min_conns")
		n, err := strconv.ParseInt(s, 10, 32)
		if err != nil {
			return nil, errors.Errorf("cannot parse pool_min_conns: %w", err)
		}
		config.MinConns = int32(n)
	} else {
		config.MinConns = defaultMinConns
	}

	if s, ok := config.Config.RuntimeParams["pool_max_conn_lifetime"]; ok {
		delete(config.Config.RuntimeParams, "pool_max_conn_lifetime")
		d, err := time.ParseDuration(s)
		if err != nil {
			return nil, errors.Errorf("invalid pool_max_conn_lifetime: %w", err)
		}
		config.MaxConnLifetime = d
	} else {
		config.MaxConnLifetime = defaultMaxConnLifetime
	}

	if s, ok := config.Config.RuntimeParams["pool_max_conn_idle_time"]; ok {
		delete(config.Config.RuntimeParams, "pool_max_conn_idle_time")
		d, err := time.ParseDuration(s)
		if err != nil {
			return nil, errors.Errorf("invalid pool_max_conn_idle_time: %w", err)
		}
		config.MaxConnIdleTime = d
	} else {
		config.MaxConnIdleTime = defaultMaxConnIdleTime
	}

	if s, ok := config.Config.RuntimeParams["pool_health_check_period"]; ok {
		delete(config.Config.RuntimeParams, "pool_health_check_period")
		d, err := time.ParseDuration(s)
		if err != nil {
			return nil, errors.Errorf("invalid pool_health_check_period: %w", err)
		}
		config.HealthCheckPeriod = d
	} else {
		config.HealthCheckPeriod = defaultHealthCheckPeriod
	}

	return config, nil
}

// Close closes all connections in the pool and rejects future Acquire calls. Blocks until all connections are returned
// to pool and closed.
func (db *DB) Close() {
	close(db.closeChan)
	db.p.Close()
}

func (db *DB) backgroundHealthCheck() {
	ticker := time.NewTicker(db.healthCheckPeriod)

	for {
		select {
		case <-db.closeChan:
			ticker.Stop()
			return
		case <-ticker.C:
			db.checkIdleConnsHealth()
			db.checkMinConns()
		}
	}
}

func (db *DB) checkIdleConnsHealth() {
	resources := db.p.AcquireAllIdle()

	now := time.Now()
	for _, res := range resources {
		if now.Sub(res.CreationTime()) > db.maxConnLifetime {
			res.Destroy()
		} else if res.IdleDuration() > db.maxConnIdleTime {
			res.Destroy()
		} else {
			res.ReleaseUnused()
		}
	}
}

func (db *DB) checkMinConns() {
	for i := db.minConns - db.PoolStats().TotalConns(); i > 0; i-- {
		go func() {
			ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
			defer cancel()
			db.p.CreateResource(ctx)
		}()
	}
}

func (db *DB) Do(ctx context.Context, f func(*Conn) error) error {
	res, err := db.p.Acquire(ctx)
	if err != nil {
		return err
	}
	defer db.releaseConn(res)

	conn := res.Value().(*Conn)
	err = f(conn)
	if err != nil {
		return err
	}

	return nil
}

func (db *DB) releaseConn(res *puddle.Resource) {
	conn := res.Value().(*Conn)
	now := time.Now()
	if conn.pgconn.IsClosed() || conn.pgconn.IsBusy() || conn.pgconn.TxStatus() != 'I' || (now.Sub(res.CreationTime()) > db.maxConnLifetime) {
		res.Destroy()
		return
	}

	res.Release()
}

func (db *DB) PoolStats() *PoolStats {
	return &PoolStats{s: db.p.Stat()}
}

type PoolStats struct {
	s *puddle.Stat
}

// AcquireCount returns the cumulative count of successful acquires from the pool.
func (s *PoolStats) AcquireCount() int64 {
	return s.s.AcquireCount()
}

// AcquireDuration returns the total duration of all successful acquires from
// the pool.
func (s *PoolStats) AcquireDuration() time.Duration {
	return s.s.AcquireDuration()
}

// AcquiredConns returns the number of currently acquired connections in the pool.
func (s *PoolStats) AcquiredConns() int32 {
	return s.s.AcquiredResources()
}

// CanceledAcquireCount returns the cumulative count of acquires from the pool
// that were canceled by a context.
func (s *PoolStats) CanceledAcquireCount() int64 {
	return s.s.CanceledAcquireCount()
}

// ConstructingConns returns the number of conns with construction in progress in
// the pool.
func (s *PoolStats) ConstructingConns() int32 {
	return s.s.ConstructingResources()
}

// EmptyAcquireCount returns the cumulative count of successful acquires from the pool
// that waited for a resource to be released or constructed because the pool was
// empty.
func (s *PoolStats) EmptyAcquireCount() int64 {
	return s.s.EmptyAcquireCount()
}

// IdleConns returns the number of currently idle conns in the pool.
func (s *PoolStats) IdleConns() int32 {
	return s.s.IdleResources()
}

// MaxResources returns the maximum size of the pool.
func (s *PoolStats) MaxConns() int32 {
	return s.s.MaxResources()
}

// TotalConns returns the total number of resources currently in the pool.
// The value is the sum of ConstructingConns, AcquiredConns, and
// IdleConns.
func (s *PoolStats) TotalConns() int32 {
	return s.s.TotalResources()
}
