package tpg

import (
	"context"
	"errors"
	"sync"

	"github.com/jackc/pgx/v4"
	"github.com/jackc/pgx/v4/pgxpool"
)

// ErrAlreadyConnected is returned when there already a connection to the
// database.
var ErrAlreadyConnected = errors.New("already connected")

// ErrNotFound is returned when QueryOne or ExecReturnID do not return a row.
var ErrNotFound = errors.New("not found")

// ErrRowBreak is a special error that can be returns from the ForEach iterator
// to stop iteration, but make the outer ForEach call return nil.
var ErrRowBreak = errors.New("row break")

var connMu sync.Mutex
var pool *pgxpool.Pool

// Connect to the database. This should be called once, and probably at
// application start up.
// The url should be like "postgres://127.0.0.1:5432/mydb",
func Connect(url string) error {
	connMu.Lock()
	defer connMu.Unlock()
	if pool != nil {
		return ErrAlreadyConnected
	}
	var err error
	pool, err = pgxpool.Connect(context.Background(), url)
	if err != nil {
		return err
	}
	return nil
}

// Tx represents a database transaction
type Tx interface {
	// Exec sql and returns the number of rows affected
	Exec(sql string, args ...interface{}) (rows int64, err error)
	// Query sql and returns the rows.
	Query(sql string, args ...interface{}) (Rows, error)
	// Query sql and returns one row.
	// Returns ErrNotFound if the row does not exist.
	QueryOne(sql string, args ...interface{}) (Row, error)
	// Exec sql and return a single row ID.
	// Expectes SQL to have a "INSERT ... RETURNED id" or be a simple query
	// like  "SELECT id FROM tbl LIMIT 1".
	ExecReturnID(sql string, args ...interface{}) (int64, error)
}

// Row is a single row from a result set.
type Row interface {
	// Scan reads the values from the current row into dest values positionally.
	// dest can include pointers to core types, values implementing the Scanner
	// interface, and nil. nil will skip the value entirely.
	// For example:
	//   var id int
	//   var name stirng
	//   var createdAt time.Time
	//   rows.ForEach(func(row Row) error) error {
	//   	return row.Scan(&id, &name, &createAt)
	//   }
	Scan(dest ...interface{}) error
}

// Rows is a series of rows from a result set.
type Rows interface {
	// ForEach iterates over each row in the result set.
	// You can return an error from the iter function to stop the iteration, and
	// that error will be returned from the outer ForEach call.
	// The ErrRowBreak is a special error that allows for you to stop the
	// iteration, but the outer ForEach call will return nil.
	ForEach(iter func(row Row) error) error
}

type dbRows struct{ pgx.Rows }

func (rows dbRows) ForEach(iter func(row Row) error) error {
	var err error
	for rows.Rows.Next() {
		err = iter(rows)
		if err != nil {
			break
		}
	}
	if err != nil && err != ErrRowBreak {
		return rows.Rows.Err()
	}
	return nil
}

type dbTx struct {
	ctx context.Context
	tx  pgx.Tx
}

func (tx *dbTx) Exec(sql string, args ...interface{}) (rows int64, err error) {
	tag, err := tx.tx.Exec(tx.ctx, sql, args...)
	if err != nil {
		return 0, err
	}
	return tag.RowsAffected(), nil

}

func (tx *dbTx) Query(sql string, args ...interface{}) (Rows, error) {
	rows, err := tx.tx.Query(tx.ctx, sql, args...)
	if err != nil {
		return nil, err
	}
	return dbRows{rows}, nil
}

func (tx *dbTx) QueryOne(sql string, args ...interface{}) (Row, error) {
	rows, err := tx.Query(sql, args...)
	if err != nil {
		return nil, err
	}
	var res Row
	if err = rows.ForEach(func(row Row) error {
		res = row
		return nil
	}); err != nil {
		return nil, err
	}
	if res == nil {
		return nil, ErrNotFound
	}
	return res, nil
}

func (tx *dbTx) ExecReturnID(sql string, args ...interface{}) (int64, error) {
	row, err := tx.QueryOne(sql, args...)
	if err != nil {
		return 0, err
	}
	var id int64
	if err := row.Scan(&id); err != nil {
		return 0, err
	}
	return id, nil
}

// Begin a database transaction.
// Transactions automatically commit.
// Return an error to rollback the transaction.
func Begin(fn func(tx Tx) error) error {
	ctx := context.Background()
	conn, err := pool.Acquire(ctx)
	if err != nil {
		return err
	}
	defer conn.Release()
	tx, err := conn.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	if err := fn(&dbTx{ctx: ctx, tx: tx}); err != nil {
		return err
	}
	return tx.Commit(ctx)
}
