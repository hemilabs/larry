// Copyright (c) 2025 Hemi Labs, Inc.
// Use of this source code is governed by the MIT License,
// which can be found in the LICENSE file.

package clickhouse

import (
	"container/list"
	"context"
	"database/sql"
	"errors"
	"fmt"
	"sync"
	"time"

	click "github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/proto"
	"github.com/hemilabs/larry/larry"
	"github.com/juju/loggo"
)

const (
	logLevel = "INFO"
)

var log = loggo.GetLogger("clickhouse")

func init() {
	if err := loggo.ConfigureLoggers(logLevel); err != nil {
		panic(err)
	}
}

// Assert required interfaces
var (
	_ larry.Batch       = (*clickBatch)(nil)
	_ larry.Database    = (*clickDB)(nil)
	_ larry.Iterator    = (*clickIterator)(nil)
	_ larry.Range       = (*clickRange)(nil)
	_ larry.Transaction = (*clickTX)(nil)
)

func xerr(err error) error {
	if err == nil {
		return nil
	}
	perr, ok := err.(*proto.Exception)
	if ok {
		switch perr.Code {
		case 386: // empty key
			err = nil
		}
	} else {
		switch {
		case errors.Is(err, sql.ErrNoRows):
			err = larry.ErrKeyNotFound
		}
	}
	return err
}

type ClickConfig struct {
	URI        string
	Tables     []string
	DropTables bool
}

func DefaultClickConfig(URI string, tables []string) *ClickConfig {
	return &ClickConfig{
		URI:    URI,
		Tables: tables,
	}
}

type clickDB struct {
	db     *sql.DB
	cfg    *ClickConfig
	tables map[string]struct{}

	mtx  sync.Mutex
	open bool
}

func NewClickDB(cfg *ClickConfig) (*clickDB, error) {
	if cfg == nil {
		return nil, larry.ErrInvalidConfig
	}
	bdb := &clickDB{
		cfg:    cfg,
		tables: make(map[string]struct{}, len(cfg.Tables)),
	}
	for _, v := range cfg.Tables {
		if _, ok := bdb.tables[v]; ok {
			return nil, larry.ErrDuplicateTable
		}
		bdb.tables[v] = struct{}{}
	}
	return bdb, nil
}

func (b *clickDB) Open(ctx context.Context) error {
	b.mtx.Lock()
	defer b.mtx.Unlock()

	if b.open {
		return larry.ErrDBOpen
	}

	opt, err := click.ParseDSN(b.cfg.URI)
	if err != nil {
		return fmt.Errorf("parse URI: %w", err)
	}
	// opt.Debug = true
	// opt.Debugf = func(format string, v ...any) {
	// 	fmt.Printf(format+"\n", v...)
	// }

	conn := click.OpenDB(opt)
	if err := conn.PingContext(ctx); err != nil {
		if exception, ok := err.(*click.Exception); ok {
			fmt.Printf("Exception [%d] %s \n%s\n", exception.Code, exception.Message, exception.StackTrace)
		}
		return err
	}
	b.db = conn

	if b.cfg.DropTables {
		for _, table := range b.cfg.Tables {
			_, err := conn.ExecContext(ctx, fmt.Sprintf("DROP TABLE IF EXISTS `%s`", table))
			if err != nil {
				return fmt.Errorf("drop table %v: %w", table, err)
			}
		}
	}

	for _, table := range b.cfg.Tables {
		_, err := conn.ExecContext(ctx, fmt.Sprintf(`  
    	CREATE TABLE IF NOT EXISTS %s (  
        key String,  
        value String  
    	) ENGINE = MergeTree ORDER BY (key)`, table))
		if err != nil {
			return fmt.Errorf("create table %v: %w", table, err)
		}
	}
	b.open = true
	return nil
}

func (b *clickDB) Close(_ context.Context) error {
	b.mtx.Lock()
	defer b.mtx.Unlock()

	if !b.open {
		return larry.ErrDBClosed
	}

	if err := xerr(b.db.Close()); err != nil {
		return err
	}
	b.open = false
	return nil
}

func (b *clickDB) Del(ctx context.Context, table string, key []byte) error {
	if _, ok := b.tables[table]; !ok {
		return larry.ErrTableNotFound
	}
	cmd := fmt.Sprintf("DELETE FROM %s WHERE key = $1", table)
	_, err := b.db.ExecContext(ctx, cmd, string(key))
	return xerr(err)
}

func (b *clickDB) Has(ctx context.Context, table string, key []byte) (bool, error) {
	if _, ok := b.tables[table]; !ok {
		return false, larry.ErrTableNotFound
	}
	cmd := fmt.Sprintf("SELECT EXISTS(SELECT key FROM %s FINAL WHERE key = $1)", table)
	var exists int
	err := b.db.QueryRowContext(ctx, cmd, string(key)).Scan(&exists)
	if err != nil {
		return false, xerr(err)
	}

	return exists == 1, nil
}

func (b *clickDB) Get(ctx context.Context, table string, key []byte) ([]byte, error) {
	if _, ok := b.tables[table]; !ok {
		return nil, larry.ErrTableNotFound
	}
	cmd := fmt.Sprintf(`SELECT value FROM %s FINAL WHERE key = $1`, table)
	var val string
	err := b.db.QueryRowContext(ctx, cmd, string(key)).Scan(&val)
	if err != nil {
		return nil, xerr(err)
	}
	return []byte(val), nil
}

func (b *clickDB) Put(ctx context.Context, table string, key, value []byte) error {
	if _, ok := b.tables[table]; !ok {
		return larry.ErrTableNotFound
	}
	if key == nil {
		return nil
	}
	cmd := fmt.Sprintf("INSERT INTO %s (key, value) VALUES ($1, $2)", table)
	_, err := b.db.ExecContext(ctx, cmd, string(key), string(value))
	return xerr(err)
}

func (b *clickDB) Begin(ctx context.Context, write bool) (larry.Transaction, error) {
	b.mtx.Lock()
	cfg := *b.cfg
	cfg.DropTables = false
	tx, err := NewClickDB(&cfg)
	if err != nil {
		return nil, err
	}
	if err := tx.Open(ctx); err != nil {
		return nil, err
	}
	_, err = tx.db.ExecContext(ctx, "BEGIN TRANSACTION")
	if err != nil {
		if err := tx.Close(ctx); err != nil {
			log.Errorf("tx begin: close tx db: %v", err)
		}
		return nil, err
	}
	return &clickTX{
		tx: tx,
		db: b,
	}, nil
}

func (b *clickDB) execute(ctx context.Context, write bool, callback func(ctx context.Context, tx larry.Transaction) error) error {
	tx, err := b.Begin(ctx, write)
	if err != nil {
		return err
	}
	err = callback(ctx, tx)
	if err != nil {
		if cerr := tx.Rollback(ctx); cerr != nil {
			return fmt.Errorf("rollback %w: %w", cerr, err)
		}
		return err
	}

	if !write {
		return tx.Rollback(ctx)
	}
	return tx.Commit(ctx)
}

func (b *clickDB) View(ctx context.Context, callback func(ctx context.Context, tx larry.Transaction) error) error {
	return b.execute(ctx, false, callback)
}

func (b *clickDB) Update(ctx context.Context, callback func(ctx context.Context, tx larry.Transaction) error) error {
	return b.execute(ctx, true, callback)
}

func (b *clickDB) NewIterator(pctx context.Context, table string) (larry.Iterator, error) {
	if _, ok := b.tables[table]; !ok {
		return nil, larry.ErrTableNotFound
	}

	// XXX PNOOMA
	ctx := click.Context(pctx, click.WithBlockBufferSize(10))

	cmd := fmt.Sprintf("SELECT * FROM %s FINAL", table)
	rows, err := b.db.QueryContext(ctx, cmd)
	if err != nil {
		return nil, err
	}
	return &clickIterator{
		db:    b,
		it:    rows,
		table: table,
	}, nil
}

func (b *clickDB) NewRange(pctx context.Context, table string, start, end []byte) (larry.Range, error) {
	if _, ok := b.tables[table]; !ok {
		return nil, larry.ErrTableNotFound
	}

	// XXX PNOOMA
	ctx := click.Context(pctx, click.WithBlockBufferSize(10))

	cmd := fmt.Sprintf("SELECT * FROM %s FINAL WHERE key >= $1 AND key < $2", table)
	rows, err := b.db.QueryContext(ctx, cmd, string(start), string(end))
	if err != nil {
		return nil, err
	}
	return &clickRange{
		db:    b,
		it:    rows,
		table: table,

		start: string(start),
		end:   string(end),
	}, nil
}

func (b *clickDB) NewBatch(ctx context.Context) (larry.Batch, error) {
	return &clickBatch{wb: new(list.List)}, nil
}

// Transactions
type clickTX struct {
	tx *clickDB
	db *clickDB
}

func (tx *clickTX) Del(ctx context.Context, table string, key []byte) error {
	if _, ok := tx.db.tables[table]; !ok {
		return larry.ErrTableNotFound
	}
	cmd := fmt.Sprintf("DELETE FROM %s WHERE key = $1", table)
	_, err := tx.tx.db.ExecContext(ctx, cmd, string(key))
	return xerr(err)
}

func (tx *clickTX) Has(ctx context.Context, table string, key []byte) (bool, error) {
	if _, ok := tx.db.tables[table]; !ok {
		return false, larry.ErrTableNotFound
	}
	cmd := fmt.Sprintf("SELECT EXISTS(SELECT key FROM %s FINAL WHERE key = $1)", table)
	var exists int
	err := tx.tx.db.QueryRowContext(ctx, cmd, string(key)).Scan(&exists)
	if err != nil {
		return false, xerr(err)
	}
	return exists == 1, nil
}

func (tx *clickTX) Get(ctx context.Context, table string, key []byte) ([]byte, error) {
	if _, ok := tx.db.tables[table]; !ok {
		return nil, larry.ErrTableNotFound
	}
	cmd := fmt.Sprintf("SELECT value FROM %s FINAL WHERE key = $1", table)
	var val string
	err := tx.tx.db.QueryRowContext(ctx, cmd, string(key)).Scan(&val)
	if err != nil {
		return nil, xerr(err)
	}
	return []byte(val), nil
}

func (tx *clickTX) Put(ctx context.Context, table string, key []byte, value []byte) error {
	if _, ok := tx.db.tables[table]; !ok {
		return larry.ErrTableNotFound
	}
	if key == nil {
		return nil
	}
	cmd := fmt.Sprintf("INSERT INTO %s (key, value) VALUES ($1, $2)", table)
	_, err := tx.tx.db.ExecContext(ctx, cmd, string(key), string(value))
	return xerr(err)
}

func (tx *clickTX) Commit(ctx context.Context) error {
	_, err := tx.tx.db.ExecContext(ctx, "COMMIT")
	if err != nil {
		return err
	}
	defer tx.db.mtx.Unlock()
	if err = tx.tx.Close(ctx); err != nil {
		log.Errorf("tx commit: close tx db: %v", err)
	}
	return nil
}

func (tx *clickTX) Rollback(ctx context.Context) error {
	defer func() {
		if err := tx.tx.Close(ctx); err != nil {
			log.Errorf("tx rollback: close tx db: %v", err)
		}
		tx.db.mtx.Unlock()
	}()
	_, err := tx.tx.db.ExecContext(ctx, "ROLLBACK")
	return err
}

func (tx *clickTX) Write(ctx context.Context, b larry.Batch) error {
	bb, ok := b.(*clickBatch)
	if !ok {
		return fmt.Errorf("unexpected batch type: %T", b)
	}
	log.Infof("writing batch of size %v", bb.wb.Len())
	i := 0
	for e := bb.wb.Front(); e != nil; e = e.Next() {
		op, ok := e.Value.(batchOp)
		if !ok {
			return fmt.Errorf("unexpected batch element type %T", e.Value)
		}
		if _, ok := tx.db.tables[op.table]; !ok {
			return larry.ErrTableNotFound
		}
		start := time.Now()
		switch op.op {
		case larry.OpDel:
			keys := ""
			for pair := op.pairs.Front(); pair != nil; pair = pair.Next() {
				kv, ok := pair.Value.(kvPair)
				if !ok {
					return fmt.Errorf("opPut: expected kkPair, got %T", pair.Value)
				}
				keys += fmt.Sprintf("'%s',", string(kv.key))
			}
			keys = keys[:len(keys)-1]
			stmt := fmt.Sprintf("ALTER TABLE %s DELETE WHERE key IN (%s)", op.table, keys)
			_, err := tx.tx.db.ExecContext(ctx, stmt)
			if err != nil {
				return fmt.Errorf("opDel: %w", err)
			}
			// kv, ok := pair.Value.(kvPair)
			// if !ok {
			// 	return fmt.Errorf("opDel: expected kkPair, got %T", pair.Value)
			// }
			// if err := tx.Del(ctx, op.table, kv.key); err != nil {
			// 	return fmt.Errorf("opDel: %w", err)
			// }
			// log.Infof("%v: wrote DEL in %v", i, time.Since(start))
		case larry.OpPut:
			scope, err := tx.tx.db.Begin()
			if err != nil {
				return err
			}
			stmt, err := scope.PrepareContext(ctx,
				fmt.Sprintf("INSERT INTO %s", op.table))
			if err != nil {
				return fmt.Errorf("opPut: %w", err)
			}
			for pair := op.pairs.Front(); pair != nil; pair = pair.Next() {
				kv, ok := pair.Value.(kvPair)
				if !ok {
					return fmt.Errorf("opPut: expected kkPair, got %T", pair.Value)
				}
				_, err := stmt.ExecContext(ctx, string(kv.key), string(kv.value))
				if err != nil {
					return fmt.Errorf("opPut: %w", err)
				}
			}
			if err := scope.Commit(); err != nil {
				return fmt.Errorf("opPut commit: %w", err)
			}
			log.Infof("%v: wrote PUT in %v", i, time.Since(start))
		default:
			return fmt.Errorf("unknown operation: %v", op.op)
		}
		i++
	}
	return nil
}

type clickIterator struct {
	db    *clickDB
	it    *sql.Rows
	table string

	key, value string
}

func (ni *clickIterator) First(pctx context.Context) bool {
	ni.key = ""
	ctx := click.Context(pctx, click.WithBlockBufferSize(10))
	cmd := fmt.Sprintf("SELECT * FROM %s FINAL", ni.table)
	rows, err := ni.db.db.QueryContext(ctx, cmd)
	if err != nil {
		log.Errorf("first query: %s", err.Error())
	}
	if err := ni.it.Close(); err != nil {
		log.Errorf("close iterator: %w", err)
	}
	ni.it = rows
	return ni.Next(ctx)
}

func (ni *clickIterator) Last(pctx context.Context) bool {
	ni.key = ""
	ctx := click.Context(pctx, click.WithBlockBufferSize(10))
	cmd := fmt.Sprintf("SELECT * FROM %s FINAL ORDER BY key DESC LIMIT 1", ni.table)
	rows, err := ni.db.db.QueryContext(ctx, cmd)
	if err != nil {
		log.Errorf("last query: %s", err.Error())
	}
	if err := ni.it.Close(); err != nil {
		log.Errorf("close iterator: %w", err)
	}
	ni.it = rows
	return ni.Next(ctx)
}

func (ni *clickIterator) Next(ctx context.Context) bool {
	ni.key = ""
	return ni.it.Next()
}

func (ni *clickIterator) Seek(pctx context.Context, key []byte) bool {
	ni.key = ""
	ctx := click.Context(pctx, click.WithBlockBufferSize(10))
	cmd := fmt.Sprintf("SELECT * FROM %s FINAL WHERE key >= $1", ni.table)
	rows, err := ni.db.db.QueryContext(ctx, cmd, string(key))
	if err != nil {
		log.Errorf("seek query: %s", err.Error())
	}
	if err := ni.it.Close(); err != nil {
		log.Errorf("close iterator: %w", err)
	}
	ni.it = rows
	return ni.Next(ctx)
}

func (ni *clickIterator) parseRow(_ context.Context) {
	if err := ni.it.Scan(&ni.key, &ni.value); err != nil {
		log.Errorf("scan row")
	}
}

func (ni *clickIterator) Key(ctx context.Context) []byte {
	if ni.key == "" {
		ni.parseRow(ctx)
	}
	return []byte(ni.key)
}

func (ni *clickIterator) Value(ctx context.Context) []byte {
	if ni.key == "" {
		ni.parseRow(ctx)
	}
	return []byte(ni.value)
}

func (ni *clickIterator) Close(ctx context.Context) {
	if err := ni.it.Close(); err != nil {
		log.Errorf("failed to close rows: %v", err)
	}
}

type clickRange struct {
	db    *clickDB
	it    *sql.Rows
	table string

	start, end string
	key, value string
}

func (ni *clickRange) First(pctx context.Context) bool {
	ni.key = ""
	ctx := click.Context(pctx, click.WithBlockBufferSize(10))
	cmd := fmt.Sprintf("SELECT * FROM %s FINAL WHERE key >= $1 AND key < $2", ni.table)
	rows, err := ni.db.db.QueryContext(ctx, cmd, ni.start, ni.end)
	if err != nil {
		log.Errorf("first query: %s", err.Error())
	}
	if err := ni.it.Close(); err != nil {
		log.Errorf("close iterator: %w", err)
	}
	ni.it = rows
	return ni.Next(ctx)
}

func (ni *clickRange) Last(pctx context.Context) bool {
	ni.key = ""
	ctx := click.Context(pctx, click.WithBlockBufferSize(10))
	cmd := fmt.Sprintf(`SELECT * FROM %s FINAL WHERE key >= $1 AND key < $2 
	ORDER BY key DESC LIMIT 1`, ni.table)
	rows, err := ni.db.db.QueryContext(ctx, cmd, ni.start, ni.end)
	if err != nil {
		log.Errorf("last query: %s", err.Error())
	}
	if err := ni.it.Close(); err != nil {
		log.Errorf("close iterator: %w", err)
	}
	ni.it = rows
	return ni.Next(ctx)
}

func (ni *clickRange) Next(ctx context.Context) bool {
	ni.key = ""
	return ni.it.Next()
}

func (ni *clickRange) parseRow(_ context.Context) {
	if err := ni.it.Scan(&ni.key, &ni.value); err != nil {
		log.Errorf("scan row")
	}
}

func (ni *clickRange) Key(ctx context.Context) []byte {
	if ni.key == "" {
		ni.parseRow(ctx)
	}
	return []byte(ni.key)
}

func (ni *clickRange) Value(ctx context.Context) []byte {
	if ni.key == "" {
		ni.parseRow(ctx)
	}
	return []byte(ni.value)
}

func (ni *clickRange) Close(_ context.Context) {
	if err := ni.it.Close(); err != nil {
		log.Errorf("failed to close rows: %v", err)
	}
}

// Batches

type kvPair struct {
	key, value []byte
}

type batchOp struct {
	op    larry.OperationT
	table string
	pairs *list.List // elements of type kvPair
}

type clickBatch struct {
	wb *list.List // elements of type batchOp
}

func (nb *clickBatch) Del(ctx context.Context, table string, key []byte) {
	l := nb.wb.Back()
	if l != nil {
		lop, ok := l.Value.(batchOp)
		if !ok {
			log.Errorf("unexpected batch element type %T", l.Value)
			return
		}
		if lop.op == larry.OpDel && lop.table == table {
			lop.pairs.PushBack(kvPair{key: key})
			return
		}
	}
	op := batchOp{op: larry.OpDel, table: table, pairs: new(list.List)}
	op.pairs.PushBack(kvPair{key: key})
	nb.wb.PushBack(op)
}

func (nb *clickBatch) Put(ctx context.Context, table string, key, value []byte) {
	l := nb.wb.Back()
	if l != nil {
		lop, ok := l.Value.(batchOp)
		if !ok {
			log.Errorf("unexpected batch element type %T", l.Value)
			return
		}
		if lop.op == larry.OpPut && lop.table == table {
			lop.pairs.PushBack(kvPair{key: key, value: value})
			return
		}
	}
	op := batchOp{op: larry.OpPut, table: table, pairs: new(list.List)}
	op.pairs.PushBack(kvPair{key: key, value: value})
	nb.wb.PushBack(op)
}

func (nb *clickBatch) Reset(ctx context.Context) {
	nb.wb.Init()
}
