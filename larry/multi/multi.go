// Copyright (c) 2024-2025 Hemi Labs, Inc.
// Use of this source code is governed by the MIT License,
// which can be found in the LICENSE file.

// Package multi implements multi DB, a database multiplexer.
// Each table in a multi DB is it's own larry database internally,
// and a multi DB provides a unified interface for managing access
// and performing operations on each one.
package multi

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"

	"github.com/hemilabs/larry/larry"
	"github.com/hemilabs/larry/larry/level"
	"github.com/juju/loggo"
	"github.com/mitchellh/go-homedir"
)

const (
	logLevel = "INFO"
	dbname   = ""
)

var log = loggo.GetLogger("multi")

func init() {
	if err := loggo.ConfigureLoggers(logLevel); err != nil {
		panic(err)
	}
}

// Assert required interfaces
var (
	_ larry.Batch       = (*multiBatch)(nil)
	_ larry.Database    = (*multiDB)(nil)
	_ larry.Transaction = (*multiTX)(nil)
)

type MultiConfig struct {
	Home string

	// Format - key: name, value: db type
	// If name is a path, the path is created, but the table should be
	// referenced using the base of the path. e.g., If "path/to/table1"
	// is passed, the table name is "table1".
	Tables map[string]string
}

func DefaultMultiConfig(home string, tables map[string]string) *MultiConfig {
	return &MultiConfig{
		Home:   home,
		Tables: tables,
	}
}

type pool map[string]larry.Database

type multiDB struct {
	mtx sync.RWMutex

	pool pool // database pool

	cfg *MultiConfig

	open bool
}

func (l *multiDB) openDB(ctx context.Context, db, name string) error {
	var (
		bhsDB larry.Database
		err   error
	)

	bhs := filepath.Join(l.cfg.Home, name)
	switch db {
	case "level":
		cfg := level.DefaultLevelConfig(bhs, []string{dbname})
		bhsDB, err = level.NewLevelDB(cfg)
	default:
		err = fmt.Errorf("unsupported db type: %v", db)
	}
	if err != nil {
		return fmt.Errorf("db pool create %v: %w", name, err)
	}

	if err := bhsDB.Open(ctx); err != nil {
		return fmt.Errorf("db pool open %v: %w", db, err)
	}
	l.pool[filepath.Base(name)] = bhsDB

	return nil
}

func NewMultiDB(cfg *MultiConfig) (larry.Database, error) {
	if cfg == nil {
		return nil, larry.ErrInvalidConfig
	}

	h, err := homedir.Expand(cfg.Home)
	if err != nil {
		return nil, fmt.Errorf("home dir: %w", err)
	}
	err = os.MkdirAll(h, 0o0700)
	if err != nil {
		return nil, fmt.Errorf("mkdir: %w", err)
	}

	l := &multiDB{
		cfg:  cfg,
		pool: make(pool, len(cfg.Tables)),
	}

	return l, nil
}

func (b *multiDB) Open(ctx context.Context) error {
	log.Tracef("Open")
	defer log.Tracef("Open exit")

	b.mtx.Lock()
	if b.open {
		b.mtx.Unlock()
		return larry.ErrDBOpen
	}

	var err error
	unwind := true
	defer func() {
		b.mtx.Unlock()
		if unwind {
			cerr := b.Close(ctx)
			if cerr != nil {
				log.Debugf("new unwind exited with: %v", cerr)
				err = errors.Join(err, cerr)
			}
			clear(b.pool)
		}
	}()

	for name, db := range b.cfg.Tables {
		err = b.openDB(ctx, db, name)
		if err != nil {
			err = fmt.Errorf("db pool %v: %w", name, err)
			return err
		}
	}

	unwind = false // Everything is good, do not unwind.
	b.open = true

	return err
}

func (l *multiDB) Close(ctx context.Context) error {
	log.Tracef("Close")
	defer log.Tracef("Close exit")

	l.mtx.Lock()
	defer l.mtx.Unlock()

	if !l.open {
		return larry.ErrDBClosed
	}

	var errSeen error

	for k, v := range l.pool {
		if err := v.Close(ctx); err != nil {
			log.Errorf("close %v: %v", k, err)
			errSeen = errors.Join(errSeen, err)
		}
		delete(l.pool, k)
	}

	l.open = false
	return errSeen
}

func (b *multiDB) Del(ctx context.Context, table string, key []byte) error {
	db, ok := b.pool[table]
	if !ok {
		return larry.ErrTableNotFound
	}
	return db.Del(ctx, dbname, key)
}

func (b *multiDB) Has(ctx context.Context, table string, key []byte) (bool, error) {
	db, ok := b.pool[table]
	if !ok {
		return false, larry.ErrTableNotFound
	}
	return db.Has(ctx, dbname, key)
}

func (b *multiDB) Get(ctx context.Context, table string, key []byte) ([]byte, error) {
	db, ok := b.pool[table]
	if !ok {
		return nil, larry.ErrTableNotFound
	}
	return db.Get(ctx, dbname, key)
}

func (b *multiDB) Put(ctx context.Context, table string, key, value []byte) error {
	db, ok := b.pool[table]
	if !ok {
		return larry.ErrTableNotFound
	}
	return db.Put(ctx, dbname, key, value)
}

func (b *multiDB) Begin(_ context.Context, write bool) (larry.Transaction, error) {
	return &multiTX{
		db:    b,
		txs:   make(map[string]larry.Transaction, len(b.pool)),
		write: write,
	}, nil
}

func (b *multiDB) execute(ctx context.Context, write bool, callback func(ctx context.Context, tx larry.Transaction) error) error {
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

func (b *multiDB) View(ctx context.Context, callback func(ctx context.Context, tx larry.Transaction) error) error {
	return b.execute(ctx, false, callback)
}

func (b *multiDB) Update(ctx context.Context, callback func(ctx context.Context, tx larry.Transaction) error) error {
	return b.execute(ctx, true, callback)
}

func (b *multiDB) NewIterator(ctx context.Context, table string) (larry.Iterator, error) {
	idb, ok := b.pool[table]
	if !ok {
		return nil, larry.ErrTableNotFound
	}
	return idb.NewIterator(ctx, dbname)
}

func (b *multiDB) NewRange(ctx context.Context, table string, start, end []byte) (larry.Range, error) {
	idb, ok := b.pool[table]
	if !ok {
		return nil, larry.ErrTableNotFound
	}
	return idb.NewRange(ctx, dbname, start, end)
}

func (b *multiDB) NewBatch(ctx context.Context) (larry.Batch, error) {
	return &multiBatch{
		db:  b,
		bts: make(map[string]larry.Batch, len(b.pool)),
	}, nil
}

// Transactions

// If a TX operates on multiple internal dbs, we cannot ensure the
// order between them. To ensure an order, one must create a TX per table.
type multiTX struct {
	db    *multiDB
	txs   map[string]larry.Transaction
	write bool
}

func (tx *multiTX) addInternalTx(ctx context.Context, table string) error {
	idb, ok := tx.db.pool[table]
	if !ok {
		return larry.ErrTableNotFound
	}

	if _, ok := tx.txs[table]; !ok {
		itx, err := idb.Begin(ctx, tx.write)
		if err != nil {
			return fmt.Errorf("create internal tx %v: %w", table, err)
		}
		tx.txs[table] = itx
	}

	return nil
}

func (tx *multiTX) Del(ctx context.Context, table string, key []byte) error {
	if err := tx.addInternalTx(ctx, table); err != nil {
		return err
	}
	return tx.txs[table].Del(ctx, dbname, key)
}

func (tx *multiTX) Has(ctx context.Context, table string, key []byte) (bool, error) {
	if err := tx.addInternalTx(ctx, table); err != nil {
		return false, err
	}
	return tx.txs[table].Has(ctx, dbname, key)
}

func (tx *multiTX) Get(ctx context.Context, table string, key []byte) ([]byte, error) {
	if err := tx.addInternalTx(ctx, table); err != nil {
		return nil, err
	}
	return tx.txs[table].Get(ctx, dbname, key)
}

func (tx *multiTX) Put(ctx context.Context, table string, key []byte, value []byte) error {
	if err := tx.addInternalTx(ctx, table); err != nil {
		return err
	}
	return tx.txs[table].Put(ctx, dbname, key, value)
}

func (tx *multiTX) Commit(ctx context.Context) error {
	var errSeen error
	for table, itx := range tx.txs {
		if err := itx.Commit(ctx); err != nil {
			errSeen = errors.Join(errSeen, fmt.Errorf("commit tx %v, %w", table, err))
		}
	}
	return errSeen
}

func (tx *multiTX) Rollback(ctx context.Context) error {
	var errSeen error
	for table, itx := range tx.txs {
		if err := itx.Rollback(ctx); err != nil {
			errSeen = errors.Join(errSeen, fmt.Errorf("rollback tx %v, %w", table, err))
		}
	}
	return errSeen
}

func (tx *multiTX) Write(ctx context.Context, b larry.Batch) error {
	bb, ok := b.(*multiBatch)
	if !ok {
		return fmt.Errorf("unexpected batch type: %T", b)
	}
	for table, ibt := range bb.bts {
		if err := tx.addInternalTx(ctx, table); err != nil {
			return fmt.Errorf("add internal tx %v: %w", table, err)
		}
		if err := tx.txs[table].Write(ctx, ibt); err != nil {
			return fmt.Errorf("write batch %v: %w", table, err)
		}
	}
	return nil
}

// Batches

// If a batch operates on multiple internal dbs, we cannot ensure the
// order between them. To ensure an order, one must create a batch per table.
type multiBatch struct {
	db  *multiDB
	bts map[string]larry.Batch
}

func (ab *multiBatch) addInternalBatch(ctx context.Context, table string) error {
	idb, ok := ab.db.pool[table]
	if !ok {
		return larry.ErrTableNotFound
	}

	if _, ok := ab.bts[table]; !ok {
		ibt, err := idb.NewBatch(ctx)
		if err != nil {
			return fmt.Errorf("create internal batch %v: %w", table, err)
		}
		ab.bts[table] = ibt
	}

	return nil
}

func (ab *multiBatch) Del(ctx context.Context, table string, key []byte) {
	if err := ab.addInternalBatch(ctx, table); err != nil {
		log.Errorf("batch del %v: %v", table, err)
		return
	}
	ab.bts[table].Del(ctx, dbname, key)
}

func (ab *multiBatch) Put(ctx context.Context, table string, key, value []byte) {
	if err := ab.addInternalBatch(ctx, table); err != nil {
		log.Errorf("batch put %v: %v", table, err)
		return
	}
	ab.bts[table].Put(ctx, dbname, key, value)
}

func (ab *multiBatch) Reset(ctx context.Context) {
	ab.bts = make(map[string]larry.Batch, len(ab.db.pool))
}
