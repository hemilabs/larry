// Copyright (c) 2024-2025 Hemi Labs, Inc.
// Use of this source code is governed by the MIT License,
// which can be found in the LICENSE file.

package aggregator

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"

	"github.com/hemilabs/larry/larry"
	"github.com/hemilabs/larry/larry/level"
	"github.com/hemilabs/larry/larry/rawdb"
	"github.com/juju/loggo"
	"github.com/mitchellh/go-homedir"
)

const logLevel = "INFO"

var log = loggo.GetLogger("aggregator")

func init() {
	if err := loggo.ConfigureLoggers(logLevel); err != nil {
		panic(err)
	}
}

// Assert required interfaces
var (
	_ larry.Batch       = (*aggregatorBatch)(nil)
	_ larry.Database    = (*aggregatorDB)(nil)
	_ larry.Transaction = (*aggregatorTX)(nil)
)

type AggregatorConfig struct {
	Home   string
	DB     string
	Tables []string // name / db type
}

func DefaultAggregatorConfig(home string, db string, tables []string) *AggregatorConfig {
	return &AggregatorConfig{
		Home:   home,
		Tables: tables,
		DB:     db,
	}
}

type pool map[string]larry.Database

type rawPool map[string]*rawdb.RawDB

type aggregatorDB struct {
	mtx sync.RWMutex

	pool pool // database pool

	cfg *AggregatorConfig

	open bool
}

func (l *aggregatorDB) openDB(ctx context.Context, db, name string) error {
	bhs := filepath.Join(l.cfg.Home, name)

	var (
		bhsDB larry.Database
		err   error
	)

	switch db {
	case "level":
		cfg := level.DefaultLevelConfig(bhs, []string{""})
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
	l.pool[name] = bhsDB

	return nil
}

func NewAggregatorDB(cfg *AggregatorConfig) (larry.Database, error) {
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

	l := &aggregatorDB{
		cfg:  cfg,
		pool: make(pool),
	}

	return l, nil
}

func (b *aggregatorDB) Open(ctx context.Context) error {
	log.Tracef("Open")
	defer log.Tracef("Open exit")

	b.mtx.Lock()
	defer b.mtx.Unlock()

	if b.open {
		return larry.ErrDBOpen
	}

	var err error
	unwind := true
	defer func() {
		if unwind {
			cerr := b.Close(ctx)
			if cerr != nil {
				log.Debugf("new unwind exited with: %v", cerr)
				err = errors.Join(err, cerr)
			}
			clear(b.pool)
		}
	}()

	for _, name := range b.cfg.Tables {
		err = b.openDB(ctx, b.cfg.DB, name)
		if err != nil {
			return fmt.Errorf("db pool %v: %w", name, err)
		}
	}

	unwind = false // Everything is good, do not unwind.
	b.open = true

	return err
}

func (l *aggregatorDB) Close(ctx context.Context) error {
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
			// do continue, leveldb does not like unfresh shutdowns
			log.Errorf("close %v: %v", k, err)
			errSeen = errors.Join(errSeen, err)
		}
		delete(l.pool, k)
	}

	l.open = false
	return errSeen
}

func (b *aggregatorDB) Del(ctx context.Context, table string, key []byte) error {
	db, ok := b.pool[table]
	if !ok {
		return larry.ErrTableNotFound
	}
	return db.Del(ctx, "", key)
}

func (b *aggregatorDB) Has(ctx context.Context, table string, key []byte) (bool, error) {
	db, ok := b.pool[table]
	if !ok {
		return false, larry.ErrTableNotFound
	}
	return db.Has(ctx, "", key)
}

func (b *aggregatorDB) Get(ctx context.Context, table string, key []byte) ([]byte, error) {
	db, ok := b.pool[table]
	if !ok {
		return nil, larry.ErrTableNotFound
	}
	return db.Get(ctx, "", key)
}

func (b *aggregatorDB) Put(ctx context.Context, table string, key, value []byte) error {
	db, ok := b.pool[table]
	if !ok {
		return larry.ErrTableNotFound
	}
	return db.Put(ctx, "", key, value)
}

func (b *aggregatorDB) Begin(_ context.Context, write bool) (larry.Transaction, error) {
	return &aggregatorTX{
		db:    b,
		txs:   make(map[string]larry.Transaction, len(b.pool)),
		write: write,
	}, nil
}

func (b *aggregatorDB) execute(ctx context.Context, write bool, callback func(ctx context.Context, tx larry.Transaction) error) error {
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

func (b *aggregatorDB) View(ctx context.Context, callback func(ctx context.Context, tx larry.Transaction) error) error {
	return b.execute(ctx, false, callback)
}

func (b *aggregatorDB) Update(ctx context.Context, callback func(ctx context.Context, tx larry.Transaction) error) error {
	return b.execute(ctx, true, callback)
}

func (b *aggregatorDB) NewIterator(ctx context.Context, table string) (larry.Iterator, error) {
	idb, ok := b.pool[table]
	if !ok {
		return nil, larry.ErrTableNotFound
	}
	return idb.NewIterator(ctx, "")
}

func (b *aggregatorDB) NewRange(ctx context.Context, table string, start, end []byte) (larry.Range, error) {
	idb, ok := b.pool[table]
	if !ok {
		return nil, larry.ErrTableNotFound
	}
	return idb.NewRange(ctx, "", start, end)
}

func (b *aggregatorDB) NewBatch(ctx context.Context) (larry.Batch, error) {
	return &aggregatorBatch{
		db:  b,
		bts: make(map[string]larry.Batch, len(b.pool)),
	}, nil
}

// Transactions

type aggregatorTX struct {
	db    *aggregatorDB
	txs   map[string]larry.Transaction
	write bool
}

func (tx *aggregatorTX) addInternalTx(ctx context.Context, table string) error {
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

func (tx *aggregatorTX) Del(ctx context.Context, table string, key []byte) error {
	if err := tx.addInternalTx(ctx, table); err != nil {
		return err
	}
	return tx.txs[table].Del(ctx, "", key)
}

func (tx *aggregatorTX) Has(ctx context.Context, table string, key []byte) (bool, error) {
	if err := tx.addInternalTx(ctx, table); err != nil {
		return false, err
	}
	return tx.txs[table].Has(ctx, "", key)
}

func (tx *aggregatorTX) Get(ctx context.Context, table string, key []byte) ([]byte, error) {
	if err := tx.addInternalTx(ctx, table); err != nil {
		return nil, err
	}
	return tx.txs[table].Get(ctx, "", key)
}

func (tx *aggregatorTX) Put(ctx context.Context, table string, key []byte, value []byte) error {
	if err := tx.addInternalTx(ctx, table); err != nil {
		return err
	}
	return tx.txs[table].Put(ctx, "", key, value)
}

func (tx *aggregatorTX) Commit(ctx context.Context) error {
	var errSeen error
	for table, itx := range tx.txs {
		if err := itx.Commit(ctx); err != nil {
			errors.Join(errSeen, fmt.Errorf("commit tx %v, %w", table, err))
		}
	}
	return errSeen
}

func (tx *aggregatorTX) Rollback(ctx context.Context) error {
	var errSeen error
	for table, itx := range tx.txs {
		if err := itx.Rollback(ctx); err != nil {
			errors.Join(errSeen, fmt.Errorf("rollback tx %v, %w", table, err))
		}
	}
	return errSeen
}

// If writing batches to multiple internal dbs, we cannot ensure the
// order between them, but this should not be a problem for the
// use case of aggregator.
func (tx *aggregatorTX) Write(ctx context.Context, b larry.Batch) error {
	bb, ok := b.(*aggregatorBatch)
	if !ok {
		return fmt.Errorf("unexpected batch type: %T", b)
	}
	for table, ibt := range bb.bts {
		tx.addInternalTx(ctx, table)
		if err := tx.txs[table].Write(ctx, ibt); err != nil {
			return fmt.Errorf("write batch %v: %w", table, err)
		}
	}
	return nil
}

// Batches

type aggregatorBatch struct {
	db  *aggregatorDB
	bts map[string]larry.Batch
}

func (ab *aggregatorBatch) addInternalBatch(ctx context.Context, table string) error {
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

func (ab *aggregatorBatch) Del(ctx context.Context, table string, key []byte) {
	if err := ab.addInternalBatch(ctx, table); err != nil {
		log.Errorf("batch del %v: %w", table, err)
		return
	}
	ab.bts[table].Del(ctx, "", key)
}

func (ab *aggregatorBatch) Put(ctx context.Context, table string, key, value []byte) {
	if err := ab.addInternalBatch(ctx, table); err != nil {
		log.Errorf("batch put %v: %w", table, err)
		return
	}
	ab.bts[table].Put(ctx, "", key, value)
}

func (ab *aggregatorBatch) Reset(ctx context.Context) {
	ab.bts = make(map[string]larry.Batch, len(ab.db.pool))
}
