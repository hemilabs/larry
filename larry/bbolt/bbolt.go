// Copyright (c) 2025 Hemi Labs, Inc.
// Use of this source code is governed by the MIT License,
// which can be found in the LICENSE file.

package bbolt

import (
	"bytes"
	"container/list"
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"sync"

	"github.com/hemilabs/larry/larry"
	"github.com/juju/loggo"
	bolt "go.etcd.io/bbolt"
	bolterrs "go.etcd.io/bbolt/errors"
)

const logLevel = "INFO"

var log = loggo.GetLogger("pebble")

func init() {
	if err := loggo.ConfigureLoggers(logLevel); err != nil {
		panic(err)
	}
}

// Assert required interfaces
var (
	_ larry.Batch       = (*boltBatch)(nil)
	_ larry.Database    = (*boltDB)(nil)
	_ larry.Iterator    = (*boltIterator)(nil)
	_ larry.Range       = (*boltRange)(nil)
	_ larry.Transaction = (*boltTX)(nil)
)

func xerr(err error) error {
	switch {
	case errors.Is(err, bolterrs.ErrDatabaseNotOpen):
		err = larry.ErrDBClosed
	case errors.Is(err, bolterrs.ErrKeyRequired):
		err = nil
	}
	return err
}

type BoltConfig struct {
	Home   string
	Tables []string
}

func DefaultBoltConfig(home string, tables []string) *BoltConfig {
	return &BoltConfig{
		Home:   home,
		Tables: tables,
	}
}

type boltDB struct {
	db *bolt.DB

	tables map[string]struct{}

	cfg *BoltConfig

	// bbolt blocks if we try to open the db while it
	// is already open, so we must use a synced variable
	mtx  sync.Mutex
	open bool
}

func NewBoltDB(cfg *BoltConfig) (larry.Database, error) {
	if cfg == nil {
		return nil, larry.ErrInvalidConfig
	}
	bdb := &boltDB{
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

func (b *boltDB) Open(_ context.Context) error {
	b.mtx.Lock()
	defer b.mtx.Unlock()

	if b.open {
		return larry.ErrDBOpen
	}

	ndb, err := bolt.Open(filepath.Join(b.cfg.Home, "bolt.db"), 0o600, nil)
	if err != nil {
		return err
	}
	err = ndb.Update(func(tx *bolt.Tx) error {
		for _, table := range b.cfg.Tables {
			_, err := tx.CreateBucketIfNotExists([]byte(table))
			if err != nil {
				return fmt.Errorf("could not create table: %v", table)
			}
		}
		return nil
	})
	if err != nil {
		return xerr(err)
	}
	b.db = ndb
	b.open = true
	return nil
}

// Note: blocks when waiting for pending Txs
func (b *boltDB) Close(_ context.Context) error {
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

func (b *boltDB) Del(ctx context.Context, table string, key []byte) error {
	err := b.db.Update(func(tx *bolt.Tx) error {
		btx := &boltTX{tx: tx}
		return btx.Del(ctx, table, key)
	})
	return err
}

func (b *boltDB) Has(ctx context.Context, table string, key []byte) (bool, error) {
	_, err := b.Get(ctx, table, key)
	if errors.Is(err, larry.ErrKeyNotFound) {
		return false, nil
	}
	return err == nil, err
}

func (b *boltDB) Get(ctx context.Context, table string, key []byte) ([]byte, error) {
	var value []byte
	var err error
	err = b.db.View(func(tx *bolt.Tx) error {
		btx := &boltTX{tx: tx}
		value, err = btx.Get(ctx, table, key)
		return err
	})
	return value, err
}

func (b *boltDB) Put(ctx context.Context, table string, key, value []byte) error {
	return b.db.Update(func(tx *bolt.Tx) error {
		btx := &boltTX{tx: tx}
		return btx.Put(ctx, table, key, value)
	})
}

func (b *boltDB) Begin(_ context.Context, write bool) (larry.Transaction, error) {
	tx, err := b.db.Begin(write)
	if err != nil {
		return nil, xerr(err)
	}
	return &boltTX{
		tx: tx,
	}, nil
}

// execute runs a transaction and commits or rolls it back depending on errors.
func (b *boltDB) execute(ctx context.Context, write bool, callback func(ctx context.Context, tx larry.Transaction) error) error {
	itx, err := b.Begin(ctx, write)
	if err != nil {
		return err
	}
	err = callback(ctx, itx)
	if err != nil {
		if rberr := itx.Rollback(ctx); rberr != nil {
			return fmt.Errorf("rollback: callback: %w -> %w", err, rberr)
		}
		return xerr(err)
	}
	return itx.Commit(ctx)
}

func (b *boltDB) View(ctx context.Context, callback func(ctx context.Context, tx larry.Transaction) error) error {
	return b.execute(ctx, false, callback)
}

func (b *boltDB) Update(ctx context.Context, callback func(ctx context.Context, tx larry.Transaction) error) error {
	return b.execute(ctx, true, callback)
}

func (b *boltDB) NewIterator(ctx context.Context, table string) (larry.Iterator, error) {
	tx, err := b.Begin(ctx, false)
	if err != nil {
		return nil, err
	}
	bu := tx.(*boltTX).tx.Bucket([]byte(table))
	if bu == nil {
		return nil, larry.ErrTableNotFound
	}
	return &boltIterator{
		tx: tx,
		it: bu.Cursor(),
	}, nil
}

func (b *boltDB) NewRange(ctx context.Context, table string, start, end []byte) (larry.Range, error) {
	tx, err := b.Begin(ctx, false)
	if err != nil {
		return nil, err
	}
	nr := &boltRange{
		tx:    tx,
		start: start,
		end:   end,
	}
	bu := tx.(*boltTX).tx.Bucket([]byte(table))
	if bu == nil {
		if err := tx.Rollback(ctx); err != nil {
			log.Errorf("close tx: %v", err)
		}
		return nil, larry.ErrTableNotFound
	}
	nr.it = bu.Cursor()
	return nr, nil
}

func (b *boltDB) NewBatch(ctx context.Context) (larry.Batch, error) {
	return &boltBatch{wb: new(list.List)}, nil
}

// Transactions

type boltTX struct {
	tx *bolt.Tx
}

func (tx *boltTX) Del(ctx context.Context, table string, key []byte) error {
	bu := tx.tx.Bucket([]byte(table))
	if bu == nil {
		return larry.ErrTableNotFound
	}
	return xerr(bu.Delete(key))
}

func (tx *boltTX) Has(ctx context.Context, table string, key []byte) (bool, error) {
	_, err := tx.Get(ctx, table, key)
	if errors.Is(err, larry.ErrKeyNotFound) {
		return false, nil
	}
	return err == nil, xerr(err)
}

func (tx *boltTX) Get(ctx context.Context, table string, key []byte) ([]byte, error) {
	bu := tx.tx.Bucket([]byte(table))
	if bu == nil {
		return nil, larry.ErrTableNotFound
	}
	val := bu.Get(key)
	if val == nil {
		return nil, larry.ErrKeyNotFound
	}
	// bbolt prevents values from being modified or will panic
	value := make([]byte, len(val))
	copy(value, val)
	return value, nil
}

func (tx *boltTX) Put(ctx context.Context, table string, key []byte, value []byte) error {
	bu := tx.tx.Bucket([]byte(table))
	if bu == nil {
		return larry.ErrTableNotFound
	}
	err := bu.Put(key, value)
	if err != nil {
		return xerr(err)
	}
	return nil
}

func (tx *boltTX) Commit(ctx context.Context) error {
	if !tx.tx.Writable() {
		return tx.Rollback(ctx)
	}
	return xerr(tx.tx.Commit())
}

func (tx *boltTX) Rollback(ctx context.Context) error {
	return xerr(tx.tx.Rollback())
}

func (tx *boltTX) Write(ctx context.Context, b larry.Batch) error {
	bb, ok := b.(*boltBatch)
	if !ok {
		return fmt.Errorf("unexpected batch type: %T", b)
	}
	for e := bb.wb.Front(); e != nil; e = e.Next() {
		f, ok := e.Value.(larry.BatchFunc)
		if !ok {
			return fmt.Errorf("unexpected batch element type %T", e.Value)
		}
		if err := f(ctx, tx); err != nil {
			return xerr(err)
		}
	}
	return nil
}

// Iterations
type boltIterator struct {
	tx  larry.Transaction
	it  *bolt.Cursor
	key []byte
	val []byte

	first bool
}

func (ni *boltIterator) First(_ context.Context) bool {
	ni.key, ni.val = ni.it.First()
	return ni.key != nil
}

func (ni *boltIterator) Last(_ context.Context) bool {
	ni.key, ni.val = ni.it.Last()
	return ni.key != nil
}

func (ni *boltIterator) Next(ctx context.Context) bool {
	if !ni.first {
		ni.first = true
		return ni.First(ctx)
	}
	ni.key, ni.val = ni.it.Next()
	return ni.key != nil
}

func (ni *boltIterator) Seek(ctx context.Context, key []byte) bool {
	ni.first = true
	ni.key, ni.val = ni.it.Seek(key)
	return ni.key != nil
}

func (ni *boltIterator) Key(_ context.Context) []byte {
	return ni.key
}

func (ni *boltIterator) Value(_ context.Context) []byte {
	return ni.val
}

func (ni *boltIterator) Close(ctx context.Context) {
	err := ni.tx.Commit(ctx)
	if err != nil {
		log.Errorf("iterator close: %v", err)
	}
}

// Ranges
type boltRange struct {
	tx larry.Transaction
	it *bolt.Cursor

	start []byte
	end   []byte
	key   []byte
	val   []byte

	first bool
}

func (nr *boltRange) First(_ context.Context) bool {
	nr.key, nr.val = nr.it.Seek(nr.start)
	if bytes.Compare(nr.key, nr.end) >= 0 {
		nr.key, nr.val = nil, nil
	}
	return nr.key != nil
}

func (nr *boltRange) Last(_ context.Context) bool {
	nr.key, nr.val = nr.it.Seek(nr.end)
	if nr.key == nil {
		nr.key, nr.val = nr.it.Last()
	}
	for nr.key != nil {
		if bytes.Compare(nr.key, nr.end) < 0 {
			return true
		}
		nr.key, nr.val = nr.it.Prev()
	}
	return false
}

func (nr *boltRange) Next(ctx context.Context) bool {
	if !nr.first {
		nr.first = true
		return nr.First(ctx)
	}
	nr.key, nr.val = nr.it.Next()
	if bytes.Compare(nr.key, nr.end) >= 0 {
		nr.key, nr.val = nil, nil
	}
	return nr.key != nil
}

func (nr *boltRange) Key(_ context.Context) []byte {
	return nr.key
}

func (nr *boltRange) Value(ctx context.Context) []byte {
	return nr.val
}

func (nr *boltRange) Close(ctx context.Context) {
	err := nr.tx.Commit(ctx)
	if err != nil {
		log.Errorf("range close: %v", err)
	}
}

// Batches

type boltBatch struct {
	wb *list.List // elements of type batchFunc
}

func (nb *boltBatch) Del(ctx context.Context, table string, key []byte) {
	var act larry.BatchFunc = func(ctx context.Context, tx larry.Transaction) error {
		return tx.Del(ctx, table, key)
	}
	nb.wb.PushBack(act)
}

func (nb *boltBatch) Put(ctx context.Context, table string, key, value []byte) {
	var act larry.BatchFunc = func(ctx context.Context, tx larry.Transaction) error {
		return tx.Put(ctx, table, key, value)
	}
	nb.wb.PushBack(act)
}

func (nb *boltBatch) Reset(ctx context.Context) {
	nb.wb.Init()
}
