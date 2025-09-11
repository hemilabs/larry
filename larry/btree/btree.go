// Copyright (c) 2025 Hemi Labs, Inc.
// Use of this source code is governed by the MIT License,
// which can be found in the LICENSE file.

package btree

import (
	"context"
	"os"
	"path/filepath"

	"github.com/guycipher/btree"
	"github.com/hemilabs/larry/larry"
)

// Assert required interfaces
var (
	_ larry.Batch       = (*btreeBatch)(nil)
	_ larry.Database    = (*btreeDB)(nil)
	_ larry.Iterator    = (*btreeIterator)(nil)
	_ larry.Range       = (*btreeRange)(nil)
	_ larry.Transaction = (*btreeTX)(nil)
)

type BtreeConfig struct {
	Home   string
	Tables []string
}

func DefaultBtreeConfig(home string, tables []string) *BtreeConfig {
	return &BtreeConfig{
		Home:   home,
		Tables: tables,
	}
}

func NewBtreeDB(cfg *BtreeConfig) (larry.Database, error) {
	if cfg == nil {
		return nil, larry.ErrInvalidConfig
	}
	bdb := &btreeDB{
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

// Database
type btreeDB struct {
	cfg    *BtreeConfig
	tables map[string]struct{}

	db *btree.BTree
}

func (db *btreeDB) Open(context.Context) error {
	bt, err := btree.Open(filepath.Join(db.cfg.Home, "btree.db"),
		os.O_CREATE|os.O_RDWR, 0o600, 3)
	if err != nil {
		return err
	}
	db.db = bt
	return nil
}

func (db *btreeDB) Close(context.Context) error {
	return db.db.Close()
}

func (db *btreeDB) Del(ctx context.Context, table string, key []byte) error {
	return db.db.Delete(larry.NewCompositeKey(table, key))
}

func (db *btreeDB) Has(ctx context.Context, table string, key []byte) (bool, error) {
	//nolint:nilerr // XXX implement proper has
	if _, err := db.Get(ctx, table, key); err != nil {
		return false, nil
	}
	return true, nil
}

func (db *btreeDB) Get(ctx context.Context, table string, key []byte) ([]byte, error) {
	value, err := db.db.Get(larry.NewCompositeKey(table, key))
	if err != nil {
		return nil, err
	}
	// XXX btree can return multiples, deal with this instead of returning
	// index 0.
	return value.V[0], nil
}

func (db *btreeDB) Put(ctx context.Context, table string, key []byte, value []byte) error {
	return larry.ErrDummy
}

func (db *btreeDB) Begin(ctx context.Context, write bool) (larry.Transaction, error) {
	return &btreeTX{}, nil
}

func (db *btreeDB) Update(ctx context.Context, callback func(ctx context.Context, tx larry.Transaction) error) error {
	return larry.ErrDummy
}

func (db *btreeDB) View(ctx context.Context, callback func(ctx context.Context, tx larry.Transaction) error) error {
	return larry.ErrDummy
}

func (db *btreeDB) NewIterator(ctx context.Context, table string) (larry.Iterator, error) {
	return &btreeIterator{}, nil
}

func (db *btreeDB) NewRange(ctx context.Context, table string, start, end []byte) (larry.Range, error) {
	return &btreeRange{}, nil
}

func (db *btreeDB) NewBatch(ctx context.Context) (larry.Batch, error) {
	return &btreeBatch{}, nil
}

// Batch
type btreeBatch struct{}

func (*btreeBatch) Del(ctx context.Context, table string, key []byte)        {}
func (*btreeBatch) Put(ctx context.Context, table string, key, value []byte) {}
func (*btreeBatch) Reset(ctx context.Context)                                {}

// Iterator
type btreeIterator struct{}

func (it *btreeIterator) First(ctx context.Context) bool {
	return false
}

func (it *btreeIterator) Last(ctx context.Context) bool {
	return false
}

func (it *btreeIterator) Next(ctx context.Context) bool {
	return false
}

func (it *btreeIterator) Seek(ctx context.Context, key []byte) bool {
	return false
}

func (it *btreeIterator) Key(ctx context.Context) []byte {
	return nil
}

func (it *btreeIterator) Value(ctx context.Context) []byte {
	return nil
}

func (it *btreeIterator) Close(ctx context.Context) {}

// Range
type btreeRange struct{}

func (r *btreeRange) First(ctx context.Context) bool {
	return false
}

func (r *btreeRange) Last(ctx context.Context) bool {
	return false
}

func (r *btreeRange) Next(ctx context.Context) bool {
	return false
}

func (r *btreeRange) Key(ctx context.Context) []byte {
	return nil
}

func (r *btreeRange) Value(ctx context.Context) []byte {
	return nil
}

func (r *btreeRange) Close(ctx context.Context) {}

// Transaction
type btreeTX struct{}

func (tx *btreeTX) Del(ctx context.Context, table string, key []byte) error {
	return larry.ErrDummy
}

func (tx *btreeTX) Has(ctx context.Context, table string, key []byte) (bool, error) {
	return false, larry.ErrDummy
}

func (tx *btreeTX) Get(ctx context.Context, table string, key []byte) ([]byte, error) {
	return nil, larry.ErrDummy
}

func (tx *btreeTX) Put(ctx context.Context, table string, key []byte, value []byte) error {
	return larry.ErrDummy
}

func (tx *btreeTX) Commit(ctx context.Context) error {
	return larry.ErrDummy
}

func (tx *btreeTX) Rollback(ctx context.Context) error {
	return larry.ErrDummy
}

func (tx *btreeTX) Write(ctx context.Context, b larry.Batch) error {
	return larry.ErrDummy
}
