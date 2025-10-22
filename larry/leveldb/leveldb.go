// Copyright (c) 2025 Hemi Labs, Inc.
// Use of this source code is governed by the MIT License,
// which can be found in the LICENSE file.

package leveldb

// This package adds a Larry database implementation using leveldb.

import (
	"context"
	"errors"
	"fmt"

	"github.com/juju/loggo"
	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/filter"
	"github.com/syndtr/goleveldb/leveldb/iterator"
	"github.com/syndtr/goleveldb/leveldb/opt"
	"github.com/syndtr/goleveldb/leveldb/util"

	"github.com/hemilabs/larry/larry"
)

const logLevel = "INFO"

var log = loggo.GetLogger("level")

func init() {
	if err := loggo.ConfigureLoggers(logLevel); err != nil {
		panic(err)
	}
}

// Assert required interfaces
var (
	_ larry.Batch       = (*leveldbBatch)(nil)
	_ larry.Database    = (*levelDB)(nil)
	_ larry.Iterator    = (*leveldbIterator)(nil)
	_ larry.Range       = (*leveldbRange)(nil)
	_ larry.Transaction = (*leveldbTX)(nil)
)

func xerr(err error) error {
	switch {
	case errors.Is(err, leveldb.ErrClosed):
		err = larry.ErrDBClosed
	case errors.Is(err, leveldb.ErrNotFound):
		err = larry.ErrKeyNotFound
	}
	return err
}

type Config struct {
	Home   string
	Tables []string
}

func DefaultLevelDBConfig(home string, tables []string) *Config {
	return &Config{
		Home:   home,
		Tables: tables,
	}
}

type levelDB struct {
	db *leveldb.DB

	tables map[string]struct{}

	cfg *Config
}

func NewLevelDB(cfg *Config) (larry.Database, error) {
	if cfg == nil {
		return nil, larry.ErrInvalidConfig
	}
	bdb := &levelDB{
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

func (b *levelDB) Open(_ context.Context) error {
	ldb, err := leveldb.OpenFile(b.cfg.Home, &opt.Options{
		BlockCacheEvictRemoved: true,
		Compression:            opt.NoCompression,
		Filter:                 filter.NewBloomFilter(10),
	})
	if err != nil {
		return xerr(err)
	}
	b.db = ldb
	return nil
}

func (b *levelDB) Close(_ context.Context) error {
	return xerr(b.db.Close())
}

func (b *levelDB) Del(_ context.Context, table string, key []byte) error {
	if _, ok := b.tables[table]; !ok {
		return larry.ErrTableNotFound
	}
	return xerr(b.db.Delete(larry.NewCompositeKey(table, key), nil))
}

func (b *levelDB) Has(_ context.Context, table string, key []byte) (bool, error) {
	if _, ok := b.tables[table]; !ok {
		return false, larry.ErrTableNotFound
	}
	has, err := b.db.Has(larry.NewCompositeKey(table, key), nil)
	return has, xerr(err)
}

func (b *levelDB) Get(_ context.Context, table string, key []byte) ([]byte, error) {
	if _, ok := b.tables[table]; !ok {
		return nil, larry.ErrTableNotFound
	}
	value, err := b.db.Get(larry.NewCompositeKey(table, key), nil)
	if err != nil {
		return nil, xerr(err)
	}
	return value, nil
}

func (b *levelDB) Put(_ context.Context, table string, key, value []byte) error {
	if _, ok := b.tables[table]; !ok {
		return larry.ErrTableNotFound
	}
	if key == nil {
		return nil
	}
	return xerr(b.db.Put(larry.NewCompositeKey(table, key), value, nil))
}

func (b *levelDB) Begin(_ context.Context, _ bool) (larry.Transaction, error) {
	tx, err := b.db.OpenTransaction()
	if err != nil {
		return nil, xerr(err)
	}
	return &leveldbTX{
		db: b,
		tx: tx,
	}, nil
}

func (b *levelDB) execute(ctx context.Context, write bool, callback func(ctx context.Context, tx larry.Transaction) error) error {
	tx, err := b.Begin(ctx, write)
	if err != nil {
		return err
	}
	err = callback(ctx, tx)
	if err != nil {
		if cerr := tx.Rollback(ctx); cerr != nil {
			return fmt.Errorf("rollback %w: %w", cerr, err)
		}
		return xerr(err)
	}
	if !write {
		return tx.Rollback(ctx)
	}
	return tx.Commit(ctx)
}

func (b *levelDB) View(ctx context.Context, callback func(ctx context.Context, tx larry.Transaction) error) error {
	return xerr(b.execute(ctx, false, callback))
}

func (b *levelDB) Update(ctx context.Context, callback func(ctx context.Context, tx larry.Transaction) error) error {
	return xerr(b.execute(ctx, true, callback))
}

func (b *levelDB) NewIterator(_ context.Context, table string) (larry.Iterator, error) {
	if _, ok := b.tables[table]; !ok {
		return nil, larry.ErrTableNotFound
	}
	r := util.BytesPrefix(larry.NewCompositeKey(table, nil))
	return &leveldbIterator{
		table: table,
		it:    b.db.NewIterator(r, nil),
	}, nil
}

func (b *levelDB) NewRange(_ context.Context, table string, start, end []byte) (larry.Range, error) {
	if _, ok := b.tables[table]; !ok {
		return nil, larry.ErrTableNotFound
	}
	r := util.BytesPrefix(larry.NewCompositeKey(table, nil))
	if end != nil {
		r.Limit = larry.NewCompositeKey(table, end)
	}
	return &leveldbRange{
		table: table,
		it: b.db.NewIterator(&util.Range{
			Start: larry.NewCompositeKey(table, start),
			Limit: r.Limit,
		}, nil),
		start: start,
		end:   end,
	}, nil
}

func (b *levelDB) NewBatch(_ context.Context) (larry.Batch, error) {
	return &leveldbBatch{db: b, wb: new(leveldb.Batch)}, nil
}

// Transactions

type leveldbTX struct {
	db *levelDB
	tx *leveldb.Transaction
}

func (tx *leveldbTX) Del(_ context.Context, table string, key []byte) error {
	if _, ok := tx.db.tables[table]; !ok {
		return larry.ErrTableNotFound
	}
	return xerr(tx.tx.Delete(larry.NewCompositeKey(table, key), nil))
}

func (tx *leveldbTX) Has(_ context.Context, table string, key []byte) (bool, error) {
	if _, ok := tx.db.tables[table]; !ok {
		return false, larry.ErrTableNotFound
	}
	has, err := tx.tx.Has(larry.NewCompositeKey(table, key), nil)
	return has, xerr(err)
}

func (tx *leveldbTX) Get(_ context.Context, table string, key []byte) ([]byte, error) {
	if _, ok := tx.db.tables[table]; !ok {
		return nil, larry.ErrTableNotFound
	}
	value, err := tx.tx.Get(larry.NewCompositeKey(table, key), nil)
	return value, xerr(err)
}

func (tx *leveldbTX) Put(_ context.Context, table string, key []byte, value []byte) error {
	if _, ok := tx.db.tables[table]; !ok {
		return larry.ErrTableNotFound
	}
	if key == nil {
		return nil
	}
	return xerr(tx.tx.Put(larry.NewCompositeKey(table, key), value, nil))
}

func (tx *leveldbTX) Commit(_ context.Context) error {
	return xerr(tx.tx.Commit())
}

func (tx *leveldbTX) Rollback(_ context.Context) error {
	tx.tx.Discard()
	return nil
}

func (tx *leveldbTX) Write(_ context.Context, b larry.Batch) error {
	return xerr(tx.tx.Write(b.(*leveldbBatch).wb, nil))
}

// Iterations
type leveldbIterator struct {
	table string
	it    iterator.Iterator
}

func (ni *leveldbIterator) First(_ context.Context) bool {
	return ni.it.First()
}

func (ni *leveldbIterator) Last(_ context.Context) bool {
	return ni.it.Last()
}

func (ni *leveldbIterator) Next(_ context.Context) bool {
	return ni.it.Next()
}

func (ni *leveldbIterator) Seek(_ context.Context, key []byte) bool {
	return ni.it.Seek(larry.NewCompositeKey(ni.table, key))
}

func (ni *leveldbIterator) Key(_ context.Context) []byte {
	return larry.KeyFromComposite(ni.table, ni.it.Key())
}

func (ni *leveldbIterator) Value(_ context.Context) []byte {
	return ni.it.Value()
}

func (ni *leveldbIterator) Close(_ context.Context) {
	ni.it.Release()
}

// Ranges
type leveldbRange struct {
	table string
	it    iterator.Iterator
	start []byte
	end   []byte
}

func (nr *leveldbRange) First(_ context.Context) bool {
	return nr.it.First()
}

func (nr *leveldbRange) Last(_ context.Context) bool {
	return nr.it.Last()
}

func (nr *leveldbRange) Next(_ context.Context) bool {
	return nr.it.Next()
}

func (nr *leveldbRange) Key(_ context.Context) []byte {
	return larry.KeyFromComposite(nr.table, nr.it.Key())
}

func (nr *leveldbRange) Value(_ context.Context) []byte {
	return nr.it.Value()
}

func (nr *leveldbRange) Close(_ context.Context) {
	nr.it.Release()
}

// Batches

type leveldbBatch struct {
	db *levelDB
	wb *leveldb.Batch
}

func (nb *leveldbBatch) Del(_ context.Context, table string, key []byte) {
	if _, ok := nb.db.tables[table]; !ok {
		log.Errorf("%s: %v", table, larry.ErrTableNotFound)
		return
	}
	nb.wb.Delete(larry.NewCompositeKey(table, key))
}

func (nb *leveldbBatch) Put(_ context.Context, table string, key, value []byte) {
	if _, ok := nb.db.tables[table]; !ok {
		log.Errorf("%s: %v", table, larry.ErrTableNotFound)
		return
	}
	if key == nil {
		return
	}
	nb.wb.Put(larry.NewCompositeKey(table, key), value)
}

func (nb *leveldbBatch) Reset(_ context.Context) {
	nb.wb.Reset()
}
