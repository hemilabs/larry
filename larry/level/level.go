// Copyright (c) 2025 Hemi Labs, Inc.
// Use of this source code is governed by the MIT License,
// which can be found in the LICENSE file.

package level

import (
	"context"
	"errors"
	"fmt"

	"github.com/hemilabs/larry/larry"
	"github.com/juju/loggo"
	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/iterator"
	"github.com/syndtr/goleveldb/leveldb/opt"
	"github.com/syndtr/goleveldb/leveldb/util"
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
	_ larry.Batch       = (*levelBatch)(nil)
	_ larry.Database    = (*levelDB)(nil)
	_ larry.Iterator    = (*levelIterator)(nil)
	_ larry.Range       = (*levelRange)(nil)
	_ larry.Transaction = (*levelTX)(nil)
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

type LevelConfig struct {
	Home   string
	Tables []string
}

func DefaultLevelConfig(home string, tables []string) *LevelConfig {
	return &LevelConfig{
		Home:   home,
		Tables: tables,
	}
}

type levelDB struct {
	db *leveldb.DB

	tables map[string]struct{}

	cfg *LevelConfig
}

func NewLevelDB(cfg *LevelConfig) (larry.Database, error) {
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

func (b *levelDB) Begin(_ context.Context, write bool) (larry.Transaction, error) {
	tx, err := b.db.OpenTransaction()
	if err != nil {
		return nil, xerr(err)
	}
	return &levelTX{
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

func (b *levelDB) NewIterator(ctx context.Context, table string) (larry.Iterator, error) {
	if _, ok := b.tables[table]; !ok {
		return nil, larry.ErrTableNotFound
	}
	r := util.BytesPrefix(larry.NewCompositeKey(table, nil))
	return &levelIterator{
		table: table,
		it:    b.db.NewIterator(r, nil),
	}, nil
}

func (b *levelDB) NewRange(ctx context.Context, table string, start, end []byte) (larry.Range, error) {
	if _, ok := b.tables[table]; !ok {
		return nil, larry.ErrTableNotFound
	}
	return &levelRange{
		table: table,
		it: b.db.NewIterator(&util.Range{
			Start: larry.NewCompositeKey(table, start),
			Limit: larry.NewCompositeKey(table, end),
		}, nil),
		start: start,
		end:   end,
	}, nil
}

func (b *levelDB) NewBatch(ctx context.Context) (larry.Batch, error) {
	return &levelBatch{db: b, wb: new(leveldb.Batch)}, nil
}

// Transactions

type levelTX struct {
	db *levelDB
	tx *leveldb.Transaction
}

func (tx *levelTX) Del(ctx context.Context, table string, key []byte) error {
	if _, ok := tx.db.tables[table]; !ok {
		return larry.ErrTableNotFound
	}
	return xerr(tx.tx.Delete(larry.NewCompositeKey(table, key), nil))
}

func (tx *levelTX) Has(ctx context.Context, table string, key []byte) (bool, error) {
	if _, ok := tx.db.tables[table]; !ok {
		return false, larry.ErrTableNotFound
	}
	has, err := tx.tx.Has(larry.NewCompositeKey(table, key), nil)
	return has, xerr(err)
}

func (tx *levelTX) Get(ctx context.Context, table string, key []byte) ([]byte, error) {
	if _, ok := tx.db.tables[table]; !ok {
		return nil, larry.ErrTableNotFound
	}
	value, err := tx.tx.Get(larry.NewCompositeKey(table, key), nil)
	return value, xerr(err)
}

func (tx *levelTX) Put(ctx context.Context, table string, key []byte, value []byte) error {
	if _, ok := tx.db.tables[table]; !ok {
		return larry.ErrTableNotFound
	}
	if key == nil {
		return nil
	}
	return xerr(tx.tx.Put(larry.NewCompositeKey(table, key), value, nil))
}

func (tx *levelTX) Commit(ctx context.Context) error {
	return xerr(tx.tx.Commit())
}

func (tx *levelTX) Rollback(ctx context.Context) error {
	tx.tx.Discard()
	return nil
}

func (tx *levelTX) Write(ctx context.Context, b larry.Batch) error {
	return xerr(tx.tx.Write(b.(*levelBatch).wb, nil))
}

// Iterations
type levelIterator struct {
	table string
	it    iterator.Iterator
}

func (ni *levelIterator) First(_ context.Context) bool {
	return ni.it.First()
}

func (ni *levelIterator) Last(_ context.Context) bool {
	return ni.it.Last()
}

func (ni *levelIterator) Next(_ context.Context) bool {
	return ni.it.Next()
}

func (ni *levelIterator) Seek(_ context.Context, key []byte) bool {
	return ni.it.Seek(larry.NewCompositeKey(ni.table, key))
}

func (ni *levelIterator) Key(_ context.Context) []byte {
	return larry.KeyFromComposite(ni.table, ni.it.Key())
}

func (ni *levelIterator) Value(_ context.Context) []byte {
	return ni.it.Value()
}

func (ni *levelIterator) Close(ctx context.Context) {
	ni.it.Release()
}

// Ranges
type levelRange struct {
	table string
	it    iterator.Iterator
	start []byte
	end   []byte
}

func (nr *levelRange) First(_ context.Context) bool {
	return nr.it.First()
}

func (nr *levelRange) Last(_ context.Context) bool {
	return nr.it.Last()
}

func (nr *levelRange) Next(_ context.Context) bool {
	return nr.it.Next()
}

func (nr *levelRange) Key(ctx context.Context) []byte {
	return larry.KeyFromComposite(nr.table, nr.it.Key())
}

func (nr *levelRange) Value(ctx context.Context) []byte {
	return nr.it.Value()
}

func (nr *levelRange) Close(ctx context.Context) {
	nr.it.Release()
}

// Batches

type levelBatch struct {
	db *levelDB
	wb *leveldb.Batch
}

func (nb *levelBatch) Del(ctx context.Context, table string, key []byte) {
	if _, ok := nb.db.tables[table]; !ok {
		log.Errorf("%s: %v", table, larry.ErrTableNotFound)
		return
	}
	nb.wb.Delete(larry.NewCompositeKey(table, key))
}

func (nb *levelBatch) Put(ctx context.Context, table string, key, value []byte) {
	if _, ok := nb.db.tables[table]; !ok {
		log.Errorf("%s: %v", table, larry.ErrTableNotFound)
		return
	}
	if key == nil {
		return
	}
	nb.wb.Put(larry.NewCompositeKey(table, key), value)
}

func (nb *levelBatch) Reset(ctx context.Context) {
	nb.wb.Reset()
}
