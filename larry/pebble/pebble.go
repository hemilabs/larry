// Copyright (c) 2025 Hemi Labs, Inc.
// Use of this source code is governed by the MIT License,
// which can be found in the LICENSE file.

// Package pebble implements a Larry database backend using pebble.
package pebble

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/cockroachdb/pebble"
	"github.com/juju/loggo"

	"github.com/hemilabs/larry/larry"
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
	_ larry.Batch       = (*pebbleBatch)(nil)
	_ larry.Database    = (*pebbleDB)(nil)
	_ larry.Iterator    = (*pebbleIterator)(nil)
	_ larry.Range       = (*pebbleRange)(nil)
	_ larry.Transaction = (*pebbleTX)(nil)
)

func xerr(err error) error {
	switch {
	case errors.Is(err, pebble.ErrClosed):
		err = larry.ErrDBClosed
	case errors.Is(err, pebble.ErrNotFound):
		err = larry.ErrKeyNotFound
	}
	return err
}

type Config struct {
	Home   string
	Tables []string
}

func DefaultPebbleConfig(home string, tables []string) *Config {
	return &Config{
		Home:   home,
		Tables: tables,
	}
}

type pebbleDB struct {
	db *pebble.DB

	tables map[string]struct{}

	cfg *Config

	// kinda sucks but we must force
	// multiple write txs to block
	txMtx sync.Mutex
}

func NewPebbleDB(cfg *Config) (larry.Database, error) {
	if cfg == nil {
		return nil, larry.ErrInvalidConfig
	}
	bdb := &pebbleDB{
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

func (b *pebbleDB) Open(_ context.Context) error {
	ldb, err := pebble.Open(b.cfg.Home, &pebble.Options{
		Levels: []pebble.LevelOptions{
			{Compression: pebble.NoCompression},
		},
	})
	if err != nil {
		return xerr(err)
	}
	b.db = ldb
	return nil
}

func (b *pebbleDB) Close(_ context.Context) error {
	return xerr(b.db.Close())
}

func (b *pebbleDB) Del(_ context.Context, table string, key []byte) error {
	if _, ok := b.tables[table]; !ok {
		return larry.ErrTableNotFound
	}
	return xerr(b.db.Delete(larry.NewCompositeKey(table, key), nil))
}

func (b *pebbleDB) Has(ctx context.Context, table string, key []byte) (bool, error) {
	if _, ok := b.tables[table]; !ok {
		return false, larry.ErrTableNotFound
	}
	_, err := b.Get(ctx, table, key)
	if errors.Is(err, larry.ErrKeyNotFound) {
		return false, nil
	}
	return err == nil, err
}

func (b *pebbleDB) Get(_ context.Context, table string, key []byte) ([]byte, error) {
	if _, ok := b.tables[table]; !ok {
		return nil, larry.ErrTableNotFound
	}
	value, closer, err := b.db.Get(larry.NewCompositeKey(table, key))
	if err != nil {
		return nil, xerr(err)
	}
	defer func() {
		err = closer.Close()
		if err != nil {
			log.Errorf("close closer: %v", err)
		}
	}()
	v := make([]byte, len(value))
	copy(v, value)
	return v, nil
}

func (b *pebbleDB) Put(_ context.Context, table string, key, value []byte) error {
	if _, ok := b.tables[table]; !ok {
		return larry.ErrTableNotFound
	}
	if key == nil {
		return nil
	}
	return xerr(b.db.Set(larry.NewCompositeKey(table, key), value, nil))
}

func (b *pebbleDB) Begin(_ context.Context, write bool) (larry.Transaction, error) {
	if write {
		b.txMtx.Lock()
	}

	// XXX pebbleDB doesn't have transactions, so we must use batches to
	// emulate the behavior. However, having reads in a pebble batch is MORE
	// expensive than write only, so we should reconsider using
	// NewIndexedBatch (read/write) and use NewBatch (write only).
	tx := b.db.NewIndexedBatch()
	return &pebbleTX{
		db:    b,
		tx:    tx,
		write: write,
	}, nil
}

func (b *pebbleDB) execute(ctx context.Context, write bool, callback func(ctx context.Context, tx larry.Transaction) error) error {
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
	return tx.Commit(ctx)
}

func (b *pebbleDB) View(ctx context.Context, callback func(ctx context.Context, tx larry.Transaction) error) error {
	return b.execute(ctx, false, callback)
}

func (b *pebbleDB) Update(ctx context.Context, callback func(ctx context.Context, tx larry.Transaction) error) error {
	return b.execute(ctx, true, callback)
}

func (b *pebbleDB) NewIterator(ctx context.Context, table string) (larry.Iterator, error) {
	if _, ok := b.tables[table]; !ok {
		return nil, larry.ErrTableNotFound
	}

	start, limit := larry.BytesPrefix(larry.NewCompositeKey(table, nil))
	opt := &pebble.IterOptions{
		LowerBound: start,
		UpperBound: limit,
	}
	iter, err := b.db.NewIterWithContext(ctx, opt)
	if err != nil {
		return nil, xerr(err)
	}

	// set iterator to before first value
	iter.SeekLT(start)

	return &pebbleIterator{
		table: table,
		it:    iter,
	}, nil
}

func (b *pebbleDB) NewRange(ctx context.Context, table string, start, end []byte) (larry.Range, error) {
	if _, ok := b.tables[table]; !ok {
		return nil, larry.ErrTableNotFound
	}

	_, endKey := larry.BytesPrefix(larry.NewCompositeKey(table, nil))
	if end != nil {
		endKey = larry.NewCompositeKey(table, end)
	}
	opt := &pebble.IterOptions{
		LowerBound: larry.NewCompositeKey(table, start),
		UpperBound: endKey,
	}
	iter, err := b.db.NewIterWithContext(ctx, opt)
	if err != nil {
		return nil, xerr(err)
	}

	// set iterator to before first value
	iter.SeekLT(nil)

	return &pebbleRange{
		table: table,
		it:    iter,
		start: start,
		end:   end,
	}, nil
}

func (b *pebbleDB) NewBatch(_ context.Context) (larry.Batch, error) {
	return &pebbleBatch{db: b, wb: b.db.NewBatch()}, nil
}

// Transactions

// PebbleDB doesn't have transactions, so emulate behavior
// Using batches.
type pebbleTX struct {
	db    *pebbleDB
	tx    *pebble.Batch
	write bool
}

func (tx *pebbleTX) Del(_ context.Context, table string, key []byte) error {
	if _, ok := tx.db.tables[table]; !ok {
		return larry.ErrTableNotFound
	}
	return xerr(tx.tx.Delete(larry.NewCompositeKey(table, key), nil))
}

func (tx *pebbleTX) Has(ctx context.Context, table string, key []byte) (bool, error) {
	if _, ok := tx.db.tables[table]; !ok {
		return false, larry.ErrTableNotFound
	}
	_, err := tx.Get(ctx, table, key)
	if errors.Is(err, larry.ErrKeyNotFound) {
		return false, nil
	}
	return err == nil, xerr(err)
}

func (tx *pebbleTX) Get(_ context.Context, table string, key []byte) ([]byte, error) {
	if _, ok := tx.db.tables[table]; !ok {
		return nil, larry.ErrTableNotFound
	}
	val, closer, err := tx.tx.Get(larry.NewCompositeKey(table, key))
	if err != nil {
		return nil, xerr(err)
	}
	defer func() {
		err = closer.Close() // XXX check for nil?
		if err != nil {
			log.Errorf("close closer: %v", err)
		}
	}()
	// pebbleDB unfortunately invalidates value outside of the batch
	value := make([]byte, len(val))
	copy(value, val)
	return value, nil
}

func (tx *pebbleTX) Put(_ context.Context, table string, key []byte, value []byte) error {
	if _, ok := tx.db.tables[table]; !ok {
		return larry.ErrTableNotFound
	}
	if key == nil {
		return nil
	}
	return xerr(tx.tx.Set(larry.NewCompositeKey(table, key), value, nil))
}

func (tx *pebbleTX) Commit(_ context.Context) error {
	if tx.write {
		defer tx.db.txMtx.Unlock()
	}
	return xerr(tx.tx.Commit(nil))
}

func (tx *pebbleTX) Rollback(_ context.Context) error {
	if tx.write {
		defer tx.db.txMtx.Unlock()
	}
	return xerr(tx.tx.Close())
}

func (tx *pebbleTX) Write(_ context.Context, b larry.Batch) error {
	return xerr(tx.tx.Apply(b.(*pebbleBatch).wb, nil))
}

// Iterations
type pebbleIterator struct {
	table string
	it    *pebble.Iterator
}

func (ni *pebbleIterator) First(_ context.Context) bool {
	return ni.it.First()
}

func (ni *pebbleIterator) Last(_ context.Context) bool {
	return ni.it.Last()
}

func (ni *pebbleIterator) Next(_ context.Context) bool {
	return ni.it.Next()
}

func (ni *pebbleIterator) Seek(_ context.Context, key []byte) bool {
	return ni.it.SeekGE(larry.NewCompositeKey(ni.table, key))
}

func (ni *pebbleIterator) Key(_ context.Context) []byte {
	return larry.KeyFromComposite(ni.table, ni.it.Key())
}

func (ni *pebbleIterator) Value(_ context.Context) []byte {
	return ni.it.Value()
}

func (ni *pebbleIterator) Close(_ context.Context) {
	if err := ni.it.Close(); err != nil {
		log.Errorf("iterator close: %v", err)
	}
}

// Ranges
type pebbleRange struct {
	table string
	it    *pebble.Iterator
	start []byte
	end   []byte
}

func (nr *pebbleRange) First(_ context.Context) bool {
	return nr.it.First()
}

func (nr *pebbleRange) Last(_ context.Context) bool {
	return nr.it.Last()
}

func (nr *pebbleRange) Next(_ context.Context) bool {
	return nr.it.Next()
}

func (nr *pebbleRange) Key(_ context.Context) []byte {
	return larry.KeyFromComposite(nr.table, nr.it.Key())
}

func (nr *pebbleRange) Value(_ context.Context) []byte {
	return nr.it.Value()
}

func (nr *pebbleRange) Close(_ context.Context) {
	if err := nr.it.Close(); err != nil {
		log.Errorf("range close: %v", err)
	}
}

// Batches

type pebbleBatch struct {
	db *pebbleDB
	wb *pebble.Batch
}

func (nb *pebbleBatch) Del(_ context.Context, table string, key []byte) {
	if _, ok := nb.db.tables[table]; !ok {
		log.Errorf("%s: %v", table, larry.ErrTableNotFound)
		return
	}
	if err := nb.wb.Delete(larry.NewCompositeKey(table, key), nil); err != nil {
		log.Errorf("delete %v: %v", table, key)
	}
}

func (nb *pebbleBatch) Put(_ context.Context, table string, key, value []byte) {
	if _, ok := nb.db.tables[table]; !ok {
		log.Errorf("%s: %v", table, larry.ErrTableNotFound)
		return
	}
	if key == nil {
		return
	}
	if err := nb.wb.Set(larry.NewCompositeKey(table, key), value, nil); err != nil {
		log.Errorf("set %v: %v %v", table, key, value)
	}
}

func (nb *pebbleBatch) Reset(_ context.Context) {
	nb.wb.Reset()
}
