// Copyright (c) 2025 Hemi Labs, Inc.
// Use of this source code is governed by the MIT License,
// which can be found in the LICENSE file.

package larry

import (
	"context"
	"errors"
)

// Assert required interfaces
var (
	_ Batch       = (*dummyBatch)(nil)
	_ Database    = (*dummyDB)(nil)
	_ Iterator    = (*dummyIterator)(nil)
	_ Range       = (*dummyRange)(nil)
	_ Transaction = (*dummyTX)(nil)

	ErrDummy = errors.New("dummy")
)

type DummyConfig struct {
	Home   string
	Tables []string
}

func DefaultDummyConfig(home string, tables []string) *DummyConfig {
	return &DummyConfig{
		Home:   home,
		Tables: tables,
	}
}

func NewDummyDB(cfg *DummyConfig) (Database, error) {
	if cfg == nil {
		return nil, ErrInvalidConfig
	}
	ddb := &dummyDB{
		cfg:    cfg,
		tables: make(map[string]struct{}, len(cfg.Tables)),
	}
	for _, v := range cfg.Tables {
		if _, ok := ddb.tables[v]; ok {
			return nil, ErrDuplicateTable
		}
		ddb.tables[v] = struct{}{}
	}

	return ddb, nil
}

// Database
type dummyDB struct {
	cfg    *DummyConfig
	tables map[string]struct{}
}

func (db *dummyDB) Open(context.Context) error {
	return ErrDummy
}

func (db *dummyDB) Close(context.Context) error {
	return ErrDummy
}

func (db *dummyDB) Del(_ context.Context, _ string, _ []byte) error {
	return ErrDummy
}

func (db *dummyDB) Has(_ context.Context, _ string, _ []byte) (bool, error) {
	return false, ErrDummy
}

func (db *dummyDB) Get(_ context.Context, _ string, _ []byte) ([]byte, error) {
	return nil, ErrDummy
}

func (db *dummyDB) Put(_ context.Context, _ string, _ []byte, _ []byte) error {
	return ErrDummy
}

func (db *dummyDB) Begin(_ context.Context, _ bool) (Transaction, error) {
	return &dummyTX{}, nil
}

func (db *dummyDB) Update(_ context.Context, _ func(_ context.Context, _ Transaction) error) error {
	return ErrDummy
}

func (db *dummyDB) View(_ context.Context, _ func(_ context.Context, _ Transaction) error) error {
	return ErrDummy
}

func (db *dummyDB) NewIterator(_ context.Context, _ string) (Iterator, error) {
	return &dummyIterator{}, nil
}

func (db *dummyDB) NewRange(_ context.Context, _ string, _, _ []byte) (Range, error) {
	return &dummyRange{}, nil
}

func (db *dummyDB) NewBatch(_ context.Context) (Batch, error) {
	return &dummyBatch{}, nil
}

// Batch
type dummyBatch struct{}

func (*dummyBatch) Del(_ context.Context, _ string, _ []byte)    {}
func (*dummyBatch) Put(_ context.Context, _ string, _, _ []byte) {}
func (*dummyBatch) Reset(_ context.Context)                      {}

// Iterator
type dummyIterator struct{}

func (it *dummyIterator) First(_ context.Context) bool {
	return false
}

func (it *dummyIterator) Last(_ context.Context) bool {
	return false
}

func (it *dummyIterator) Next(_ context.Context) bool {
	return false
}

func (it *dummyIterator) Seek(_ context.Context, _ []byte) bool {
	return false
}

func (it *dummyIterator) Key(_ context.Context) []byte {
	return nil
}

func (it *dummyIterator) Value(_ context.Context) []byte {
	return nil
}

func (it *dummyIterator) Close(_ context.Context) {}

// Range
type dummyRange struct{}

func (r *dummyRange) First(_ context.Context) bool {
	return false
}

func (r *dummyRange) Last(_ context.Context) bool {
	return false
}

func (r *dummyRange) Next(_ context.Context) bool {
	return false
}

func (r *dummyRange) Key(_ context.Context) []byte {
	return nil
}

func (r *dummyRange) Value(_ context.Context) []byte {
	return nil
}

func (r *dummyRange) Close(_ context.Context) {}

// Transaction
type dummyTX struct{}

func (tx *dummyTX) Del(_ context.Context, _ string, _ []byte) error {
	return ErrDummy
}

func (tx *dummyTX) Has(_ context.Context, _ string, _ []byte) (bool, error) {
	return false, ErrDummy
}

func (tx *dummyTX) Get(_ context.Context, _ string, _ []byte) ([]byte, error) {
	return nil, ErrDummy
}

func (tx *dummyTX) Put(_ context.Context, _ string, _ []byte, _ []byte) error {
	return ErrDummy
}

func (tx *dummyTX) Commit(_ context.Context) error {
	return ErrDummy
}

func (tx *dummyTX) Rollback(_ context.Context) error {
	return ErrDummy
}

func (tx *dummyTX) Write(_ context.Context, _ Batch) error {
	return ErrDummy
}
