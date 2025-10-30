// Copyright (c) 2025 Hemi Labs, Inc.
// Use of this source code is governed by the MIT License,
// which can be found in the LICENSE file.

package testutil

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"math/rand/v2"
	"slices"
	"testing"

	"golang.org/x/sync/errgroup"

	"github.com/hemilabs/larry/larry"
)

type NewDBFunc func(home string, tables []string) larry.Database

// RunLarryTests runs the suite of tests to assert correct functionality
// of a larry db implementation. The distributed flag signals that tests
// can't be run in parallel, or should perform additional steps during
// test execution.
func RunLarryTests(t *testing.T, dbFunc NewDBFunc, distributed bool) {
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	const insertCount = 10
	t.Run("basic", func(t *testing.T) {
		if !distributed {
			t.Parallel()
		}
		db, tables := prepareTestSuite(ctx, t, 5, 0, dbFunc)
		defer func() {
			err := db.Close(ctx)
			if err != nil {
				t.Fatal(err)
			}
		}()

		if err := dbBasic(ctx, db, tables, insertCount); err != nil {
			t.Fatal(err)
		}
	})

	t.Run("transactions", func(t *testing.T) {
		if !distributed {
			t.Parallel()
		}
		db, tables := prepareTestSuite(ctx, t, 5, 0, dbFunc)
		defer func() {
			err := db.Close(ctx)
			if err != nil {
				t.Fatal(err)
			}
		}()

		if err := dbTransactionsRollback(ctx, db, tables, insertCount); err != nil {
			t.Errorf("dbTransactionsRollback: %v", err)
		}
		if err := dbTransactionsCommit(ctx, db, tables, insertCount); err != nil {
			t.Errorf("dbTransactionsCommit: %v", err)
		}
		if err := dbTransactionsDelete(ctx, db, tables, insertCount); err != nil {
			t.Errorf("dbTransactionsDelete: %v", err)
		}
		if err := dbTransactionsErrors(ctx, db, tables); err != nil {
			t.Errorf("dbTransactionsErrors: %v", err)
		}
		if err := dbTransactionsMultipleWrite(ctx, db, tables[0], 5); err != nil {
			t.Errorf("dbTransactionsMultipleWrite: %v", err)
		}
	})

	t.Run("iterator", func(t *testing.T) {
		if !distributed {
			t.Parallel()
		}
		db, tables := prepareTestSuite(ctx, t, 3, insertCount, dbFunc)
		defer func() {
			err := db.Close(ctx)
			if err != nil {
				t.Fatal(err)
			}
		}()

		for _, table := range tables {
			if err := dbIterateNext(ctx, db, table, insertCount); err != nil {
				t.Errorf("dbIterateNext: %v", err)
			}
			if err := dbIterateFirstLast(ctx, db, table, insertCount); err != nil {
				t.Errorf("dbIterateFirstLast: %v", err)
			}
			if err := dbIterateSeek(ctx, db, table, insertCount); err != nil {
				t.Errorf("dbIterateSeek: %v", err)
			}
			if err := dbIterateSeek(ctx, db, table, insertCount); err != nil {
				t.Errorf("dbIterateSeek: %v", err)
			}
			// if err := dbIterateConcurrentWrites(ctx, db, table, insertCount); err != nil {
			// 	t.Errorf("dbIterateConcurrentWrites: %v", err)
			// }
		}
	})

	t.Run("range", func(t *testing.T) {
		if !distributed {
			t.Parallel()
		}
		db, tables := prepareTestSuite(ctx, t, 3, insertCount, dbFunc)
		defer func() {
			err := db.Close(ctx)
			if err != nil {
				t.Fatal(err)
			}
		}()

		if err := dbRange(ctx, db, tables, insertCount); err != nil {
			t.Errorf("dbRange: %v", err)
		}
		if err := dbRangeEmpty(ctx, db, tables, insertCount); err != nil {
			t.Errorf("dbRangeEmpty: %v", err)
		}
		if err := dbRangeFirstLast(ctx, db, tables, insertCount); err != nil {
			t.Fatal(err)
		}
	})

	t.Run("batch", func(t *testing.T) {
		if !distributed {
			t.Parallel()
		}
		db, tables := prepareTestSuite(ctx, t, 1, 0, dbFunc)
		defer func() {
			err := db.Close(ctx)
			if err != nil {
				t.Fatal(err)
			}
		}()

		if err := dbBatch(ctx, db, tables[0], 10); err != nil {
			t.Fatal(err)
		}
		if err := dbBatchNoop(ctx, db, tables[0]); err != nil {
			t.Fatal(err)
		}
	})

	t.Run("reopen", func(t *testing.T) {
		if !distributed {
			t.Parallel()
		}
		db, tables := prepareTestSuite(ctx, t, 1, 0, dbFunc)
		defer func() {
			err := db.Close(ctx)
			if err != nil {
				t.Fatal(err)
			}
		}()

		if distributed {
			err := db.Close(ctx)
			if err != nil {
				t.Fatal(err)
			}

			db = reopen(dbFunc, tables)
			err = db.Open(ctx)
			if err != nil {
				t.Fatal(err)
			}
		}

		if err := dbOpenCloseOpen(ctx, db, tables[0]); err != nil {
			t.Fatal(err)
		}
	})
}

// RunLarryBatchBenchmarks runs the suite of benchmarks for a
// larry db implementation.
func RunLarryBatchBenchmarks(b *testing.B, dbFunc NewDBFunc) {
	ctx, cancel := context.WithCancel(b.Context())
	defer cancel()

	benchmarkLarryBatch(ctx, b, dbFunc)
	benchmarkLarryGet(ctx, b, dbFunc)
	benchmarkLarryIter(ctx, b, dbFunc)
}

// TODO: populate this when distributed dbs are added
func reopen(_ NewDBFunc, _ []string) larry.Database {
	return nil
}

func prepareTestSuite(ctx context.Context, t *testing.T, tableCount, insert int, dbFunc NewDBFunc) (larry.Database, []string) {
	home := t.TempDir()

	tables := make([]string, 0, tableCount)
	for i := range tableCount {
		tables = append(tables, fmt.Sprintf("table%v", i))
	}

	db := dbFunc(home, tables)
	err := db.Open(ctx)
	if err != nil {
		t.Fatal(err)
	}

	for _, table := range tables {
		for i := range insert {
			err := db.Put(ctx, table, newKey(i), newVal(i))
			if err != nil {
				t.Fatal(fmt.Errorf("put [%v,%v]: %w", i, i, err))
			}
		}
	}

	return db, tables
}

func newKey(i int) []byte {
	if i < 0 {
		panic("invalid key: value < 0")
	}
	if i > math.MaxUint32 {
		panic("invalid key: value > maxuint32")
	}
	var key [4]byte
	binary.BigEndian.PutUint32(key[:], uint32(i))
	return key[:]
}

func newVal(i int) []byte {
	if i < 0 {
		panic("invalid key: value < 0")
	}
	var value [8]byte
	binary.BigEndian.PutUint64(value[:], uint64(i))
	return value[:]
}

// dbputs splits N inserts into multiple tables.
func dbputs(ctx context.Context, db larry.Database, tables []string, insertCount int) error {
	for i := range insertCount {
		table := tables[i%len(tables)]
		err := db.Put(ctx, table, newKey(i), newVal(i))
		if err != nil {
			return fmt.Errorf("put %v in %v: %w", i, table, err)
		}
	}
	return nil
}

// dbputEmpty inserts empty records and expects a no-op.
func dbputEmpty(ctx context.Context, db larry.Database, tables []string) error {
	for _, table := range tables {
		err := db.Put(ctx, table, nil, nil)
		if err != nil {
			return fmt.Errorf("put empty key %v: %w", table, err)
		}

		has, err := db.Has(ctx, table, nil)
		if err != nil {
			return fmt.Errorf("has: %w", err)
		}
		if has {
			return errors.New("expected no empty record")
		}
	}
	return nil
}

// dbputInvalidTable inserts into an invalid table and expects an error.
func dbputInvalidTable(ctx context.Context, db larry.Database, table string) error {
	err := db.Put(ctx, table, newKey(0), nil)
	if !errors.Is(err, larry.ErrTableNotFound) {
		return fmt.Errorf("put expected not found error %v: %w", table, err)
	}
	return nil
}

// dbputDuplicate inserts / updates the value of a key multiple times.
// After each put, we assert the value matches the last insert.
func dbputDuplicate(ctx context.Context, db larry.Database, table string, insertCount int) error {
	for i := range insertCount {
		key := newKey(0)
		value := newVal(i)
		err := db.Put(ctx, table, key, value)
		if err != nil {
			return fmt.Errorf("put %v: %v", table, i)
		}
		rv, err := db.Get(ctx, table, key)
		if err != nil {
			return fmt.Errorf("get %v: %v %w", table, i, err)
		}
		if !bytes.Equal(rv, value) {
			return fmt.Errorf("get unequal %v %v: got %v, expected %v",
				table, i, rv, value)
		}
	}
	return nil
}

// dbgets gets records split across tables.
// The retrieved value is asserted.
func dbgets(ctx context.Context, db larry.Database, tables []string, insertCount int) error {
	for i := range insertCount {
		table := tables[i%len(tables)]
		value, err := db.Get(ctx, table, newKey(i))
		if err != nil {
			return fmt.Errorf("get %v: %v %w", table, i, err)
		}
		if !bytes.Equal(value, newVal(i)) {
			return fmt.Errorf("get unequal %v: %v", table, i)
		}
	}
	return nil
}

// dbgetInvalidTable gets from an invalid table and expects an error.
func dbgetInvalidTable(ctx context.Context, db larry.Database, table string) error {
	_, err := db.Get(ctx, table, newKey(0))
	if !errors.Is(err, larry.ErrTableNotFound) {
		return fmt.Errorf("get expected not found error %v: %w", table, err)
	}
	return nil
}

// dbhas asserts records split across tables exist.
func dbhas(ctx context.Context, db larry.Database, tables []string, insertCount int) error {
	for i := range insertCount {
		table := tables[i%len(tables)]
		has, err := db.Has(ctx, table, newKey(i))
		if err != nil {
			return fmt.Errorf("has %v: %v %w", table, i, err)
		}
		if !has {
			return fmt.Errorf("has %v: %v", table, i)
		}
	}
	return nil
}

// dbhasInvalidTable calls has for an invalid table and expects an error.
func dbhasInvalidTable(ctx context.Context, db larry.Database, table string) error {
	has, err := db.Has(ctx, table, newKey(0))
	if !errors.Is(err, larry.ErrTableNotFound) {
		return fmt.Errorf("has expected not found error %v: %w", table, err)
	}
	if has {
		return fmt.Errorf("expected not has %v: %v", table, 0)
	}
	return nil
}

// dbdels deletes records split across tables.
func dbdels(ctx context.Context, db larry.Database, tables []string, insertCount int) error {
	for i := range insertCount {
		table := tables[i%len(tables)]
		err := db.Del(ctx, table, newKey(i))
		if err != nil {
			return fmt.Errorf("del %v: %v %w", table, i, err)
		}
	}
	return nil
}

// dbdelInvalidKey deletes an invalid record and expects a no-op.
func dbdelInvalidKey(ctx context.Context, db larry.Database, tables []string, insertCount int) error {
	for i := range insertCount {
		table := tables[i%len(tables)]
		err := db.Del(ctx, table, newKey(i))
		if err != nil {
			return fmt.Errorf("del invalid key %v: %v %w", table, i, err)
		}
	}
	return nil
}

// dbdelInvalidTable deletes from an invalid table and expects an error.
func dbdelInvalidTable(ctx context.Context, db larry.Database, table string) error {
	err := db.Del(ctx, table, newKey(0))
	if !errors.Is(err, larry.ErrTableNotFound) {
		return fmt.Errorf("del expected not found error %v: %w", table, err)
	}
	return nil
}

// dbhasNegative asserts records split across tables don't exist.
func dbhasNegative(ctx context.Context, db larry.Database, tables []string, insertCount int) error {
	for i := range insertCount {
		table := tables[i%len(tables)]
		has, err := db.Has(ctx, table, newKey(i))
		if err != nil {
			return fmt.Errorf("has %v: %v %w", table, i, err)
		}
		if has {
			return fmt.Errorf("has %v: %v", table, i)
		}
	}
	return nil
}

// dbgetsNegative gets records split across tables.
// It expects the operation to fail with an error.
func dbgetsNegative(ctx context.Context, db larry.Database, tables []string, insertCount int) error {
	for i := range insertCount {
		table := tables[i%len(tables)]
		_, err := db.Get(ctx, table, newKey(i))
		if !errors.Is(err, larry.ErrKeyNotFound) {
			return fmt.Errorf("get expected not found error %v: %v %w", table, i, err)
		}
	}
	return nil
}

// dbhasOdds asserts odd records split across tables exist.
func dbhasOdds(ctx context.Context, db larry.Database, tables []string, insertCount int) error {
	for i := range insertCount {
		table := tables[i%len(tables)]
		has, err := db.Has(ctx, table, newKey(i))
		if i%2 == 0 {
			// Assert we don't have evens
			if err != nil {
				return fmt.Errorf("odds has %v: %v %w", table, i, err)
			}
			if has {
				return fmt.Errorf("odds has %v: %v", table, i)
			}
		} else {
			if err != nil {
				return fmt.Errorf("odds has %v: %v %w", table, i, err)
			}
			if !has {
				return fmt.Errorf("odds has %v: %v", table, i)
			}
		}
	}
	return nil
}

// txputEmpty inserts empty records and expects a no-op.
func txputEmpty(ctx context.Context, tx larry.Transaction, tables []string) error {
	for _, table := range tables {
		err := tx.Put(ctx, table, nil, nil)
		if err != nil {
			return fmt.Errorf("tx put empty %v: %w", table, err)
		}
	}
	return nil
}

// dbputEmpty inserts into an invalid table and expects an error.
func txputInvalidTable(ctx context.Context, tx larry.Transaction, table string) error {
	err := tx.Put(ctx, table, newKey(0), nil)
	if !errors.Is(err, larry.ErrTableNotFound) {
		return fmt.Errorf("tx put expected not found error %v: %w", table, err)
	}
	return nil
}

// This fails if we try to access the changes made using a tx put
// with a tx get after, and it returns the new value
// func txputDuplicate(ctx context.Context, tx larry.Transaction, table string, insertCount int) error {
// 	for i := range insertCount {
// 		var key [4]byte
// 		var value [8]byte
// 		binary.BigEndian.PutUint32(key[:], uint32(0))
// 		binary.BigEndian.PutUint64(value[:], uint64(i))
// 		err := tx.Put(ctx, table, key[:], value[:])
// 		if err != nil {
// 			return fmt.Errorf("put %v: %v", table, i)
// 		}
// 		rv, err := tx.Get(ctx, table, key[:])
// 		if err != nil {
// 			return fmt.Errorf("get %v: %v %w", table, i, err)
// 		}
// 		if i != 0 && bytes.Equal(rv, value[:]) {
// 			return fmt.Errorf("get equal %v: expect %d, got %d", table, value, rv)
// 		}
// 	}
// 	return nil
// }

// dbputs splits N inserts into multiple tables.
func txputs(ctx context.Context, tx larry.Transaction, tables []string, insertCount int) error {
	for i := range insertCount {
		table := tables[i%len(tables)]
		err := tx.Put(ctx, table, newKey(i), newVal(i))
		if err != nil {
			return fmt.Errorf("tx put %v in %v: %w", i, table, err)
		}
	}
	return nil
}

// dbdels deletes even records split across tables.
func txdelsEven(ctx context.Context, tx larry.Transaction, tables []string, insertCount int) error {
	for i := range insertCount {
		table := tables[i%len(tables)]
		key := newKey(i)
		if i%2 == 0 {
			err := tx.Del(ctx, table, key)
			if err != nil {
				return fmt.Errorf("del %v: %v %w", table, i, err)
			}
		} else {
			// Assert odd record exist
			value, err := tx.Get(ctx, table, key)
			if err != nil {
				return fmt.Errorf("even get %v: %v %w", table, i, err)
			}
			if !bytes.Equal(value, newVal(i)) {
				return fmt.Errorf("even get unequal %v: %v", table, i)
			}
		}
	}
	return nil
}

// dbdelInvalidKey deletes an invalid record and expects a no-op.
func txdelInvalidKey(ctx context.Context, tx larry.Transaction, table string) error {
	err := tx.Del(ctx, table, newKey(0))
	if err != nil {
		return fmt.Errorf("del invalid key %v: %w", table, err)
	}
	return nil
}

// dbBasic tests all basic operations of a db.
func dbBasic(ctx context.Context, db larry.Database, tables []string, insertCount int) error {
	// Already Open
	if err := db.Open(ctx); err == nil {
		return errors.New("expected already open error")
	}

	// Put Empty
	err := dbputEmpty(ctx, db, tables)
	if err != nil {
		return fmt.Errorf("dbputEmpty: %w", err)
	}

	// Put Invalid Table
	err = dbputInvalidTable(ctx, db, fmt.Sprintf("table%d", len(tables)))
	if err != nil {
		return fmt.Errorf("dbputInvalidTable: %w", err)
	}

	// Put Duplicate
	err = dbputDuplicate(ctx, db, tables[0], insertCount)
	if err != nil {
		return fmt.Errorf("dbputDuplicate: %w", err)
	}

	// Puts
	err = dbputs(ctx, db, tables, insertCount)
	if err != nil {
		return fmt.Errorf("dbputs: %w", err)
	}

	// Get Invalid Table
	err = dbgetInvalidTable(ctx, db, fmt.Sprintf("table%d", len(tables)))
	if err != nil {
		return fmt.Errorf("dbgetInvalidTable: %w", err)
	}

	// Get
	err = dbgets(ctx, db, tables, insertCount)
	if err != nil {
		return fmt.Errorf("dbgets: %w", err)
	}

	// Has Invalid Table
	err = dbhasInvalidTable(ctx, db, fmt.Sprintf("table%d", len(tables)))
	if err != nil {
		return fmt.Errorf("dbgetInvalidTable: %w", err)
	}

	// Has
	err = dbhas(ctx, db, tables, insertCount)
	if err != nil {
		return fmt.Errorf("dbhas: %w", err)
	}

	// Del
	err = dbdels(ctx, db, tables, insertCount)
	if err != nil {
		return fmt.Errorf("dbdels: %w", err)
	}

	// Del Invalid Table
	err = dbdelInvalidTable(ctx, db, fmt.Sprintf("table%d", len(tables)))
	if err != nil {
		return fmt.Errorf("dbdelInvalidTable: %w", err)
	}

	// Del Invalid Key
	err = dbdelInvalidKey(ctx, db, tables, insertCount)
	if err != nil {
		return fmt.Errorf("dbdelInvalidKey: %w", err)
	}

	// Has negative
	err = dbhasNegative(ctx, db, tables, insertCount)
	if err != nil {
		return fmt.Errorf("dbhasNegative: %w", err)
	}

	// Get negative
	err = dbgetsNegative(ctx, db, tables, insertCount)
	if err != nil {
		return fmt.Errorf("dbgetsNegative: %w", err)
	}

	return nil
}

// dbTransactionsRollback inserts N records into a Tx, rolls it back, and
// expects the inserted records to not be present in the db.
func dbTransactionsRollback(ctx context.Context, db larry.Database, tables []string, insertCount int) error {
	tx, err := db.Begin(ctx, true)
	if err != nil && !errors.Is(err, larry.ErrDBClosed) {
		return fmt.Errorf("db begin: %w", err)
	}
	defer func() {
		if err != nil {
			_ = tx.Rollback(ctx)
		}
	}()
	err = txputs(ctx, tx, tables, insertCount)
	if err != nil {
		return fmt.Errorf("txputs: %w", err)
	}
	err = tx.Rollback(ctx)
	if err != nil {
		return fmt.Errorf("tx rollback: %w", err)
	}
	err = dbhasNegative(ctx, db, tables, insertCount)
	if err != nil {
		return fmt.Errorf("dbhasNegative: %w", err)
	}
	return nil
}

// dbTransactionsCommit inserts N records into a Tx, commits it, and
// expects the inserted records to be present in the db.
func dbTransactionsCommit(ctx context.Context, db larry.Database, tables []string, insertCount int) error {
	tx, err := db.Begin(ctx, true)
	if err != nil {
		return fmt.Errorf("db begin: %w", err)
	}
	defer func() {
		if err != nil {
			_ = tx.Rollback(ctx)
		}
	}()
	err = txputs(ctx, tx, tables, insertCount)
	if err != nil {
		return fmt.Errorf("txputs: %w", err)
	}
	err = tx.Commit(ctx)
	if err != nil {
		return fmt.Errorf("tx commit: %w", err)
	}
	err = dbgets(ctx, db, tables, insertCount)
	if err != nil {
		return fmt.Errorf("dbgets: %w", err)
	}
	return nil
}

// dbTransactionsMultipleWrite creates multiple write TXs concurrently
// that update the same value. It asserts that each new write TX created
// blocks until the previous one is committed.
func dbTransactionsMultipleWrite(ctx context.Context, db larry.Database, table string, txCount int) error {
	last := txCount + 1
	key := newKey(0)
	err := db.Put(ctx, table, key, newVal(last))
	if err != nil {
		return errors.New("initial put")
	}
	eg, ctx := errgroup.WithContext(ctx)
	for i := range txCount {
		eg.Go(func() error {
			tx, err := db.Begin(ctx, true)
			if err != nil {
				return fmt.Errorf("tx%d - db begin: %w", i, err)
			}
			defer func() {
				if err != nil {
					_ = tx.Rollback(ctx)
				}
			}()

			rv, err := tx.Get(ctx, table, key)
			if err != nil {
				return fmt.Errorf("tx%d - get: %w", i, err)
			}
			// see if value set by last tx matches "last"
			ve := newVal(last)
			if !bytes.Equal(rv, ve) {
				return fmt.Errorf("tx%d - expected %v, got %v", i, ve, rv)
			}

			// set value and "last" to "i"
			ve = newVal(i)
			err = tx.Put(ctx, table, key, ve)
			if err != nil {
				return fmt.Errorf("tx put %v in %v: %w", i, table, err)
			}
			// this should work and doesn't race because write txs
			// are meant to block on creation if one already exists
			last = i
			err = tx.Commit(ctx)
			if err != nil {
				return fmt.Errorf("tx%d - tx commit: %w", i, err)
			}
			return nil
		})
	}
	if err := eg.Wait(); err != nil {
		return err
	}
	return nil
}

// dbTransactionsDelete deletes even records into a Tx, commits it, and
// expects the deleted records to not be present in the db.
func dbTransactionsDelete(ctx context.Context, db larry.Database, tables []string, insertCount int) error {
	tx, err := db.Begin(ctx, true)
	if err != nil {
		return fmt.Errorf("begin: %w", err)
	}
	defer func() {
		if err != nil {
			_ = tx.Rollback(ctx)
		}
	}()
	err = txdelsEven(ctx, tx, tables, insertCount)
	if err != nil {
		return fmt.Errorf("txdelsEven: %w", err)
	}
	err = tx.Commit(ctx)
	if err != nil {
		return fmt.Errorf("tx commit: %w", err)
	}
	err = dbhasOdds(ctx, db, tables, insertCount)
	if err != nil {
		return fmt.Errorf("dbhasOdds: %w", err)
	}

	return nil
}

// dbTransactionsErrors checks edge case TX errors.
func dbTransactionsErrors(ctx context.Context, db larry.Database, tables []string) error {
	tx, err := db.Begin(ctx, true)
	if err != nil {
		return fmt.Errorf("begin: %w", err)
	}
	defer func() {
		if err != nil {
			_ = tx.Rollback(ctx)
		}
	}()
	err = txputEmpty(ctx, tx, tables)
	if err != nil {
		return fmt.Errorf("txputEmpty: %w", err)
	}
	err = txputInvalidTable(ctx, tx, fmt.Sprintf("table%d", len(tables)))
	if err != nil {
		return fmt.Errorf("txputInvalidTable: %w", err)
	}
	err = txdelInvalidKey(ctx, tx, tables[0])
	if err != nil {
		return fmt.Errorf("txdelInvalidKey: %w", err)
	}
	// err = txputDuplicate(ctx, tx, tables[0], insertCount)
	// if err != nil {
	// 	return fmt.Errorf("txputDuplicate: %w", err)
	// }
	err = tx.Rollback(ctx)
	if err != nil {
		return fmt.Errorf("tx rollback: %w", err)
	}

	return nil
}

// dbIterateNext iterates through a table and asserts the number
// of found records matches the number of inserts.
func dbIterateNext(ctx context.Context, db larry.Database, table string, recordCount int) error {
	it, err := db.NewIterator(ctx, table)
	if err != nil {
		return err
	}
	defer it.Close(ctx)

	// Next
	i := 0
	for it.Next(ctx) {
		key := it.Key(ctx)
		val := it.Value(ctx)
		if !bytes.Equal(key, newKey(i)) {
			return fmt.Errorf("next unequal key: got %v, expected %v", key, newKey(i))
		}
		if !bytes.Equal(val, newVal(i)) {
			return fmt.Errorf("next unequal value: got %v, expected %v", val, newVal(i))
		}
		i++
	}
	if recordCount != i {
		return fmt.Errorf("found %d records, expected %d", i, recordCount)
	}
	return nil
}

// dbIterateFirstLast asserts the first and last record of
// the iterator matches the expected value.
func dbIterateFirstLast(ctx context.Context, db larry.Database, table string, recordCount int) error {
	it, err := db.NewIterator(ctx, table)
	if err != nil {
		return err
	}
	defer it.Close(ctx)

	// First
	if !it.First(ctx) {
		return errors.New("first")
	}
	key := it.Key(ctx)
	val := it.Value(ctx)
	if !bytes.Equal(key, newKey(0)) {
		return fmt.Errorf("first unequal key: got %v, expected %v", key, newKey(0))
	}
	if !bytes.Equal(val, newVal(0)) {
		return fmt.Errorf("first unequal value: got %v, expected %v", val, newVal(0))
	}

	// Last
	if !it.Last(ctx) {
		return errors.New("last")
	}
	key = it.Key(ctx)
	val = it.Value(ctx)
	if !bytes.Equal(key, newKey(recordCount-1)) {
		return fmt.Errorf("last unequal key: got %v, expected %v", key, newKey(recordCount-1))
	}
	if !bytes.Equal(val, newVal(recordCount-1)) {
		return fmt.Errorf("last unequal value: got %v, expected %v", val, newVal(recordCount-1))
	}

	return nil
}

// dbIterateSeek seeks every record using an iterator and verifies that
// Next returns the next record after a seek.
func dbIterateSeek(ctx context.Context, db larry.Database, table string, recordCount int) error {
	it, err := db.NewIterator(ctx, table)
	if err != nil {
		return err
	}
	defer it.Close(ctx)

	// Seek even
	for i := range recordCount {
		if i%2 == 0 {
			expectedKey := newKey(i)
			if !it.Seek(ctx, expectedKey) {
				return fmt.Errorf("seek %v", expectedKey)
			}
			key := it.Key(ctx)
			val := it.Value(ctx)
			if !bytes.Equal(key, expectedKey) {
				return fmt.Errorf("seek unequal key: got %v, expected %v",
					key, expectedKey)
			}
			if !bytes.Equal(val, newVal(i)) {
				return fmt.Errorf("seek unequal value: got %v, expected %v",
					val, newVal(i))
			}
		}
	}

	// Verify that Next returns the Next record after a seek.
	if !it.Seek(ctx, newKey(1)) {
		return errors.New("seek 1")
	}
	if !it.Next(ctx) {
		return errors.New("next")
	}
	if !bytes.Equal(it.Key(ctx), newKey(2)) {
		return fmt.Errorf("not equal seek, got %v wanted %v", it.Key(ctx), newKey(2))
	}

	return nil
}

// func dbIterateConcurrentWrites(ctx context.Context, db larry.Database, table string, recordCount int) error {
// 	it, err := db.NewIterator(ctx, table)
// 	if err != nil {
// 		return err
// 	}
// 	defer it.Close(ctx)

// 	err = db.Put(ctx, table, []byte{uint8(recordCount)}, []byte{uint8(recordCount)})
// 	if err != nil {
// 		return fmt.Errorf("put [%v,%v]: %w", recordCount, err)
// 	}

// 	// Next
// 	i := 0
// 	for it.Next(ctx) {
// 		key := it.Key(ctx)
// 		val := it.Value(ctx)
// 		expected := []byte{uint8(i)}
// 		if !bytes.Equal(key, expected) {
// 			return fmt.Errorf("next unequal key: got %v, expected %v", key, expected)
// 		}
// 		if !bytes.Equal(val, expected) {
// 			return fmt.Errorf("next unequal value: got %v, expected %v", val, expected)
// 		}
// 		i++
// 	}
// 	if recordCount+1 != i {
// 		return fmt.Errorf("found %d records, expected %d", i, recordCount)
// 	}
// 	return nil
// }

// dbRange iterates through a subset of records, asserting the number of
// records iterated match the number of inserted records.
func dbRange(ctx context.Context, db larry.Database, tables []string, total int) error {
	frac := total / 4
	start := frac
	end := frac * 3
	for _, table := range tables {
		s := newKey(start)
		e := newKey(end)

		it, err := db.NewRange(ctx, table, s, e)
		if err != nil {
			return fmt.Errorf("new range: %w", err)
		}
		i := 0
		for it.Next(ctx) {
			if !bytes.Equal(it.Key(ctx), newKey(start+i)) {
				return fmt.Errorf("invalid key got %v wanted %v",
					it.Key(ctx), newKey(start+i))
			}
			if !bytes.Equal(it.Value(ctx), newVal(start+i)) {
				return fmt.Errorf("invalid value got %x wanted %x",
					it.Value(ctx), newVal(start+i))
			}
			i++
		}
		if i != end-start {
			return fmt.Errorf("invalid record count got %v want %v", i, end-start)
		}
		it.Close(ctx)
	}
	return nil
}

// dbRangeFirstLast asserts the first and last record of
// the range matches the expected value.
func dbRangeFirstLast(ctx context.Context, db larry.Database, tables []string, total int) error {
	frac := total / 4
	start := frac
	// Test with end outside and inside
	// values inserted into the dbs
	for _, end := range []int{total + start, frac * 3} {
		for _, table := range tables {
			s := newKey(start)
			e := newKey(end)

			it, err := db.NewRange(ctx, table, s, e)
			if err != nil {
				return fmt.Errorf("new range: %w", err)
			}

			// First
			if !it.First(ctx) {
				return errors.New("first")
			}
			key := it.Key(ctx)
			val := it.Value(ctx)
			if !bytes.Equal(key, newKey(start)) {
				return fmt.Errorf("first unequal key: got %v, expected %v", key, newKey(start))
			}
			if !bytes.Equal(val, newVal(start)) {
				return fmt.Errorf("first unequal value: got %v, expected %v", val, newVal(start))
			}

			// Last
			if !it.Last(ctx) {
				return errors.New("last")
			}
			key = it.Key(ctx)
			val = it.Value(ctx)
			expectedKey := newKey(min(total-1, end-1))
			expectedVal := newVal(min(total-1, end-1))
			if !bytes.Equal(key, expectedKey) {
				return fmt.Errorf("last unequal key: got %v, expected %v", key, expectedKey)
			}
			if !bytes.Equal(val, expectedVal) {
				return fmt.Errorf("last unequal value: got %v, expected %v", val, expectedVal)
			}

			it.Close(ctx)
		}
	}
	return nil
}

func dbRangeEmpty(ctx context.Context, db larry.Database, tables []string, total int) error {
	for _, table := range tables {
		it, err := db.NewRange(ctx, table, nil, nil)
		if err != nil {
			return fmt.Errorf("new range: %w", err)
		}
		i := 0
		for it.Next(ctx) {
			if !bytes.Equal(it.Key(ctx), newKey(i)) {
				return fmt.Errorf("invalid key got %v wanted %v",
					it.Key(ctx), newKey(i))
			}
			if !bytes.Equal(it.Value(ctx), newVal(i)) {
				return fmt.Errorf("invalid value got %x wanted %x",
					it.Value(ctx), newVal(i))
			}
			i++
		}
		if i != total {
			return fmt.Errorf("invalid record count got %v want %v", i, total)
		}
		it.Close(ctx)
	}
	return nil
}

// dbBatch inserts N records into a batch, commits it, iterates
// through the table to ensure they exist, and deletes them.
func dbBatch(ctx context.Context, db larry.Database, table string, recordCount int) error {
	// Stuff a bunch of records into the same table to validate that
	// everything is executed as expected.
	b, err := db.NewBatch(ctx)
	if err != nil {
		return fmt.Errorf("new batch: %w", err)
	}

	for i := range recordCount {
		// Emulate user records "userXXXX"
		var key [8]byte
		copy(key[:], fmt.Appendf(nil, "user%04v", i))
		value := make([]byte, len(key)*2)
		copy(value[len(key):], key[:])
		b.Put(ctx, table, key[:], value)

		// Emulate user records "passXXXX"
		var pkey [8]byte
		copy(pkey[:], fmt.Appendf(nil, "pass%04v", i))
		eval := fmt.Appendf(nil, "thisisapassword%v", i)
		b.Put(ctx, table, pkey[:], eval)

		// Emulate avatar records "avatarXXXX"
		akey := fmt.Appendf(nil, "avatar%d", i)
		aval := fmt.Appendf(nil, "thisisavatar%d", i)
		b.Put(ctx, table, akey, aval)
	}
	err = db.Update(ctx, func(ctx context.Context, tx larry.Transaction) error {
		return tx.Write(ctx, b)
	})
	if err != nil {
		return fmt.Errorf("update 1: %w", err)
	}

	// Read everything back and create a batch to delete all keys.
	it, err := db.NewIterator(ctx, table)
	if err != nil {
		return fmt.Errorf("new iterator: %w", err)
	}
	defer it.Close(ctx)

	bd, err := db.NewBatch(ctx)
	if err != nil {
		return fmt.Errorf("new batch: %w", err)
	}
	i := 0
	for it.Next(ctx) {
		key := slices.Clone(it.Key(ctx))
		bd.Del(ctx, table, key)
		i++
	}
	if i != recordCount*3 {
		return fmt.Errorf("invalid record count got %v, wanted %v", i, recordCount*3)
	}
	// Close iterator so that we don't block
	it.Close(ctx)

	err = db.Update(ctx, func(ctx context.Context, txn larry.Transaction) error {
		return txn.Write(ctx, bd)
	})
	if err != nil {
		return fmt.Errorf("update 2: %w", err)
	}

	return nil
}

// dbBatchNoop ensures invalid deletes in a batch are ignored.
func dbBatchNoop(ctx context.Context, db larry.Database, table string) error {
	b, err := db.NewBatch(ctx)
	if err != nil {
		return fmt.Errorf("new batch: %w", err)
	}

	// Invalid del should be noop
	b.Del(ctx, table, newKey(0))

	// valid put
	validKey := newKey(1)
	b.Put(ctx, table, validKey, newVal(1))

	err = db.Update(ctx, func(ctx context.Context, tx larry.Transaction) error {
		return tx.Write(ctx, b)
	})
	if err != nil {
		return fmt.Errorf("update: %w", err)
	}

	_, err = db.Get(ctx, table, validKey)
	if err != nil {
		return fmt.Errorf("get: %w", err)
	}

	return nil
}

func dbOpenCloseOpen(ctx context.Context, db larry.Database, table string) error {
	// Open again expect fail
	err := db.Open(ctx)
	if err == nil {
		return errors.New("db open: expected error")
	}
	err = db.Put(ctx, table, []byte("xxx"), []byte("yyy"))
	if err != nil {
		return fmt.Errorf("db put: %w", err)
	}
	_, err = db.Get(ctx, table, []byte("xxx"))
	if err != nil {
		return fmt.Errorf("db get: %w", err)
	}
	// make sure we can close with tx open
	_, err = db.Begin(ctx, true)
	if err != nil {
		return fmt.Errorf("db begin: %w", err)
	}
	err = db.Close(ctx)
	if err != nil {
		return fmt.Errorf("db close: %w", err)
	}
	err = db.Open(ctx)
	if err != nil {
		return fmt.Errorf("db open: %w", err)
	}
	_, err = db.Get(ctx, table, []byte("xxx"))
	if err != nil {
		return fmt.Errorf("db get 2: %w", err)
	}

	return nil
}

func benchmarkLarryBatch(ctx context.Context, b *testing.B, dbFunc NewDBFunc) {
	for _, insertCount := range []int{1, 10, 100, 1000, 10000} {
		benchName := fmt.Sprintf("batch/%d", insertCount)
		b.Run(benchName, func(b *testing.B) {
			home := b.TempDir()

			tables := []string{"t"}

			db := dbFunc(home, tables)
			err := db.Open(ctx)
			if err != nil {
				b.Fatal(err)
			}
			defer func() {
				err := db.Close(ctx)
				if err != nil {
					b.Fatal(err)
				}
			}()

			keyList := make([][]byte, 0, insertCount)
			for i := range insertCount {
				keyList = append(keyList, newKey(i))
			}

			wb, err := db.NewBatch(ctx)
			if err != nil {
				b.Fatal(err)
			}
			for _, k := range keyList {
				wb.Put(ctx, "t", k, nil)
			}

			for b.Loop() {
				err := db.Update(ctx, func(ctx context.Context, tx larry.Transaction) error {
					return tx.Write(ctx, wb)
				})
				if err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func benchmarkLarryIter(ctx context.Context, b *testing.B, dbFunc NewDBFunc) {
	insertCount := 10
	benchName := fmt.Sprintf("iterate/%d", insertCount)
	b.Run(benchName, func(b *testing.B) {
		home := b.TempDir()

		table := "t"
		tables := []string{table}

		db := dbFunc(home, tables)
		err := db.Open(ctx)
		if err != nil {
			b.Fatal(err)
		}
		defer func() {
			err := db.Close(ctx)
			if err != nil {
				b.Fatal(err)
			}
		}()

		errc := db.Update(ctx, func(ctx context.Context, tx larry.Transaction) error {
			for i := range insertCount {
				key := newKey(i)
				if err := tx.Put(ctx, table, key, key); err != nil {
					return err
				}
			}
			return nil
		})
		if errc != nil {
			b.Fatal(errc)
		}

		it, err := db.NewIterator(ctx, table)
		if err != nil {
			b.Fatal(err)
		}

		for b.Loop() {
			it.First(ctx)
			for it.Next(ctx) {
			}
		}

		it.Close(ctx)
	})
}

func benchmarkLarryGet(ctx context.Context, b *testing.B, dbFunc NewDBFunc) {
	for _, insertCount := range []int{1, 10, 100, 1000, 10000, 100000} {
		benchName := fmt.Sprintf("get/%d", insertCount)
		b.Run(benchName, func(b *testing.B) {
			home := b.TempDir()

			table := "t"
			tables := []string{table}

			db := dbFunc(home, tables)
			err := db.Open(ctx)
			if err != nil {
				b.Fatal(err)
			}
			defer func() {
				err := db.Close(ctx)
				if err != nil {
					b.Fatal(err)
				}
			}()

			errc := db.Update(ctx, func(ctx context.Context, tx larry.Transaction) error {
				for i := range insertCount {
					key := newKey(i)
					if err := tx.Put(ctx, table, key, key); err != nil {
						return err
					}
				}
				return nil
			})
			if errc != nil {
				b.Fatal(errc)
			}

			for b.Loop() {
				var key [4]byte
				//nolint:gosec // strong randomizer not needed
				r := rand.Uint32N(uint32(insertCount))
				binary.BigEndian.PutUint32(key[:], r)
				_, err := db.Get(ctx, table, key[:])
				if err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

// TODO extra tests
// iterator / range concurrent put / del (for reverse reliant iters)
// iterator / range no keys
// insert large key / value
// tx ordered operations
// consider making multiple write txs not block
// tx put empty
