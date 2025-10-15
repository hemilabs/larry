// Copyright (c) 2025 Hemi Labs, Inc.
// Use of this source code is governed by the MIT License,
// which can be found in the LICENSE file.

package larry

import (
	"bytes"
	"errors"
	"testing"
)

func TestCompositeKey(t *testing.T) {
	t.Parallel()
	type testTableItem struct {
		name       string
		key, table string
		expected   string
	}
	testTable := []testTableItem{
		{
			name:     "default",
			key:      "key",
			table:    "table",
			expected: "table:key",
		},
		{
			name:     "empty table",
			key:      "key",
			table:    "",
			expected: "key",
		},
		{
			name:     "empty key",
			key:      "",
			table:    "table",
			expected: "table:",
		},
		{
			name:     "all empty",
			key:      "",
			table:    "",
			expected: "",
		},
	}
	for _, tti := range testTable {
		t.Run(tti.name, func(t *testing.T) {
			ck := NewCompositeKey(tti.table, []byte(tti.key))
			if !bytes.Equal(ck, []byte(tti.expected)) {
				t.Errorf("NewCompositeKey: got %s, expected %v", string(ck), tti.expected)
				t.Fail()
			}
			k := KeyFromComposite(tti.table, ck)
			if !bytes.Equal(k, []byte(tti.key)) {
				t.Errorf("KeyFromComposite: got %s, expected %v", string(k), tti.key)
				t.Fail()
			}
		})
	}
}

func TestInvalidComposite(t *testing.T) {
	t.Parallel()
	defer func() {
		if r := recover(); r == nil {
			t.Errorf("expected panic")
		}
	}()
	_ = KeyFromComposite("table", []byte(""))
}

func TestDummy(t *testing.T) {
	t.Parallel()

	ctx := t.Context()
	home := t.TempDir()
	cfg := DefaultDummyConfig(home, []string{"table"})
	failCfg := DefaultDummyConfig(home, []string{"table", "table"})

	// Open without a config
	_, err := NewDummyDB(nil)
	if err == nil {
		t.Fatal("expected error")
	}

	// Open with duplicate tables
	_, err = NewDummyDB(failCfg)
	if err == nil {
		t.Fatal("expected error")
	}

	db, err := NewDummyDB(cfg)
	if err != nil {
		t.Fatal(err)
	}

	// Basic

	if err := db.Open(ctx); !errors.Is(err, ErrDummy) {
		t.Fatalf("expected error: %v", ErrDummy)
	}

	if err := db.Close(ctx); !errors.Is(err, ErrDummy) {
		t.Fatalf("expected error: %v", ErrDummy)
	}

	if err := db.Del(ctx, "", nil); !errors.Is(err, ErrDummy) {
		t.Fatalf("expected error: %v", ErrDummy)
	}

	if _, err := db.Has(ctx, "", nil); !errors.Is(err, ErrDummy) {
		t.Fatalf("expected error: %v", ErrDummy)
	}

	if _, err := db.Get(ctx, "", nil); !errors.Is(err, ErrDummy) {
		t.Fatalf("expected error: %v", ErrDummy)
	}

	if err := db.Put(ctx, "", nil, nil); !errors.Is(err, ErrDummy) {
		t.Fatalf("expected error: %v", ErrDummy)
	}

	if err := db.Update(ctx, nil); !errors.Is(err, ErrDummy) {
		t.Fatalf("expected error: %v", ErrDummy)
	}

	if err := db.View(ctx, nil); !errors.Is(err, ErrDummy) {
		t.Fatalf("expected error: %v", ErrDummy)
	}

	// Transactions

	tx, err := db.Begin(ctx, true)
	if err != nil {
		t.Fatal(err)
	}

	if err := tx.Del(ctx, "", nil); !errors.Is(err, ErrDummy) {
		t.Fatalf("expected error: %v", ErrDummy)
	}

	if _, err := tx.Has(ctx, "", nil); !errors.Is(err, ErrDummy) {
		t.Fatalf("expected error: %v", ErrDummy)
	}

	if _, err := tx.Get(ctx, "", nil); !errors.Is(err, ErrDummy) {
		t.Fatalf("expected error: %v", ErrDummy)
	}

	if err := tx.Put(ctx, "", nil, nil); !errors.Is(err, ErrDummy) {
		t.Fatalf("expected error: %v", ErrDummy)
	}

	if err := tx.Commit(ctx); !errors.Is(err, ErrDummy) {
		t.Fatalf("expected error: %v", ErrDummy)
	}

	if err := tx.Rollback(ctx); !errors.Is(err, ErrDummy) {
		t.Fatalf("expected error: %v", ErrDummy)
	}

	if err := tx.Write(ctx, nil); !errors.Is(err, ErrDummy) {
		t.Fatalf("expected error: %v", ErrDummy)
	}

	// Batch

	b, err := db.NewBatch(ctx)
	if err != nil {
		t.Fatal(err)
	}

	b.Del(ctx, "", nil)
	b.Put(ctx, "", nil, nil)
	b.Reset(ctx)

	// Iterator

	it, err := db.NewIterator(ctx, "")
	if err != nil {
		t.Fatal(err)
	}

	if it.First(ctx) {
		t.Fatal("expected false")
	}

	if it.Last(ctx) {
		t.Fatal("expected false")
	}

	if it.Next(ctx) {
		t.Fatal("expected false")
	}

	if it.Seek(ctx, nil) {
		t.Fatal("expected false")
	}

	if it.Key(ctx) != nil {
		t.Fatal("expected nil result")
	}

	if it.Value(ctx) != nil {
		t.Fatal("expected nil result")
	}

	it.Close(ctx)

	// Range

	r, err := db.NewRange(ctx, "", nil, nil)
	if err != nil {
		t.Fatal(err)
	}

	if r.First(ctx) {
		t.Fatal("expected false")
	}

	if r.Last(ctx) {
		t.Fatal("expected false")
	}

	if r.Next(ctx) {
		t.Fatal("expected false")
	}

	if r.Key(ctx) != nil {
		t.Fatal("expected nil result")
	}

	if r.Value(ctx) != nil {
		t.Fatal("expected nil result")
	}

	r.Close(ctx)
}
