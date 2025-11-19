// Copyright (c) 2025 Hemi Labs, Inc.
// Use of this source code is governed by the MIT License,
// which can be found in the LICENSE file.

package pebble

import (
	"testing"

	"github.com/hemilabs/larry/internal/testutil"
	"github.com/hemilabs/larry/larry"
)

func TestPebble(t *testing.T) {
	t.Parallel()
	dbFunc := func(home string, tables []string, _ map[string]string) larry.Database {
		cfg := DefaultPebbleConfig(home, tables)
		db, err := NewPebbleDB(cfg)
		if err != nil {
			panic(err)
		}
		return db
	}
	testutil.RunLarryTests(t, dbFunc, false)
}

func BenchmarkPebble(b *testing.B) {
	dbFunc := func(home string, tables []string, _ map[string]string) larry.Database {
		cfg := DefaultPebbleConfig(home, tables)
		db, err := NewPebbleDB(cfg)
		if err != nil {
			panic(err)
		}
		return db
	}
	testutil.RunLarryBatchBenchmarks(b, dbFunc)
}
