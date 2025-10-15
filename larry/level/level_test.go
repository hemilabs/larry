// Copyright (c) 2025 Hemi Labs, Inc.
// Use of this source code is governed by the MIT License,
// which can be found in the LICENSE file.

package level

import (
	"testing"

	"github.com/hemilabs/larry/larry"
	"github.com/hemilabs/larry/larry/testutil"
)

func TestLevelDB(t *testing.T) {
	t.Parallel()
	dbFunc := func(home string, tables []string) larry.Database {
		cfg := DefaultLevelConfig(home, tables)
		db, err := NewLevelDB(cfg)
		if err != nil {
			panic(err)
		}
		return db
	}
	testutil.RunLarryTests(t, dbFunc, false)
}

func BenchmarkLevelDB(b *testing.B) {
	dbFunc := func(home string, tables []string) larry.Database {
		cfg := DefaultLevelConfig(home, tables)
		db, err := NewLevelDB(cfg)
		if err != nil {
			panic(err)
		}
		return db
	}
	testutil.RunLarryBatchBenchmarks(b, dbFunc)
}
