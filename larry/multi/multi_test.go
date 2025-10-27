// Copyright (c) 2024-2025 Hemi Labs, Inc.
// Use of this source code is governed by the MIT License,
// which can be found in the LICENSE file.

package multi

import (
	"testing"

	"github.com/hemilabs/larry/internal/testutil"
	"github.com/hemilabs/larry/larry"
)

func TestMultiDB(t *testing.T) {
	t.Parallel()
	dbFunc := func(home string, tables []string) larry.Database {
		tm := make(map[string]string, len(tables))
		for _, t := range tables {
			tm[t] = TypeLevelDB
		}
		cfg := DefaultMultiConfig(home, tm)
		db, err := NewMultiDB(cfg)
		if err != nil {
			panic(err)
		}
		return db
	}
	testutil.RunLarryTests(t, dbFunc, false)
}

func BenchmarkLevelDB(b *testing.B) {
	dbFunc := func(home string, tables []string) larry.Database {
		tm := make(map[string]string, len(tables))
		for _, t := range tables {
			tm[t] = TypeLevelDB
		}
		cfg := DefaultMultiConfig(home, tm)
		db, err := NewMultiDB(cfg)
		if err != nil {
			panic(err)
		}
		return db
	}
	testutil.RunLarryBatchBenchmarks(b, dbFunc)
}
