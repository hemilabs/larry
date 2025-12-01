// Copyright (c) 2025 Hemi Labs, Inc.
// Use of this source code is governed by the MIT License,
// which can be found in the LICENSE file.

package leveldb

import (
	"testing"

	"github.com/hemilabs/larry/internal/testutil"
	"github.com/hemilabs/larry/larry"
)

var dbFunc = func(home string, tables []string, _ map[string]string) larry.Database {
	cfg := DefaultLevelDBConfig(home, tables)
	db, err := NewLevelDB(cfg)
	if err != nil {
		panic(err)
	}
	return db
}

func TestLevelDB(t *testing.T) {
	t.Parallel()
	testutil.RunLarryTests(t, dbFunc, false)
}

func BenchmarkLevelDB(b *testing.B) {
	testutil.RunLarryBatchBenchmarks(b, dbFunc)
}
