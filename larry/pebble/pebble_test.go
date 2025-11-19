// Copyright (c) 2025 Hemi Labs, Inc.
// Use of this source code is governed by the MIT License,
// which can be found in the LICENSE file.

package pebble

import (
	"testing"

	"github.com/hemilabs/larry/internal/testutil"
	"github.com/hemilabs/larry/larry"
)

var dbFunc = func(home string, tables []string, _ map[string]string) larry.Database {
	cfg := DefaultPebbleConfig(home, tables)
	db, err := NewPebbleDB(cfg)
	if err != nil {
		panic(err)
	}
	return db
}

func TestPebble(t *testing.T) {
	t.Parallel()
	testutil.RunLarryTests(t, dbFunc, false)
}

func BenchmarkPebble(b *testing.B) {
	testutil.RunLarryBatchBenchmarks(b, dbFunc)
}
