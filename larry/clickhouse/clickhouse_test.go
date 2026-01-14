// Copyright (c) 2025 Hemi Labs, Inc.
// Use of this source code is governed by the MIT License,
// which can be found in the LICENSE file.

package clickhouse_test

import (
	"context"
	"testing"

	"github.com/hemilabs/larry/internal/testutil"
	"github.com/hemilabs/larry/larry"
	"github.com/hemilabs/larry/larry/clickhouse"
	"github.com/hemilabs/larry/larry/leveldb"
)

func TestClickhouse(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	c, err := testutil.CreateClickContainer(ctx, t)
	if err != nil {
		t.Fatal(err)
	}
	conn, err := c.ConnectionString(ctx)
	if err != nil {
		t.Fatal(err)
	}

	dbFunc := func(home string, tables []string, opt map[string]string) larry.Database {
		if _, ok := opt["level"]; ok {
			cfg := leveldb.DefaultLevelDBConfig(home, tables)
			db, err := leveldb.NewLevelDB(cfg)
			if err != nil {
				panic(err)
			}
			return db
		}

		_, ok := opt["reopen"]
		drop := !ok
		cfg := clickhouse.ClickConfig{
			URI:        conn,
			Tables:     tables,
			DropTables: drop,
		}
		db, err := clickhouse.NewClickDB(&cfg)
		if err != nil {
			panic(err)
		}
		return db
	}
	testutil.RunLarryTests(t, dbFunc, true)
}
