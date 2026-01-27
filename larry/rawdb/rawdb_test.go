// Copyright (c) 2024-2025 Hemi Labs, Inc.
// Use of this source code is governed by the MIT License,
// which can be found in the LICENSE file.

package rawdb

import (
	"context"
	"os"
	"testing"

	"github.com/hemilabs/larry/internal/testutil"
)

func testRawDB(t *testing.T, dbs, remoteURI string) {
	blockSize := int64(4096)

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	home := t.TempDir()
	remove := true
	defer func() {
		if !remove {
			t.Logf("did not remove home: %v", home)
			return
		}

		if err := os.RemoveAll(home); err != nil {
			panic(err)
		}
	}()

	table := DefaultTable
	if dbs == TypeClickhouse {
		table = "rawdb"
	}
	rdb, err := NewRawDB(&Config{
		DB: dbs, Home: home, MaxSize: blockSize, RemoteURI: remoteURI,
		Table: table,
	})
	if err != nil {
		t.Fatal(err)
	}
	err = rdb.Open(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		err := rdb.Close(ctx)
		if err != nil {
			panic(err)
		}
	}()

	if dbs != TypeClickhouse {
		// Open again and expect locked failure
		rdb2, err := NewRawDB(
			&Config{
				DB: dbs, Home: home, MaxSize: blockSize, RemoteURI: remoteURI,
				Table: table,
			})
		if err != nil {
			t.Fatal(err)
		}
		err = rdb2.Open(ctx)
		if err == nil {
			t.Fatal("expected locked db")
		}
	}

	if err := testutil.RunRawDBTests(ctx, rdb, table); err != nil {
		t.Fatal(err)
	}
}

//nolint:paralleltest // distributed dbs use testcontainers
func TestRawDBS(t *testing.T) {
	dbs := []string{TypeLevelDB, TypePebbleDB, TypeClickhouse}
	for _, v := range dbs {
		t.Run(v, func(t *testing.T) {
			var conn string
			if v == TypeClickhouse {
				c, err := testutil.CreateClickContainer(t.Context(), t)
				if err != nil {
					t.Error(err)
				}
				conn, err = c.ConnectionString(t.Context())
				if err != nil {
					t.Error(err)
				}
			} else {
				t.Parallel()
			}
			testRawDB(t, v, conn)
		})
	}
}
