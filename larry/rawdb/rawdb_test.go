// Copyright (c) 2024-2025 Hemi Labs, Inc.
// Use of this source code is governed by the MIT License,
// which can be found in the LICENSE file.

package rawdb

import (
	"bytes"
	"context"
	"math"
	"os"
	"testing"

	"github.com/hemilabs/larry/internal/testutil"
)

func testRawDB(t *testing.T, dbs, remoteURI string) {
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

	blockSize := int64(4096)
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
			&Config{DB: dbs, Home: home, MaxSize: blockSize, RemoteURI: remoteURI,
				Table: table})
		if err != nil {
			t.Fatal(err)
		}
		err = rdb2.Open(ctx)
		if err == nil {
			t.Fatal("expected locked db")
		}
	}

	key := []byte("key")
	data := []byte("hello, world!")
	err = rdb.Put(ctx, table, key, data)
	if err != nil {
		t.Fatalf("%T %v", err, err)
	}
	KEY := []byte("KEY")
	DATA := []byte("HELLO, WORLD!")
	err = rdb.Put(ctx, table, KEY, DATA)
	if err != nil {
		t.Fatal(err)
	}

	// Get data out again
	dataRead, err := rdb.Get(ctx, table, key)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(data, dataRead) {
		t.Fatal("data not identical")
	}
	dataRead, err = rdb.Get(ctx, table, KEY)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(DATA, dataRead) {
		t.Fatal("data not identical")
	}

	// Overflow to next file
	overflowData := make([]byte, int(blockSize)-len(data)-len(DATA)+1)
	for k := range overflowData {
		k = k % (math.MaxUint8 + 1)
		if k < 0 || k > math.MaxUint8 {
			t.Fatalf("uint8 overflow: %v", k)
		}
		overflowData[k] = uint8(k)
	}
	overflowKey := []byte("overflow")
	err = rdb.Put(ctx, table, overflowKey, overflowData)
	if err != nil {
		t.Fatal(err)
	}
	overflowRead, err := rdb.Get(ctx, table, overflowKey)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(overflowData, overflowRead) {
		t.Fatal("overflow data not identical")
	}
}

//nolint:paralleltest // distributed dbs use testcontainers
func TestRawDBS(t *testing.T) {
	dbs := []string{TypeLevelDB, TypePebble, TypeClickhouse}
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
			log.Infof("testing: %v", v)
			testRawDB(t, v, conn)
		})
	}
}
