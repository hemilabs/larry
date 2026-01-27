// Copyright (c) 2024-2025 Hemi Labs, Inc.
// Use of this source code is governed by the MIT License,
// which can be found in the LICENSE file.

package rawdb

import (
	"testing"

	"github.com/hemilabs/larry/internal/testutil"
	"github.com/hemilabs/larry/larry"
)

func TestRawDB(t *testing.T) {
	t.Parallel()
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
			}
			dbFunc := func(home string, tables []string, _ map[string]string) larry.Database {
				rdb, err := NewRawDB(&Config{
					DB: v, Home: home, MaxSize: testutil.RawDBTestBlockSize,
					RemoteURI: conn, Table: tables[0],
				})
				if err != nil {
					panic(err)
				}
				return rdb
			}
			testutil.RunRawDBTests(t, dbFunc, conn != "")
		})
	}
}
