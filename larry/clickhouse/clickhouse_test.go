// Copyright (c) 2025 Hemi Labs, Inc.
// Use of this source code is governed by the MIT License,
// which can be found in the LICENSE file.

package clickhouse_test

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/docker/go-connections/nat"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	cont "github.com/testcontainers/testcontainers-go/modules/clickhouse"
	"github.com/testcontainers/testcontainers-go/wait"

	"github.com/hemilabs/larry/internal/testutil"
	"github.com/hemilabs/larry/larry"
	"github.com/hemilabs/larry/larry/clickhouse"
	"github.com/hemilabs/larry/larry/leveldb"
)

func TestClickhouse(t *testing.T) {
	t.Parallel()
	testutil.DockerTestCheck(t)

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	c, err := createContainer(ctx, t)
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

func createContainer(ctx context.Context, t *testing.T) (*cont.ClickHouseContainer, error) {
	t.Helper()
	zkPort := nat.Port("2181/tcp")
	zkcontainer, err := testcontainers.Run(ctx, "zookeeper:3.7",
		testcontainers.WithExposedPorts(zkPort.Port()),
		testcontainers.WithWaitStrategy(wait.ForListeningPort(zkPort)),
	)
	testcontainers.CleanupContainer(t, zkcontainer)
	require.NoError(t, err)

	ipaddr, err := zkcontainer.ContainerIP(ctx)
	require.NoError(t, err)

	c, err := cont.Run(ctx,
		"clickhouse/clickhouse-server:25.10-alpine",
		cont.WithConfigFile(filepath.Join("testdata", "config.xml")),
		cont.WithZookeeper(ipaddr, zkPort.Port()),
	)
	testcontainers.CleanupContainer(t, c)
	require.NoError(t, err)

	return c, nil
}
