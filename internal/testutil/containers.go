// Copyright (c) 2025 Hemi Labs, Inc.
// Use of this source code is governed by the MIT License,
// which can be found in the LICENSE file.

package testutil

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/docker/go-connections/nat"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	cont "github.com/testcontainers/testcontainers-go/modules/clickhouse"
	"github.com/testcontainers/testcontainers-go/wait"

	_ "embed"
)

//go:embed testdata/config.xml
var config []byte

func CreateClickContainer(ctx context.Context, t *testing.T) (*cont.ClickHouseContainer, error) {
	t.Helper()

	DockerTestCheck(t)

	td := t.TempDir()
	f, err := os.Create(filepath.Join(td, "config.xml"))
	if err != nil {
		return nil, err
	}
	defer f.Close()
	_, err = f.Write(config)
	if err != nil {
		return nil, err
	}

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
		cont.WithConfigFile(filepath.Join(td, "config.xml")),
		cont.WithZookeeper(ipaddr, zkPort.Port()),
	)
	testcontainers.CleanupContainer(t, c)
	require.NoError(t, err)

	err = c.CopyToContainer(ctx, config, "/etc/clickhouse-server/config.d/config.xml", 777)
	if err != nil {
		return nil, err
	}

	return c, nil
}
