// Copyright (c) 2025-2026 Hemi Labs, Inc.
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

const (
	zookeeperImage  = "zookeeper:3.9@sha256sha256:b87f5ea0cdc73d71c74875277ca2e862f7abb3c0bfa365bd818db71eef870917"
	clickhouseImage = "clickhouse/clickhouse-server:25.12-alpine@sha256:74da41cd61db84f652c6364fd30d59e19b7276d34f7c82515f5f0e70d6f325da"
)

//go:embed testdata/config.xml
var clickhouseConfig []byte

func CreateClickContainer(ctx context.Context, t *testing.T) (*cont.ClickHouseContainer, error) {
	t.Helper()

	DockerTestCheck(t)

	td := t.TempDir()
	f, err := os.Create(filepath.Join(td, "config.xml"))
	if err != nil {
		return nil, err
	}
	defer f.Close()
	_, err = f.Write(clickhouseConfig)
	if err != nil {
		return nil, err
	}

	zkPort := nat.Port("2181/tcp")
	zkcontainer, err := testcontainers.Run(ctx, zookeeperImage,
		testcontainers.WithExposedPorts(zkPort.Port()),
		testcontainers.WithWaitStrategy(wait.ForListeningPort(zkPort)),
	)
	testcontainers.CleanupContainer(t, zkcontainer)
	require.NoError(t, err)

	ipaddr, err := zkcontainer.ContainerIP(ctx)
	require.NoError(t, err)
	c, err := cont.Run(ctx,
		clickhouseImage,
		cont.WithConfigFile(filepath.Join(td, "config.xml")),
		cont.WithZookeeper(ipaddr, zkPort.Port()),
	)
	testcontainers.CleanupContainer(t, c)
	require.NoError(t, err)

	err = c.CopyToContainer(ctx, clickhouseConfig, "/etc/clickhouse-server/config.d/config.xml", 777)
	if err != nil {
		return nil, err
	}

	return c, nil
}
