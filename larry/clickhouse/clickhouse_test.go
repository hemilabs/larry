package clickhouse

import (
	"context"
	"os"
	"testing"
)

func TestOpenClickhouse(t *testing.T) {
	uri := os.Getenv("CLICKHOUSE_TEST_URI")
	if uri == "" {
		t.Skip("clickhouse URI not set")
	}

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	table := "ttab"

	cfg := DefaultClickConfig(uri, []string{table})
	cfg.DropTables = true
	db, err := NewClickDB(cfg)
	if err != nil {
		t.Fatal(err)
	}

	if err := db.Open(ctx); err != nil {
		t.Fatal(err)
	}

	if err := db.Close(ctx); err != nil {
		t.Fatal(err)
	}
}
