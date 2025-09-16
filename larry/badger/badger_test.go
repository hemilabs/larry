package badger

import (
	"context"
	"encoding/binary"
	"testing"

	"github.com/hemilabs/larry/larry"
)

func BenchmarkNativeBatch(b *testing.B) {
	ctx, cancel := context.WithCancel(b.Context())
	defer cancel()

	home := b.TempDir()
	table := "table"
	tables := []string{table}

	cfg := DefaultBadgerConfig(home, tables)
	db, err := NewBadgerDB(cfg)
	if err != nil {
		panic(err)
	}

	if err := db.Open(ctx); err != nil {
		b.Fatal(err)
	}

	bdb := db.(*badgerDB)

	const insertCount = 100000

	keyList := make([][]byte, 0, insertCount)
	for i := range insertCount {
		var key [4]byte
		binary.BigEndian.PutUint32(key[:], uint32(i))
		keyList = append(keyList, key[:])
	}

	for b.Loop() {
		wb := bdb.db.NewWriteBatch()
		for _, k := range keyList {
			wb.Set(k, nil)
		}
		wb.Flush()
	}
}

func BenchmarkBatch(b *testing.B) {
	ctx, cancel := context.WithCancel(b.Context())
	defer cancel()

	home := b.TempDir()
	tables := []string{""}

	cfg := DefaultBadgerConfig(home, tables)
	db, err := NewBadgerDB(cfg)
	if err != nil {
		panic(err)
	}

	if err := db.Open(ctx); err != nil {
		b.Fatal(err)
	}

	const insertCount = 100000

	keyList := make([][]byte, 0, insertCount)
	for i := range insertCount {
		var key [4]byte
		binary.BigEndian.PutUint32(key[:], uint32(i))
		keyList = append(keyList, key[:])
	}

	wb, err := db.NewBatch(ctx)
	if err != nil {
		b.Fatal(err)
	}
	for _, k := range keyList {
		wb.Put(ctx, "", k, nil)
	}

	for b.Loop() {
		db.Update(ctx, func(ctx context.Context, tx larry.Transaction) error {
			return tx.Write(ctx, wb)
		})
	}
}
