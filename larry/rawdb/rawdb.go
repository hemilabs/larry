// Copyright (c) 2024-2025 Hemi Labs, Inc.
// Use of this source code is governed by the MIT License,
// which can be found in the LICENSE file.

// Package rawdb is an append-only, file-based key-value store for immutable
// data. It uses Larry compliant databases to index metadata while storing
// values in rolling disk files. No transactions or deletions are supported.
package rawdb

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"sync"

	"github.com/juju/loggo"

	"github.com/hemilabs/larry/larry"
	"github.com/hemilabs/larry/larry/clickhouse"
	"github.com/hemilabs/larry/larry/leveldb"
	"github.com/hemilabs/larry/larry/pebble"
)

const (
	logLevel = "INFO"

	indexDir     = "index"
	DataDir      = "data"
	DefaultTable = ""

	DefaultMaxFileSize = 256 * 1024 * 1024 // 256MB file max; will never be bigger.

	TypeLevelDB    = "leveldb"
	TypePebbleDB   = "pebble"
	TypeClickhouse = "clickhouse"
)

var (
	log             = loggo.GetLogger("db")
	lastFilenameKey = []byte("lastfilename")
)

func init() {
	if err := loggo.ConfigureLoggers(logLevel); err != nil {
		panic(err)
	}
}

// Assert required interfaces
var _ larry.Database = (*RawDB)(nil)

type RawDB struct {
	mtx sync.RWMutex

	cfg *Config

	index larry.Database
	open  bool
}

type Config struct {
	DB        string
	Home      string
	RemoteURI string
	Table     string
	MaxSize   int64
}

// DefaultRawDBConfig will set an empty string as the indexing
// table in order to minimize space used. Some indexing databases
// may not allow this, and thus this parameter should be
// manually changed if necessary.
func DefaultRawDBConfig(home, remoteURI string) *Config {
	return &Config{
		Home:      home,
		MaxSize:   DefaultMaxFileSize,
		Table:     DefaultTable,
		RemoteURI: remoteURI,
		DB:        TypeLevelDB,
	}
}

func NewRawDB(cfg *Config) (larry.Database, error) {
	log.Tracef("New")
	defer log.Tracef("New exit")

	if cfg == nil {
		return nil, errors.New("must provide config")
	}

	if cfg.MaxSize < 4096 {
		return nil, fmt.Errorf("invalid max size: %v", cfg.MaxSize)
	}

	switch cfg.DB {
	case TypeLevelDB:
	case TypePebbleDB:
	case TypeClickhouse:
		if cfg.Table == "" {
			return nil,
				errors.New("clickhouse prevents empty strings as table names")
		}
	default:
		return nil, fmt.Errorf("invalid db: %v", cfg.DB)
	}

	return &RawDB{
		cfg: cfg,
	}, nil
}

func (r *RawDB) Open(ctx context.Context) error {
	log.Tracef("Open")
	defer log.Tracef("Open exit")

	r.mtx.Lock()
	defer r.mtx.Unlock()

	if r.open {
		return errors.New("already open")
	}

	err := os.MkdirAll(filepath.Join(r.cfg.Home, DataDir), 0o0700)
	if err != nil {
		return fmt.Errorf("mkdir: %w", err)
	}
	// We do this late because directories are created at this point
	switch r.cfg.DB {
	case TypeLevelDB:
		lcfg := leveldb.DefaultLevelDBConfig(filepath.Join(r.cfg.Home, indexDir),
			[]string{r.cfg.Table})
		r.index, err = leveldb.NewLevelDB(lcfg)
	case TypePebbleDB:
		lcfg := pebble.DefaultPebbleConfig(filepath.Join(r.cfg.Home, indexDir),
			[]string{r.cfg.Table})
		r.index, err = pebble.NewPebbleDB(lcfg)
	case TypeClickhouse:
		lcfg := clickhouse.DefaultClickConfig(r.cfg.RemoteURI,
			[]string{r.cfg.Table})
		r.index, err = clickhouse.NewClickDB(lcfg)
	default:
	}
	if err != nil {
		return fmt.Errorf("new: %w", err)
	}

	err = r.index.Open(ctx)
	if err != nil {
		return fmt.Errorf("open: %w", err)
	}
	r.open = true

	return nil
}

func (r *RawDB) Close(ctx context.Context) error {
	log.Tracef("Close")
	defer log.Tracef("Close exit")

	r.mtx.Lock()
	defer r.mtx.Unlock()

	err := r.index.Close(ctx)
	if err != nil {
		return err
	}
	r.open = false

	// Don't set r.index to nil since that races during shutdown. Just let
	// the commands error out with ErrClose.

	return nil
}

// DB returns the underlying index database.
// You should probably not be calling this! It is used for external database
// upgrades.
func (r *RawDB) DB() larry.Database {
	return r.index
}

func (r *RawDB) Del(_ context.Context, _ string, _ []byte) error {
	return larry.ErrNotSupported
}

func (r *RawDB) Has(ctx context.Context, table string, key []byte) (bool, error) {
	log.Tracef("Has")
	defer log.Tracef("Has exit")

	return r.index.Has(ctx, table, key)
}

func (r *RawDB) Put(ctx context.Context, table string, key, value []byte) error {
	log.Tracef("Put")
	defer log.Tracef("Put exit")

	if int64(len(value)) > r.cfg.MaxSize {
		return fmt.Errorf("length exceeds maximum length: %v > %v",
			len(value), r.cfg.MaxSize)
	}

	// Assert we do not have this key stored yet.
	if ok, err := r.index.Has(ctx, table, key); ok {
		return errors.New("key already exists")
	} else if err != nil {
		return err
	}

	r.mtx.Lock()
	defer r.mtx.Unlock()

	tries := 0
	for {
		// This should not happen, but we must ensure we aren't spinning.
		if tries > 1 {
			return errors.New("could not determine last filename")
		}

		lfe, err := r.index.Get(ctx, table, lastFilenameKey)
		if err != nil {
			if errors.Is(err, larry.ErrKeyNotFound) {
				lfe = []byte{0, 0, 0, 0}
			} else {
				return err
			}
		}
		last := binary.BigEndian.Uint32(lfe)
		lastFilename := filepath.Join(r.cfg.Home, DataDir,
			fmt.Sprintf("%010v", last))

		// determine if data fits.
		fh, err := os.OpenFile(lastFilename, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o600)
		if err != nil {
			return err
		}
		defer func() {
			// Close all files we opened along the way.
			err := fh.Close()
			if err != nil {
				log.Errorf("close %v: %v", lastFilename, err)
			}
		}()

		fi, err := fh.Stat()
		if err != nil {
			return err
		} else if fi.Size()+int64(len(value)) > r.cfg.MaxSize {
			last++
			lastData := make([]byte, 8)
			binary.BigEndian.PutUint32(lastData, last)
			err = r.index.Put(ctx, table, lastFilenameKey, lastData)
			if err != nil {
				return err
			}
			tries++
			continue
		}
		// Encoded coordinates.
		c := make([]byte, 4+4+4)
		binary.BigEndian.PutUint32(c[0:4], last)

		fis := fi.Size()
		if fis < 0 || fis > math.MaxUint32 {
			return fmt.Errorf("invalid file info conversion to uint32: %v",
				fi.Size())
		}
		binary.BigEndian.PutUint32(c[4:8], uint32(fis))

		vl := len(value)
		if vl > math.MaxUint32 {
			return fmt.Errorf("invalid len conversion to uint32: %v",
				len(value))
		}
		binary.BigEndian.PutUint32(c[8:12], uint32(vl))

		// Append value to latest file.
		n, err := fh.Write(value)
		if err != nil {
			return err
		}
		if n != len(value) {
			return fmt.Errorf("partial write, data corruption: %v != %v", n, len(value))
		}

		// Write coordinates
		err = r.index.Put(ctx, table, key, c)
		if err != nil {
			return err
		}

		return nil
	}
}

func (r *RawDB) Get(ctx context.Context, table string, key []byte) ([]byte, error) {
	log.Tracef("Get: %x", key)
	defer log.Tracef("Get exit: %x", key)

	c, err := r.index.Get(ctx, table, key)
	if err != nil {
		return nil, err
	}
	if len(c) != 12 {
		// Should not happen.
		return nil, errors.New("invalid coordinates")
	}
	filename := filepath.Join(r.cfg.Home, DataDir, fmt.Sprintf("%010v",
		binary.BigEndian.Uint32(c[0:4])))
	offset := binary.BigEndian.Uint32(c[4:8])
	size := binary.BigEndian.Uint32(c[8:12])
	f, err := os.OpenFile(filename, os.O_RDONLY, 0o600)
	if err != nil {
		return nil, err
	}
	defer func() {
		err := f.Close()
		if err != nil {
			log.Errorf("close: %v", err)
		}
	}()

	data := make([]byte, size)
	n, err := f.ReadAt(data, int64(offset))
	if err != nil {
		return nil, err
	}
	if n != int(size) {
		return nil, errors.New("invalid read size")
	}

	return data, nil
}

func (r *RawDB) Begin(_ context.Context, _ bool) (larry.Transaction, error) {
	return nil, larry.ErrNotSupported
}

func (r *RawDB) View(_ context.Context, _ func(_ context.Context, _ larry.Transaction) error) error {
	return larry.ErrNotSupported
}

func (r *RawDB) Update(_ context.Context, _ func(ctx context.Context, _ larry.Transaction) error) error {
	return larry.ErrNotSupported
}

func (r *RawDB) NewIterator(_ context.Context, _ string) (larry.Iterator, error) {
	return nil, larry.ErrNotSupported
}

func (r *RawDB) NewRange(_ context.Context, _ string, _, _ []byte) (larry.Range, error) {
	return nil, larry.ErrNotSupported
}

func (r *RawDB) NewBatch(_ context.Context) (larry.Batch, error) {
	return nil, larry.ErrNotSupported
}
