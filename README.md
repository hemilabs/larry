# Larry

Larry is a common interface for writing Go code using different key-value (kv) databases. It provides a unified API, making it easier to switch between different databases without changing your application logic.

## Getting Started

### Prerequisites

- [Go v1.25+](https://go.dev/dl/)

### Installing

From your project, run the following command to retrieve the library:

```sh
go get github.com/hemilabs/larry
```

## Features

Larry was originally designed to be integrated into other Hemi projects and repositories, as such it provides:

- **Unified Interface**: Interact with multiple KV databases using a consistent API.
- **Database Support**: Easily extendable to support various KV databases.
- **Simple Integration**: Quick setup and integration into your existing Go projects.

## Supported Databases

The focus of Larry is not to create and maintain a series of database implementations. However, several databases were adapted to conform to the Larry API, which you may use / modify for your own use-case:

### KV Databases

| Database   | Driver                                      | Status      | 
|------------|---------------------------------------------|-------------|
| badgerDB   | [`github.com/hypermodeinc/badger`](https://github.com/hypermodeinc/badger)             | Supported   |
| bbolt      | [`github.com/etcd-io/bbolt`](https://github.com/etcd-io/bbolt)                   | Supported   |
| bitcask    | [`git.mills.io/prologic/bitcask`](https://git.mills.io/prologic/bitcask)              | Supported   |
| btree      | [`github.com/guycipher/btree`](https://github.com/guycipher/btree)                 | In Progress |
| buntDB     | [`github.com/tidwall/buntdb`](https://github.com/tidwall/buntdb)                  | Supported   |
| levelDB    | [`github.com/syndtr/goleveldb`](https://github.com/syndtr/goleveldb)                | Supported   |
| nutsDB     | [`github.com/nutsdb/nutsdb`](https://github.com/nutsdb/nutsdb)                   | Supported   |
| pebble     | [`github.com/cockroachdb/pebble`](https://github.com/cockroachdb/pebble)              | Supported   |

### General Purpose

| Database   | Driver                                      | Status      | 
|------------|---------------------------------------------|-------------|
| clickhouse   | [`github.com/ClickHouse/clickhouse-go`](https://github.com/ClickHouse/clickhouse-go)             | In Progress   |
| mongoDB      | [`github.com/mongodb/mongo-go-driver`](github.com/mongodb/mongo-go-driver)                   | Supported   |

## Replicator

The Replicator implementation can be used to copy and "translate" the contents of one Larry API compliant database to another (e.g., leveldb to mongoDB).

It functions by keeping a log of the operations performed in the source DB using the Larry API, which are then replayed in the destination DB, either in a synchronous (direct mode) or asynchronous (lazy mode) manner.

![TX Comparison](images/Replicator.svg)

## MultiDB

The MultiDB implementation aggregates multiple Larry API compliant databases into a single DB, where each DB is accessed as a table.

This allows one to separate database tables into multiple files, as well as locking transactions to a specific table, rather than the entire database.

## License

This project is licensed under the [MIT License](https://github.com/hemilabs/larry/blob/main/LICENSE).