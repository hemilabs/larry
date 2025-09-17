# Larry

"I'm Larry, the database guy!" - Larry, the database guy

Larry is a common interface for writing Go code using different key-value (kv) databases. It provides a unified API, making it easier to switch between different databases without changing your application logic.

## Features

Larry was originally designed to be integrated into other Hemi projects and repositories, as such it provides:

- **Unified Interface**: Interact with multiple KV databases using a consistent API.
- **Database Support**: Easily extendable to support various KV databases.
- **Simple Integration**: Quick setup and integration into your existing Go projects.

## Supported Databases

The focus of Larry is not to create and maintain a series of database implementations. However, several databases were adapted to conform to the larry API, which you may use / modify for your own use-case:

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






## License

This project is licensed under the [MIT License](https://github.com/hemilabs/larry/blob/main/LICENSE).