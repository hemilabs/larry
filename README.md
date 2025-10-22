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
| levelDB    | [`github.com/syndtr/goleveldb`](https://github.com/syndtr/goleveldb) | Supported   |


### General Purpose

| Database   | Driver                                      | Status      | 
|------------|---------------------------------------------|-------------|
| TBD        | `------`                                    | ------      |

## License

This project is licensed under the [MIT License](https://github.com/hemilabs/larry/blob/main/LICENSE).
