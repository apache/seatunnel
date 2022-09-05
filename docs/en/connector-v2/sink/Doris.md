# Doris

> Doris sink connector

## Description

A sink plugin which use stream load to write data to doris.

## Key features

- [ ] [exactly-once](../../concept/connector-v2-features.md)
- [ ] [schema projection](../../concept/connector-v2-features.md)

## Options

| name       | type        | required | default value |
|------------| ----------  | ----- |---------------|
| fenodes    | string      | yes   | -             |
| user       | string      | yes   | -             |
| password   | string      | yes   | -             |
| databse    | string      | yes   | -             |
| table      | string      | yes   | -             |
| batch_size | string      |       | 10            |

### fenodes [string]

Doris frontend endpoint address, eg: localhost:8030

### user & password [string]

Doris frontend endpoint's username & password

### database & table [string]

Doris cluster name & table name.

## Example

```hocon
sink {
  Doris {
    fenodes: "seatunnel-doris-network:8030",
    user: "root",
    password: "",
    table: "seatunnel",
    database: "test"
    batch_size: 10
  }
}
```
