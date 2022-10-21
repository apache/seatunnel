# MongoDb

> MongoDB sink connector

## Description

Write data to `MongoDB`

## Key features

- [x] [batch](../../concept/connector-v2-features.md)
- [x] [stream](../../concept/connector-v2-features.md)
- [ ] [exactly-once](../../concept/connector-v2-features.md)
- [ ] [schema projection](../../concept/connector-v2-features.md)
- [ ] [parallelism](../../concept/connector-v2-features.md)
- [ ] [support user-defined split](../../concept/connector-v2-features.md)

## Options

| name           | type   | required | default value |
|--------------- | ------ |----------| ------------- |
| uri            | string | yes      | -             |
| database       | string | yes      | -             |
| collection     | string | yes      | -             |
| common-options |        | no       | -             |

### uri [string]

uri to write to mongoDB

### database [string]

database to write to mongoDB

### collection [string]

collection to write to mongoDB

### common options

Sink plugin common parameters, please refer to [Sink Common Options](common-options.md) for details

## Example

```bash
mongodb {
    uri = "mongodb://username:password@127.0.0.1:27017/mypost?retryWrites=true&writeConcern=majority"
    database = "mydatabase"
    collection = "mycollection"
}
```

## Changelog

### 2.2.0-beta 2022-09-26

- Add MongoDB Sink Connector
