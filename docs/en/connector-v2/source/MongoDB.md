# MongoDb

> MongoDb source connector

## Description

Read data from MongoDB.

## Key features

- [x] [batch](../../concept/connector-v2-features.md)
- [ ] [stream](../../concept/connector-v2-features.md)
- [ ] [exactly-once](../../concept/connector-v2-features.md)
- [x] [schema projection](../../concept/connector-v2-features.md)
- [ ] [parallelism](../../concept/connector-v2-features.md)
- [ ] [support user-defined split](../../concept/connector-v2-features.md)

## Options

| name           | type   | required | default value |
|----------------|--------|----------|---------------|
| uri            | string | yes      | -             |
| database       | string | yes      | -             |
| collection     | string | yes      | -             |
| schema         | object | yes      | -             |
| common-options |        | yes      | -             |

### uri [string]

MongoDB uri

### database [string]

MongoDB database

### collection [string]

MongoDB collection

### schema [object]

#### fields [Config]

Because `MongoDB` does not have the concept of `schema`, when engine reads `MongoDB` , it will sample `MongoDB` data and infer the `schema` . In fact, this process will be slow and may be inaccurate. This parameter can be manually specified. Avoid these problems. 

such as:

```
schema {
  fields {
    id = int
    key_aa = string
    key_bb = string
  }
}
```

### common options 

Source Plugin common parameters, refer to [Source Plugin](common-options.md) for details

## Example

```bash
mongodb {
    uri = "mongodb://username:password@127.0.0.1:27017/mypost?retryWrites=true&writeConcern=majority"
    database = "mydatabase"
    collection = "mycollection"
    schema {
      fields {
        id = int
        key_aa = string
        key_bb = string
      }
    }
    result_table_name = "mongodb_result_table"
}
```

## Changelog

### 2.2.0-beta 2022-09-26

- Add MongoDB Source Connector
