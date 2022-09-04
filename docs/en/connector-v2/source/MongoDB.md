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

| name                  | type   | required | default value |
|-----------------------|--------|----------|---------------|
| readconfig.uri        | string | yes      | -             |
| readconfig.database   | string | yes      | -             |
| readconfig.collection | string | yes      | -             |
| readconfig.*          | string | no       | -             |
| schema                | object | yes      | -             |
| common-options        | string | yes      | -             |

### readconfig.uri [string]

MongoDB uri

### readconfig.database [string]

MongoDB database

### readconfig.collection [string]

MongoDB collection

### readconfig.* [string]

More other parameters can be configured here, see [MongoDB Configuration](https://docs.mongodb.com/spark-connector/current/configuration/) for details, see the Input Configuration section. The way to specify parameters is to prefix the original parameter name `readconfig.` For example, the way to set `spark.mongodb.input.partitioner` is `readconfig.spark.mongodb.input.partitioner="MongoPaginateBySizePartitioner"` . If you do not specify these optional parameters, the default values of the official MongoDB documentation will be used.

### schema [object]

Because `MongoDB` does not have the concept of `schema`, when spark reads `MongoDB` , it will sample `MongoDB` data and infer the `schema` . In fact, this process will be slow and may be inaccurate. This parameter can be manually specified. Avoid these problems. `schema` is a `json` string, 

such as

```
readconfig.schema {
  fields {
    id = int
    key_aa = string
    key_bb = string
  }
}
```

### common options [string]

Source Plugin common parameters, refer to [Source Plugin](common-options.md) for details

## Example

```bash
mongodb {
    readconfig.uri = "mongodb://username:password@127.0.0.1:27017/mypost?retryWrites=true&writeConcern=majority"
    readconfig.database = "mydatabase"
    readconfig.collection = "mycollection"
    readconfig.spark.mongodb.input.partitioner = "MongoPaginateBySizePartitioner"
    readconfig.schema {
      fields {
        id = int
        key_aa = string
        key_bb = string
      }
    }
    result_table_name = "mongodb_result_table"
}
```
