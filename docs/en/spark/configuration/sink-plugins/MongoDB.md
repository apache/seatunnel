# Sink plugin : MongoDB [Spark]

## Description

Write data to `MongoDB`

## Options

| name                   | type   | required | default value |
| ---------------------- | ------ | -------- | ------------- |
| writeconfig.uri        | string | yes      | -             |
| writeconfig.database   | string | yes      | -             |
| writeconfig.collection | string | yes      | -             |
| common-options         | string | no       |               |

### writeconfig.uri [string]

uri to write to mongoDB

### writeconfig.database [string]

database to write to mongoDB

### readconfig.collection [string]

collection to write to mongoDB

### common options [string]

Sink plugin common parameters, please refer to [Sink Plugin](./sink-plugin.md) for details

## Examples

```bash
mongodb {
    writeconfig.uri = "mongodb://username:password@127.0.0.1:27017/test_db"
    writeconfig.database = "test_db"
    writeconfig.collection = "test_collection"
    result_table_name = "mongodb_result_table"
}
```
