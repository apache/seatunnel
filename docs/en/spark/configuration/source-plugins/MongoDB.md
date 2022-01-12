# Source plugin: MongoDB [Spark]

## Description

Read data from MongoDB.

## Options

| name           | type   | required | default value |
| -------------- | ------ | -------- | ------------- |
| readconfig.uri            | string | yes      | -             |
| readconfig.database       | string | yes      | -         |
| readconfig.collection     | string | yes      | -             |

### readconfig.uri [string]

MongoDB uri

### readconfig.database [string]

MongoDB database

### readconfig.collection [string]

MongoDB collection

### common options [string]

Source Plugin common parameters, refer to [Source Plugin](./source-plugin.md) for details

## Example

```bash
mongodb {
    readconfig.uri = "mongodb://username:password@192.168.0.1:27017/test"
    readconfig.database = "test"
    readconfig.collection = "collection1"
    result_table_name = "mongodb_result_table"
}
```
