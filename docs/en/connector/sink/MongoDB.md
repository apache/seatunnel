# MongoDB

> MongoDB sink connector

## Description

Write data to `MongoDB`

:::tip

Engine Supported and plugin name

* [x] Spark: MongoDB
* [ ] Flink

:::

## Options

| name                   | type   | required | default value |
|------------------------| ------ |----------| ------------- |
| writeconfig.uri        | string | yes      | -             |
| writeconfig.database   | string | yes      | -             |
| writeconfig.collection | string | yes      | -             |
| writeconfig.*          | string | no       | -             |

### writeconfig.uri [string]

uri to write to mongoDB

### writeconfig.database [string]

database to write to mongoDB

### writeconfig.collection [string]

collection to write to mongoDB

### writeconfig.* [string]

More other parameters can be configured here, see [MongoDB Configuration](https://docs.mongodb.com/spark-connector/current/configuration/) for details, see the Output Configuration section. The way to specify parameters is to add a prefix to the original parameter name `writeconfig.` For example, the way to set `localThreshold` is `writeconfig.localThreshold=20` . If you do not specify these optional parameters, the default values of the official MongoDB documentation will be used.

## Examples

```bash
mongodb {
    writeconfig.uri = "mongodb://username:password@127.0.0.1:27017/test_db"
    writeconfig.database = "test_db"
    writeconfig.collection = "test_collection"
}
```
