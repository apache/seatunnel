## Input plugin : MongoDB

* Author: InterestingLab
* Homepage: https://interestinglab.github.io/waterdrop
* Version: 1.0.0

### Description

从MongoDB中获取数据

### Options

| name | type | required | default value |
| --- | --- | --- | --- |
| [readConfig.uri](#readConfig.uri-string) | string | yes | - |
| [readConfig.database](#readConfig.database-string) | string | yes | - |
| [readConfig.collection](#readConfig.collection-string) | string | yes | - |
| [table_name](#table_name-string) | string | yes | - |


##### readConfig.uri [string]

要读取mongoDB的uri

##### readConfig.database [string]

要读取mongoDB的database

##### readConfig.collection [string]

要读取mongoDB的collection

#### readConfig.[xxx]

这里还可以配置更多，详见https://docs.mongodb.com/spark-connector/v1.1/configuration/

##### table_name [string]

从mongoDB获取到的数据，注册成临时表的表名



### Example

```
mongodb{
        readConfig.uri="mongodb://myhost:mypost"
        readConfig.database="mydatabase"
        readConfig.collection="mycollection"
        readConfig.spark.mongodb.input.partitioner = "MongoPaginateBySizePartitioner"
        table_name = "test"
      }
```
