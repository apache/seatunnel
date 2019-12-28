## Input plugin : MongoDB

* Author: InterestingLab
* Homepage: https://interestinglab.github.io/waterdrop
* Version: 1.1.2

### Description

从[MongoDB](https://www.mongodb.com/)读取数据

### Options

| name | type | required | default value |
| --- | --- | --- | --- |
| [readconfig.uri](#readconfig.uri-string) | string | yes | - |
| [readconfig.database](#readconfig.database-string) | string | yes | - |
| [readconfig.collection](#readconfig.collection-string) | string | yes | - |
| [readconfig.*](#readconfig.*-string) | string | no | - |
| [common-options](#common-options-string)| string | yes | - |


##### readconfig.uri [string]

要读取mongoDB的uri

##### readconfig.database [string]

要读取mongoDB的database

##### readconfig.collection [string]

要读取mongoDB的collection

#### readconfig

这里还可以配置更多其他参数，详见https://docs.mongodb.com/spark-connector/v1.1/configuration/, 参见其中的`Input Configuration`部分
指定参数的方式是在原参数名称上加上前缀"readconfig." 如设置`spark.mongodb.input.partitioner`的方式是 `readconfig.spark.mongodb.input.partitioner="MongoPaginateBySizePartitioner"`。如果不指定这些非必须参数，将使用MongoDB官方文档的默认值

##### common options [string]

`Input` 插件通用参数，详情参照 [Input Plugin](/zh-cn/configuration/input-plugin)



### Example

```
mongodb{
        readconfig.uri="mongodb://myhost:mypost"
        readconfig.database="mydatabase"
        readconfig.collection="mycollection"
        readconfig.spark.mongodb.input.partitioner = "MongoPaginateBySizePartitioner"
        result_table_name = "test"
      }
```
