## Output plugin : MongoDB

* Author: InterestingLab
* Homepage: https://interestinglab.github.io/waterdrop
* Version: 1.1.2

### Description

写入数据到[MongoDB](https://www.mongodb.com/)

### Options

| name | type | required | default value | engine |
| --- | --- | --- | --- |--- |
| [writeconfig.uri](#writeconfig.uri-string) | string | yes | - | spark streaming |
| [writeconfig.database](#writeconfig.database-string) | string | yes | - | all streaming |
| [writeconfig.collection](#writeconfig.collection-string) | string | yes | - | all streaming |
| [writeconfig.*](#writeconfig.*-string) | string | no | - | spark streaming |
| [writeconfig.host](#writeconfig.port-integer) | string | yes | - | structured streaming |
| [writeconfig.port](#writeconfig.port-integer) | integer | no | 27017 | structured streaming |
| [update_fields](#update_fields-string) | string | no | - | structured streaming |
| [mongo_output_mode](#mongo_output_mode-string) | string | no | insert | structured streaming |
| [streaming_output_mode](#streaming_output_mode-string) | string | no | append | structured streaming |
| [checkpointLocation](#checkpointLocation-string) | string | no | - | structured streaming |
| [common-options](#common-options-string)| string | no | - |


##### writeconfig.uri [string]

要写入mongoDB的uri

##### writeconfig.database [string]

要写入mongoDB的database

##### writeconfig.collection [string]

要写入mongoDB的collection

##### writeconfig

这里还可以配置更多其他参数，详见https://docs.mongodb.com/spark-connector/v1.1/configuration/
, 参见其中的`Output Configuration`部分
指定参数的方式是在原参数名称上加上前缀"writeconfig." 如设置`localThreshold`的方式是 `writeconfig.localThreshold=20`。如果不指定这些非必须参数，将使用MongoDB官方文档的默认值


#### Notes
在作为structured streaming 的output的时候，你可以添加一些额外的参数，来达到相应的效果

##### writeconfig.port [integer]
如果你的mongoDB 的端口不是默认的27017，你可以手动指定

##### mongo_output_mode [string]
写入mongo中采取的模式,支持 insert|updateOne|updateMany|upsert|replace,默认为insert 

##### update_fields [string]
当你指定的模式是更新或者是替代的是，你需要指定根据哪些字段去更新。根据多个字段更新字段用逗号隔开，例如根据学号和姓名字段更新则：update_fields = "id,name"

##### checkpointLocation [string]
你可以指定是否启用checkpoint，通过配置**checkpointLocation**这个参数

##### streaming_output_mode [string]
你可以指定输出模式，complete|append|update三种，详见Spark文档http://spark.apache.org/docs/latest/structured-streaming-programming-guide.html#output-modes

##### common options [string]

`Output` 插件通用参数，详情参照 [Output Plugin](/zh-cn/configuration/output-plugin)

> spark streaming or batch

```
mongodb{
    writeconfig.uri="mongodb://myhost:mypost"
    writeconfig.database="mydatabase"
    writeconfig.collection="mycollection"
}
```

> structured streaming

```
mongodb{
    writeconfig.host="my host"
    writeconfig.port=27017
    writeconfig.database="mydatabase"
    writeconfig.collection="mycollection"
    mongo_output_mode = "updateOne"
    update_fields = "id,name"
    streaming_output_mode = "update"
    checkpointLocation = "/your/path"
}
```