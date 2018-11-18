## Output plugin : MongoDB

* Author: InterestingLab
* Homepage: https://interestinglab.github.io/waterdrop
* Version: 1.0.0

### Description

写入数据到[MongoDB](https://www.mongodb.com/)

### Options

| name | type | required | default value |
| --- | --- | --- | --- |
| [writeconfig.uri](#writeconfig.uri-string) | string | yes | - |
| [writeconfig.database](#writeconfig.database-string) | string | yes | - |
| [writeconfig.collection](#writeconfig.collection-string) | string | yes | - |
| [writeconfig.*](#writeconfig.*-string) | string | no | - |



##### writeconfig.uri [string]

要写入mongoDB的uri

##### writeconfig.database [string]

要写入mongoDB的database

##### writeconfig.collection [string]

要写入mongoDB的collection

#### writeconfig

这里还可以配置更多其他参数，详见https://docs.mongodb.com/spark-connector/v1.1/configuration/
, 参见其中的`Output Configuration`部分
指定参数的方式是在原参数名称上加上前缀"writeconfig." 如设置`localThreshold`的方式是 `writeconfig.localThreshold=20`。如果不指定这些非必须参数，将使用MongoDB官方文档的默认值


### Example

```
mongodb{
        writeconfig.uri="mongodb://myhost:mypost"
        writeconfig.database="mydatabase"
        writeconfig.collection="mycollection"
      }
```
