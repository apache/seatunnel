## Output plugin : MongoDB

* Author: InterestingLab
* Homepage: https://interestinglab.github.io/waterdrop
* Version: 1.0.0

### Description

写入数据到[MongoDB](https://www.mongodb.com/)

### Options

| name | type | required | default value |
| --- | --- | --- | --- |
| [writeConfig.uri](#writeConfig.uri-string) | string | yes | - |
| [writeConfig.database](#writeConfig.database-string) | string | yes | - |
| [writeConfig.collection](#writeConfig.collection-string) | string | yes | - |



##### writeConfig.uri [string]

要写入mongoDB的uri

##### writeConfig.database [string]

要写入mongoDB的database

##### writeConfig.collection [string]

要写入mongoDB的collection

#### writeConfig.[xxx]

这里还可以配置更多其他参数，详见https://docs.mongodb.com/spark-connector/v1.1/configuration/，参见其中的`Output Configuration`部分


### Example

```
mongodb{
        readConfig.uri="mongodb://myhost:mypost"
        readConfig.database="mydatabase"
        readConfig.collection="mycollection"
      }
```
