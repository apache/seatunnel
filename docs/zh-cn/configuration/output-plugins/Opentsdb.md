## Output plugin : Opentsdb

* Author: InterestingLab
* Homepage: https://interestinglab.github.io/waterdrop
* Version: 1.3.0

### Description

输出数据到Opentsdb

### Options

| name | type | required | default value | engine |
| --- | --- | --- | --- | --- |
| [postUrl](#postUrl-string) | string | yes | - | structured streaming |
| [metric](#metric-string) | string | yes | - | structured streaming |
| [tags_fields](#tags_fields-array) | array | no | - | structured streaming |
| [value_fields](#value_fields-array) | array | yes | - | structured streaming |
| [timestamp](#user-string) | string | yes | - | structured streaming |
| [streaming_output_mode](#streaming_output_mode-string) | string | no | append | structured streaming |
| [trigger_type](#streaming_output_mode-string) | string | no | default | structured streaming |
| [interval](#interval-string)| string | no | - | structured streaming |
| [common-options](#common-options-string)| string | no | - | structured streaming |


##### postUrl [string]

输出到Opentsdb的http请求地址，示例：`http://localhost:4222/api/put?summary`

##### metric [string]

Opentsdb对应的metric，需要提前在服务器创建完成

##### tags_fields [array]

tags对象中包含的信息，会按照配置信息（K）和原始数据（v）形成 "K" -> V键值对

##### value_fields [string]

Opentsdb的Value信息，会根据配置信息将原始数据变成多行Opentsdb支持的数据行，转化规则见*示例说明*

##### timestamp [string]

时间戳字段

##### streaming_output_mode [string]

输出模式，支持 `Append` 、`Update` 或 `Complete`。


##### trigger_type

Trigger Type， 支持default、ProcessingTime、OneTime 和 Continuous

##### interval [string]

Trigger触发周期。 当 trigger_type 为ProcessingTime 或 Continuous 时配置。

##### common options [string]

`Output` 插件通用参数，详情参照 [Output Plugin](/zh-cn/configuration/output-plugin)

### Example

```
opentsdb{
    postUrl = "http://localhost:4222/api/put?summary"
    metric = "test_metric"
    tags_fields = ["col1","col2","col3"]
    measures = ["men1","men2"]
    value_fields = "timestamps"
  }
```

### 示例说明

##### Schema信息

Schema[(col1,DataType), (col2,DataType), (col3,DataType), (col4,DataType), (men1,LongType), (men2,LongType), (men3,LongType), (time,TimeStamp)]

##### 原始数据

Row("v1", "v2", "v3", "v4", 123, 22, 33, 1553081227)

##### 转换规则

1. 首先根据tags_fields，生成一个tags对象

```
{
    "col1": "v1",
    "col2" : "v2",
    "col3" : "v3"
}
```

2. 根据value_fields，生成2行数据，并更新tags字段(默认生成一个key为st_group的字段)
```
{
    "metric": "test_metric",
    "timestamp": 1553081227,
    "value": 123
    "tags": {
        "col1" : "v1",
        "col2" : "v2",
        "col3" : "v3",
        "st_group": "men1"
    }
}
```

3. 根据第二步一样生成第二行数据， 其中变化为

```
{
    ...
    "tags":{
        "st_group":"men2"
    }
}
```