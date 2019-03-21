## Output plugin : Opentsdb

* Author: InterestingLab
* Homepage: https://interestinglab.github.io/waterdrop
* Version: 1.0.0

### Description

输出数据到Opentsdb

### Options

| name | type | required | default value |
| --- | --- | --- | --- |
| [postUrl](#postUrl-string) | string | yes | - |
| [metric](#metric-string) | string | yes | - |
| [tags_fields](#tags_fields-array) | array | no | - |
| [value_fields](#value_fields-array) | array | yes | - |
| [timestamp](#user-string) | string | yes | - |


##### postUrl [string]

输出到Opentsdb的http请求地址，示例：`http://localhost:4222/api/put?summary`

##### metric [string]

Opentsdb对应的metric，需要提前在服务器创建完成

##### tags_fields [array]

tags对象中包含的信息，会按照配置信息（K）和原始数据（v）形成 "K" -> V键值对

##### value_fields [string]

Opentsdb的Value信息，会根据配置信息将原始数据变成多行Opentsdb支持的数据行，转化规则见*示例说明*


##### timestamp [string]````

时间戳字段


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
Schema[(col1,DataType),(col2,DataType),(col3,DataType),(col4,DataType)(men1,LongType),(men2,LongType),(men3,LongType),(time,TimeStamp)]

##### 原始数据
Row("v1","v2","v3","v4",123,22,33,1553081227)

##### 转换规则

&emsp; 1.首先根据tags_fields，生成一个tags对象 

&emsp; {

&emsp; &emsp;      "col1" : "v1",

&emsp; &emsp;      "col2" : "v2",

&emsp; &emsp;      "col3" : "v3"

&emsp; }

&emsp; 2.根据value_fields，生成2行数据，并更新tags字段(默认生成一个key为st_group的字段)

&emsp;{

&emsp;&emsp;  "metric":"test_metric",

&emsp;&emsp;  "timestamp":1553081227,

&emsp;&emsp;  "value":123

&emsp;&emsp;"tags":{

&emsp;&emsp;&emsp;     "col1" : "v1",

&emsp;&emsp;&emsp;     "col2" : "v2",

&emsp;&emsp;&emsp;     "col3" : "v3",

&emsp;&emsp;&emsp;     "st_group":"men1"

&emsp;}


&emsp; 3.根据第二步一样生成第二行数据， 其中变化为

&emsp;{

&emsp;&emsp; ...

&emsp;&emsp;"tags":{

&emsp;&emsp;&emsp;     "st_group":"men2"

&emsp;}
