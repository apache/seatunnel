## Source plugin : FakeStream [Spark]

* Author: InterestingLab
* Homepage: https://interestinglab.github.io/waterdrop
* Version: 2.0.0

### Description

`FakeStream` 主要用于方便得生成用户指定的数据，作为输入来对Waterdrop进行功能验证，测试，以及性能测试等。


### Options

| name | type | required | default value |
| --- | --- | --- | --- |
| [content](#content-array) | array | no | - |
| [rate](#rate-number) | number | yes | - |
| [common-options](#common-options-string)| string | yes | - |


##### content [array]

测试数据字符串列表

##### rate [number]

每秒生成测试用例个数

##### common options [string]

`Source` 插件通用参数，详情参照 [Source Plugin](/zh-cn/v2/spark/configuration/source-plugins/)



### Examples

```
fakeStream {
    content = ['name=ricky&age=23', 'name=gary&age=28']
    rate = 5
}
```
生成的数据如下，从`content`列表中随机抽取其中的字符串

```
+-----------------+
|raw_message      |
+-----------------+
|name=gary&age=28 |
|name=ricky&age=23|
+-----------------+
```
