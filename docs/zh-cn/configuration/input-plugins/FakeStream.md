## Input plugin : FakeStream [Streaming]

* Author: InterestingLab
* Homepage: https://interestinglab.github.io/waterdrop
* Version: 1.0.0

### Description

Fake Input主要用于方便得生成用户指定的数据，作为输入来对Waterdrop进行功能验证，测试，以及性能测试等。

### Options

| name | type | required | default value |
| --- | --- | --- | --- |
| [data_format](#data_format-string) | string | no | text |
| [json_keys](#json_keys-array) | array | no | - |
| [num_of_fields](#num_of_fields-number) | number | no | 10 |
| [rate](#rate-number) | number | yes | - |
| [text_delimeter](#text_delimeter-string) | string | no | , |
| [common-options](#common-options-string)| string | yes | - |


##### data_format [string]

测试数据类型，支持text以及json

##### json_keys [array]

json数据key列表，当`data_format`为json时使用

##### num_of_fields [number]

字段个数，当`data_format`为text时使用

##### rate [number]

每秒生成测试用例个数

##### text_delimeter [string]

文本数据分隔符，当`data_format`为text时使用

##### common options [string]

`Input` 插件通用参数，详情参照 [Input Plugin](/zh-cn/configuration/input-plugin)


### Examples

1. 使用`data_format`

    ```
    fakeStream {
        data_format = "text"
        text_delimeter = ","
        num_of_fields = 5
        rate = 5
    }
    ```

* **Input**

    ```
    +-------------------------------------------------------------------------------------------+
    |raw_message                                                                                |
    +-------------------------------------------------------------------------------------------+
    |Random1-1462437280,Random215896330,Random3-2009195549,Random41027365838,Random51525395111  |
    |Random1-2135047059,Random2-1030689538,Random3-854912064,Random4126768642,Random5-1483841750|
    +-------------------------------------------------------------------------------------------+
    ```


2. 不使用`data_format`

    ```
    fakeStream {
        content = ['name=ricky&age=23', 'name=gary&age=28']
        rate = 5
    }
    ```

* **Input**

    ```
    +-----------------+
    |raw_message      |
    +-----------------+
    |name=gary&age=28 |
    |name=ricky&age=23|
    +-----------------+
    ```

    > 从`content`列表中随机抽取其中的字符串
