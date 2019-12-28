## Filter plugin : Kv

* Author: InterestingLab
* Homepage: https://interestinglab.github.io/waterdrop
* Version: 1.0.0

### Description

提取指定字段所有的Key-Value, 常用于解析url参数中的key和value

### Options

| name | type | required | default value |
| --- | --- | --- | --- |
| [default_values](#default_values-array) | array | no | [] |
| [exclude_fields](#exclude_fields-array) | array | no | [] |
| [field_prefix](#field_prefix-string) | string | no |  |
| [field_split](#field_split-string) | string | no | & |
| [include_fields](#include_fields-array) | array | no | [] |
| [source_field](#source_field-string) | string | no | raw_message |
| [target_field](#target_field-string) | string | no | \_\_root\_\_ |
| [value_split](#value_split-string) | string | no | = |
| [common-options](#common-options-string)| string | no | - |


##### default_values [array]

指定kv默认值，格式为 `key=defalut_value`, key与value之间使用`=`分割，可以指定多个，举例:

`default_values = ["mykey1=123", "mykey2=waterdrop"]`

##### exclude_fields [array]

不需要包括的字段

##### field_prefix [string]

字段指定前缀

##### field_split [string]

字段分隔符

##### include_fields [array]

需要包括的字段

##### source_field [string]

源字段，若不配置默认为`raw_message`

##### target_field [string]

目标字段，若不配置默认为`__root__`

##### value_split [string]

字段值分隔符

##### common options [string]

`Filter` 插件通用参数，详情参照 [Filter Plugin](/zh-cn/configuration/filter-plugin)


### Examples

1. 使用`target_field`

    ```
    kv {
        source_field = "message"
        target_field = "kv_map"
        field_split = "&"
        value_split = "="
    }
    ```

    * **Input**

    ```
    +-----------------+
    |message         |
    +-----------------+
    |name=ricky&age=23|
    |name=gary&age=28 |
    +-----------------+
    ```

    * **Output**

    ```
    +-----------------+-----------------------------+
    |message          |kv_map                    |
    +-----------------+-----------------------------+
    |name=ricky&age=23|Map(name -> ricky, age -> 23)|
    |name=gary&age=28 |Map(name -> gary, age -> 28) |
    +-----------------+-----------------------------+
    ```

    > kv处理的结果支持**select * from where kv_map.age = 23**此类SQL语句

2. 不使用`target_field`

    ```
    kv {
            source_field = "message"
            field_split = "&"
            value_split = "="
        }
    ```

    * **Input**

    ```
    +-----------------+
    |message         |
    +-----------------+
    |name=ricky&age=23|
    |name=gary&age=28 |
    +-----------------+
    ```

    * **Output**

    ```
    +-----------------+---+-----+
    |message         |age|name |
    +-----------------+---+-----+
    |name=ricky&age=23|23 |ricky|
    |name=gary&age=28 |28 |gary |
    +-----------------+---+-----+

    ```
