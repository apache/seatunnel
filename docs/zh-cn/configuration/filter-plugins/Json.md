## Filter plugin : Sql

* Author: InterestingLab
* Homepage: https://interestinglab.github.io/waterdrop
* Version: 1.0.0

### Description

对原始数据集指定字段进行Json解析

### Options

| name | type | required | default value |
| --- | --- | --- | --- |
| [source_field](#source_field-string) | string | no | raw_message |
| [target_field](#target_field-string) | string | no | replaced |

##### source_field [string]

源字段，若不配置默认为`raw_message`

##### target_field [string]

目标字段，若不配置默认为`__ROOT__`，Json解析后的结果将统一放置Dataframe最顶层

### Examples

1. 不使用`target_field`

    ```
    Json {
        source_field = "message"
    }
    ```

   #### Input

    ```
    +---------------------------+
    |message                    |
    +---------------------------+
    |{"name": "ricky", "age": 24}|
    |{"name": "gary", "age": 28}|
    +---------------------------+
    ```

   #### Output

    ```
    +----------------------------+---+-----+
    |message                     |age|name |
    +----------------------------+---+-----+
    |{"name": "gary", "age": 28} |28 |gary |
    |{"name": "ricky", "age": 23}|23 |ricky|
    +----------------------------+---+-----+
    ```

1. 使用`target_field`

    ```
    Json {
        source_field = "message"
        target_field = "info"
    }
    ```

   #### Input

    ```
    +---------------------------+
    |message                    |
    +---------------------------+
    |{"name": "ricky", "age": 24}|
    |{"name": "gary", "age": 28}|
    +---------------------------+
    ```

   #### Output

    ```
    +----------------------------+----------+
    |message                     |info      |
    +----------------------------+----------+
    |{"name": "gary", "age": 28} |[28,gary] |
    |{"name": "ricky", "age": 23}|[23,ricky]|
    +----------------------------+----------+

    ```

    > Json处理的结果支持**select * from where info.age = 27**此类SQL语句
