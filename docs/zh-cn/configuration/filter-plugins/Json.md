## Filter plugin : Json

* Author: InterestingLab
* Homepage: https://interestinglab.github.io/waterdrop
* Version: 1.0.0

### Description

对原始数据集指定字段进行Json解析

### Options

| name | type | required | default value |
| --- | --- | --- | --- |
| [source_field](#source_field-string) | string | no | raw_message |
| [target_field](#target_field-string) | string | no | \_\_root\_\_ |
| [schema_dir](#schema_dir-string) | string | no | - |
| [schema_file](#schema_file-string) | string | no | - |
| [common-options](#common-options-string)| string | no | - |


##### source_field [string]

源字段，若不配置默认为`raw_message`

##### target_field [string]

目标字段，若不配置默认为`__root__`，Json解析后的结果将统一放置Dataframe最顶层

##### schema_dir [string]

样式目录，若不配置默认为`$WaterdropRoot/plugins/json/files/schemas/`

##### schema_file [string]

样式文件名，若不配置默认为空，即不指定结构，由系统根据数据源输入自行推导。

##### common options [string]

`Filter` 插件通用参数，详情参照 [Filter Plugin](/zh-cn/configuration/filter-plugin)


### Use cases

1. `json schema` **使用场景**

单个任务的数据源中可能包含不同样式的 json 数据，比如来自 kafka 的 topicA 样式为

```json
{
  "A": "a_val",
  "B": "b_val"
}
```

来自 topicB 样式为

```json
{
  "C": "c_val",
  "D": "d_val"
}
```

运行 filter 时需要将 topicA 和 topicB 的数据融合在一张宽表中进行计算。则可指定一份 schema，其内容样式为：
```json
{
  "A": "a_val",
  "B": "b_val",
  "C": "c_val",
  "D": "d_val"
}
```

则 topicA 和 topicB 的融合输出结果为：

```
+-----+-----+-----+-----+
|A    |B    |C    |D    |
+-----+-----+-----+-----+
|a_val|b_val|null |null |
|null |null |c_val|d_val|
+-----+-----+-----+-----+
```

### Examples

1. 不使用`target_field`

    ```
    json {
        source_field = "message"
    }
    ```

    * **Input**

    ```
    +----------------------------+
    |message                   |
    +----------------------------+
    |{"name": "ricky", "age": 24}|
    |{"name": "gary", "age": 28} |
    +----------------------------+
    ```

    * **Output**

    ```
    +----------------------------+---+-----+
    |message                   |age|name |
    +----------------------------+---+-----+
    |{"name": "gary", "age": 28} |28 |gary |
    |{"name": "ricky", "age": 23}|23 |ricky|
    +----------------------------+---+-----+
    ```

2. 使用`target_field`

    ```
    json {
        source_field = "message"
        target_field = "info"
    }
    ```

    * **Input**

    ```
    +----------------------------+
    |message                   |
    +----------------------------+
    |{"name": "ricky", "age": 24}|
    |{"name": "gary", "age": 28} |
    +----------------------------+
    ```

    * **Output**

    ```
    +----------------------------+----------+
    |message                   |info      |
    +----------------------------+----------+
    |{"name": "gary", "age": 28} |[28,gary] |
    |{"name": "ricky", "age": 23}|[23,ricky]|
    +----------------------------+----------+

    ```

    > json处理的结果支持**select * from where info.age = 23**此类SQL语句

3. 使用`schema_file`
    ```
    json {
        source_field = "message"
        schema_file = "demo.json"
    }
    ```
    
    * **Schema**
    
    在 Driver Node 的 `/opt/waterdrop/plugins/json/files/schemas/demo.json` 中放置内容如下：
    
    ```json
    {
       "name": "demo",
       "age": 24,
       "city": "LA"
    }
    ```
    
    * **Input**
    ```
    +----------------------------+
    |message                   |
    +----------------------------+
    |{"name": "ricky", "age": 24}|
    |{"name": "gary", "age": 28} |
    +----------------------------+
    ```
    
    * **Output**

    ```
    +----------------------------+---+-----+-----+
    |message                     |age|name |city |
    +----------------------------+---+-----+-----+
    |{"name": "gary", "age": 28} |28 |gary |null |
    |{"name": "ricky", "age": 23}|23 |ricky|null |
    +----------------------------+---+-----+-----+
    ```

    > 若使用 cluster 模式进行部署，需确保 json schemas 目录被打包到 plugins.tar.gz 中