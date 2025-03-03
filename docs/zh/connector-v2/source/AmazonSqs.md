# AmazonSqs

> AmazonSqs 源连接器

## 支持一下引擎

> Spark<br/>
> Flink<br/>
> SeaTunnel Zeta<br/>

## 关键特性

- [x] [批处理](../../concept/connector-v2-features.md)
- [x] [流处理](../../concept/connector-v2-features.md)
- [ ] [精确一次](../../concept/connector-v2-features.md)
- [ ] [列投影](../../concept/connector-v2-features.md)
- [ ] [并行度](../../concept/connector-v2-features.md)
- [ ] [支持用户自定义分片](../../concept/connector-v2-features.md)

## 描述

从 Amazon SQS 读取数据.

## 源选项

|          名称           |  类型  | 必需 | 默认值 | 描述                                                                                                                                                                                                                                      |
|-------------------------|--------|----|---------|-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| url                     | String | 是  | -       | 从 Amazon SQ S读取的队列 URL.                                                                                                                                                                                                                 |
| region                  | String | 否  | -       | SQS 服务的 AWS 分区                                                                                                                                                                                                                          |
| schema                  | Config | 否 | -       | 数据的结构，包括字段名和字段类型.                                                                                                                                                                                                                       |
| format                  | String | 否 | json    | 数据格式。默认格式为json。可选文本格式，canal-json和debezium-json。如果你使用json或text格式。默认字段分隔符为 ", ". 如果自定义分隔符，请添加"field_delimiter"选项。如果使用 canal 格式,详见[canal-json](../formats/canal-json.md).如果使用 debezium 格式,详见[debezium-json](../formats/debezium-json.md).. |
| format_error_handle_way | String | 否 | fail    | 数据格式错误的处理方法. 默认值为fail，可选值为（fail，skip）. 当选择失败时，数据格式错误将被阻止，并引发异常. 当选择跳过时，数据格式错误将跳过此行数据.                                                                                                                                                   |
| field_delimiter         | String | 否 | ,       | 自定义数据格式的字段分隔符.                                                                                                                                                                                                                          |
| common-options          |        | 否 | -       | 源插件常用参数, 详见 [源通用选项](../source-common-options.md)                                                                                                                                                           |

## 任务示例

```bash
source {
  AmazonSqs {
    url = "http://127.0.0.1:4566"
    region = "us-east-1"
    format = text
    field_delimiter = "#"
    schema = {
      fields {
        artist = string
        c_map = "map<string, array<int>>"
        c_array = "array<int>"
        c_string = string
        c_boolean = boolean
        c_tinyint = tinyint
        c_smallint = smallint
        c_int = int
        c_bigint = bigint
        c_float = float
        c_double = double
        c_decimal = "decimal(30, 8)"
        c_null = "null"
        c_bytes = bytes
        c_date = date
        c_timestamp = timestamp
      }
    }
  }
}

transform {
    # 如果你想了解更多关于如何配置seatunnel的信息，并查看转换插件的完整列表,
    # 请前往 https://seatunnel.apache.org/docs/transform-v2/sql
}

sink {
    Console {}
}
```

## 变更日志

### 下一个版本

