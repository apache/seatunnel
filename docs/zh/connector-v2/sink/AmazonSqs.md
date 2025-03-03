# AmazonSqs

> Amazon SQS 接收器连接器

## 支持以下引擎

> Spark<br/>
> Flink<br/>
> SeaTunnel Zeta<br/>

## 描述

将数据写入 Amazon SQS

## 关键特性

- [x] [批处理](../../concept/connector-v2-features.md)
- [x] [流处理](../../concept/connector-v2-features.md)
- [x] [精确一次](../../concept/connector-v2-features.md)
- [ ] [列映射](../../concept/connector-v2-features.md)
- [ ] [并行性](../../concept/connector-v2-features.md)
- [ ] [支持用户定义的拆分](../../concept/connector-v2-features.md)

## 参数和选项

|          名称           |  类型  | 必需 | 默认值 |                                                                                                                                                                                                             Description                                                                                                                                                                                                             |
|-------------------------|--------|--|---------|-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| url                     | String | 是 | -       | 从Amazon SQS读取的队列URL.                                                                                                                                                                                                                                                                                                                                                                                              |
| region                  | String | 否 | -       | SQS服务的AWS区域                                                                                                                                                                                                                                                                                                                                                                                                  |
| format                  | String | 否 | json    | 数据格式。默认格式为json。可选文本格式，canal json和debezium json。如果你使用json或文本格式。默认字段分隔符为“，”。如果自定义分隔符，请添加“field_delimiter”选项。如果您使用canal格式，请参阅[canal-json]（../formats/canal-json.md）了解详细信息。如果您使用debezium格式，请参阅[debezium json]（../formats/debezium json.md）了解详细信息. |
| format_error_handle_way | String | 否 | fail    | 数据格式错误的处理方法。默认值为fail，可选值为（fail，skip）。当选择失败时，数据格式错误将被阻止，并引发异常。当选择跳过时，数据格式错误将跳过此行数据.                                                                                                                                                              |
| field_delimiter         | String | 否 | ,       | 自定义数据格式的字段分隔符.                                                                                                                                                                                                                                                                                                                                                                                      |

## 任务示例

```bash
source {
  FakeSource {
    schema = {
      fields {
        c_map = "map<string, string>"
        c_array = "array<int>"
        c_string = string
        c_boolean = boolean
        c_tinyint = tinyint
        c_smallint = smallint
        c_int = int
        c_bigint = bigint
        c_float = float
        c_double = double
        c_bytes = bytes
        c_date = date
        c_decimal = "decimal(38, 18)"
        c_timestamp = timestamp
        c_row = {
          c_map = "map<string, string>"
          c_array = "array<int>"
          c_string = string
          c_boolean = boolean
          c_tinyint = tinyint
          c_smallint = smallint
          c_int = int
          c_bigint = bigint
          c_float = float
          c_double = double
          c_bytes = bytes
          c_date = date
          c_decimal = "decimal(38, 18)"
          c_timestamp = timestamp
        }
      }
    }
    plugin_output = "fake"
  }
}

sink {
  AmazonSqs {
    url = "http://127.0.0.1:8000"
    region = "us-east-1"
    queue = "queueName"
    format = text
    field_delimiter = "|"  
  }
}
```

