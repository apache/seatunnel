# AmazonSqs

> Amazon SQS sink connector

## Support Those Engines

> Spark<br/>
> Flink<br/>
> SeaTunnel Zeta<br/>

## Description

Write data to Amazon SQS

## Key Features

- [x] [batch](../../concept/connector-v2-features.md)
- [x] [stream](../../concept/connector-v2-features.md)
- [x] [exactly-once](../../concept/connector-v2-features.md)
- [ ] [column projection](../../concept/connector-v2-features.md)
- [ ] [parallelism](../../concept/connector-v2-features.md)
- [ ] [support user-defined split](../../concept/connector-v2-features.md)

## Sink Options

|          Name           |  Type  | Required | Default |                                                                                                                                                                                                             Description                                                                                                                                                                                                             |
|-------------------------|--------|----------|---------|-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| url                     | String | Yes      | -       | The Queue URL to read from Amazon SQS.                                                                                                                                                                                                                                                                                                                                                                                              |
| region                  | String | No       | -       | The AWS region for the SQS service                                                                                                                                                                                                                                                                                                                                                                                                  |
| format                  | String | No       | json    | Data format. The default format is json. Optional text format, canal-json and debezium-json.If you use json or text format. The default field separator is ", ". If you customize the delimiter, add the "field_delimiter" option.If you use canal format, please refer to [canal-json](../formats/canal-json.md) for details.If you use debezium format, please refer to [debezium-json](../formats/debezium-json.md) for details. |
| format_error_handle_way | String | No       | fail    | The processing method of data format error. The default value is fail, and the optional value is (fail, skip). When fail is selected, data format error will block and an exception will be thrown. When skip is selected, data format error will skip this line data.                                                                                                                                                              |
| field_delimiter         | String | No       | ,       | Customize the field delimiter for data format.                                                                                                                                                                                                                                                                                                                                                                                      |

## Task Example

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
    result_table_name = "fake"
  }
}

sink {
  amazonsqs {
    url = "http://127.0.0.1:8000"
    region = "us-east-1"
    queue = "queueName"
    format = text
    field_delimiter = "|"  
  }
}
```

