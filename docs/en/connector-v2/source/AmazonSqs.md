# AmazonSqs

> AmazonSqs source connector

## Support Those Engines

> Spark<br/>
> Flink<br/>
> SeaTunnel Zeta<br/>

## Key Features

- [x] [batch](../../concept/connector-v2-features.md)
- [x] [stream](../../concept/connector-v2-features.md)
- [ ] [exactly-once](../../concept/connector-v2-features.md)
- [ ] [column projection](../../concept/connector-v2-features.md)
- [ ] [parallelism](../../concept/connector-v2-features.md)
- [ ] [support user-defined split](../../concept/connector-v2-features.md)

## Description

Read data from Amazon SQS.

## Source Options

|          Name           |  Type  | Required | Default |                                                                                                                                                                                                             Description                                                                                                                                                                                                             |
|-------------------------|--------|----------|---------|-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| url                     | String | Yes      | -       | The Queue URL to read from Amazon SQS.                                                                                                                                                                                                                                                                                                                                                                                              |
| region                  | String | No       | -       | The AWS region for the SQS service                                                                                                                                                                                                                                                                                                                                                                                                  |
| schema                  | Config | No       | -       | The structure of the data, including field names and field types.                                                                                                                                                                                                                                                                                                                                                                   |
| format                  | String | No       | json    | Data format. The default format is json. Optional text format, canal-json and debezium-json.If you use json or text format. The default field separator is ", ". If you customize the delimiter, add the "field_delimiter" option.If you use canal format, please refer to [canal-json](../formats/canal-json.md) for details.If you use debezium format, please refer to [debezium-json](../formats/debezium-json.md) for details. |
| format_error_handle_way | String | No       | fail    | The processing method of data format error. The default value is fail, and the optional value is (fail, skip). When fail is selected, data format error will block and an exception will be thrown. When skip is selected, data format error will skip this line data.                                                                                                                                                              |
| field_delimiter         | String | No       | ,       | Customize the field delimiter for data format.                                                                                                                                                                                                                                                                                                                                                                                      |
| common-options          |        | No       | -       | Source plugin common parameters, please refer to [Source Common Options](common-options.md) for details                                                                                                                                                                                                                                                                                                                             |

## Task Example

```bash
source {
  amazonsqs {
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
    # If you would like to get more information about how to configure seatunnel and see full list of transform plugins,
    # please go to https://seatunnel.apache.org/docs/transform-v2/sql
}

sink {
    Console {}
}
```

## Changelog

### next version

