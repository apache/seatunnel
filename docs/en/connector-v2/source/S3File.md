# S3File

> S3 file source connector

## Description

Read data from aws s3 file system.

:::tip

If you use spark/flink, In order to use this connector, You must ensure your spark/flink cluster already integrated hadoop. The tested hadoop version is 2.x.

If you use SeaTunnel Engine, It automatically integrated the hadoop jar when you download and install SeaTunnel Engine. You can check the jar package under ${SEATUNNEL_HOME}/lib to confirm this.

To use this connector you need put hadoop-aws-3.1.4.jar and aws-java-sdk-bundle-1.11.271.jar in ${SEATUNNEL_HOME}/lib dir.

:::

## Key features

- [x] [batch](../../concept/connector-v2-features.md)
- [ ] [stream](../../concept/connector-v2-features.md)
- [x] [exactly-once](../../concept/connector-v2-features.md)

Read all the data in a split in a pollNext call. What splits are read will be saved in snapshot.

- [x] [column projection](../../concept/connector-v2-features.md)
- [x] [parallelism](../../concept/connector-v2-features.md)
- [ ] [support user-defined split](../../concept/connector-v2-features.md)
- [x] file format type
  - [x] text
  - [x] csv
  - [x] parquet
  - [x] orc
  - [x] json
  - [x] excel

## Options

|              name               |  type   | required |                     default value                     |
|---------------------------------|---------|----------|-------------------------------------------------------|
| path                            | string  | yes      | -                                                     |
| file_format_type                | string  | yes      | -                                                     |
| bucket                          | string  | yes      | -                                                     |
| fs.s3a.endpoint                 | string  | yes      | -                                                     |
| fs.s3a.aws.credentials.provider | string  | yes      | com.amazonaws.auth.InstanceProfileCredentialsProvider |
| read_columns                    | list    | no       | -                                                     |
| access_key                      | string  | no       | -                                                     |
| access_secret                   | string  | no       | -                                                     |
| hadoop_s3_properties            | map     | no       | -                                                     |
| delimiter                       | string  | no       | \001                                                  |
| parse_partition_from_path       | boolean | no       | true                                                  |
| date_format                     | string  | no       | yyyy-MM-dd                                            |
| datetime_format                 | string  | no       | yyyy-MM-dd HH:mm:ss                                   |
| time_format                     | string  | no       | HH:mm:ss                                              |
| skip_header_row_number          | long    | no       | 0                                                     |
| schema                          | config  | no       | -                                                     |
| common-options                  |         | no       | -                                                     |
| sheet_name                      | string  | no       | -                                                     |
| file_filter_pattern             | string  | no       | -                                                     |

### path [string]

The source file path.

### fs.s3a.endpoint [string]

fs s3a endpoint

### fs.s3a.aws.credentials.provider [string]

The way to authenticate s3a. We only support `org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider` and `com.amazonaws.auth.InstanceProfileCredentialsProvider` now.

More information about the credential provider you can see [Hadoop AWS Document](https://hadoop.apache.org/docs/stable/hadoop-aws/tools/hadoop-aws/index.html#Simple_name.2Fsecret_credentials_with_SimpleAWSCredentialsProvider.2A)

### delimiter [string]

Field delimiter, used to tell connector how to slice and dice fields when reading text files

default `\001`, the same as hive's default delimiter

### parse_partition_from_path [boolean]

Control whether parse the partition keys and values from file path

For example if you read a file from path `s3n://hadoop-cluster/tmp/seatunnel/parquet/name=tyrantlucifer/age=26`

Every record data from file will be added these two fields:

|     name      | age |
|---------------|-----|
| tyrantlucifer | 26  |

Tips: **Do not define partition fields in schema option**

### date_format [string]

Date type format, used to tell connector how to convert string to date, supported as the following formats:

`yyyy-MM-dd` `yyyy.MM.dd` `yyyy/MM/dd`

default `yyyy-MM-dd`

### datetime_format [string]

Datetime type format, used to tell connector how to convert string to datetime, supported as the following formats:

`yyyy-MM-dd HH:mm:ss` `yyyy.MM.dd HH:mm:ss` `yyyy/MM/dd HH:mm:ss` `yyyyMMddHHmmss`

default `yyyy-MM-dd HH:mm:ss`

### time_format [string]

Time type format, used to tell connector how to convert string to time, supported as the following formats:

`HH:mm:ss` `HH:mm:ss.SSS`

default `HH:mm:ss`

### skip_header_row_number [long]

Skip the first few lines, but only for the txt and csv.

For example, set like following:

`skip_header_row_number = 2`

then SeaTunnel will skip the first 2 lines from source files

### file_format_type [string]

File type, supported as the following file types:

`text` `csv` `parquet` `orc` `json` `excel`

If you assign file type to `json`, you should also assign schema option to tell connector how to parse data to the row you want.

For example:

upstream data is the following:

```json

{"code":  200, "data":  "get success", "success":  true}

```

You can also save multiple pieces of data in one file and split them by newline:

```json lines

{"code":  200, "data":  "get success", "success":  true}
{"code":  300, "data":  "get failed", "success":  false}

```

you should assign schema as the following:

```hocon

schema {
    fields {
        code = int
        data = string
        success = boolean
    }
}

```

connector will generate data as the following:

| code |    data     | success |
|------|-------------|---------|
| 200  | get success | true    |

If you assign file type to `parquet` `orc`, schema option not required, connector can find the schema of upstream data automatically.

If you assign file type to `text` `csv`, you can choose to specify the schema information or not.

For example, upstream data is the following:

```text

tyrantlucifer#26#male

```

If you do not assign data schema connector will treat the upstream data as the following:

|        content        |
|-----------------------|
| tyrantlucifer#26#male |

If you assign data schema, you should also assign the option `delimiter` too except CSV file type

you should assign schema and delimiter as the following:

```hocon

delimiter = "#"
schema {
    fields {
        name = string
        age = int
        gender = string 
    }
}

```

connector will generate data as the following:

|     name      | age | gender |
|---------------|-----|--------|
| tyrantlucifer | 26  | male   |

### bucket [string]

The bucket address of s3 file system, for example: `s3n://seatunnel-test`, if you use `s3a` protocol, this parameter should be `s3a://seatunnel-test`.

### access_key [string]

The access key of s3 file system. If this parameter is not set, please confirm that the credential provider chain can be authenticated correctly, you could check this [hadoop-aws](https://hadoop.apache.org/docs/stable/hadoop-aws/tools/hadoop-aws/index.html)

### access_secret [string]

The access secret of s3 file system. If this parameter is not set, please confirm that the credential provider chain can be authenticated correctly, you could check this [hadoop-aws](https://hadoop.apache.org/docs/stable/hadoop-aws/tools/hadoop-aws/index.html)

### hadoop_s3_properties [map]

If you need to add a other option, you could add it here and refer to this [hadoop-aws](https://hadoop.apache.org/docs/stable/hadoop-aws/tools/hadoop-aws/index.html)

```
hadoop_s3_properties {
      "xxx" = "xxx"
   }
```

### schema [config]

#### fields [Config]

The schema of upstream data.

### read_columns [list]

The read column list of the data source, user can use it to implement field projection.

The file type supported column projection as the following shown:

- text
- json
- csv
- orc
- parquet
- excel

**Tips: If the user wants to use this feature when reading `text` `json` `csv` files, the schema option must be configured**

### common options

Source plugin common parameters, please refer to [Source Common Options](common-options.md) for details.

### sheet_name [string]

Reader the sheet of the workbook,Only used when file_format_type is excel.

## Example

```hocon

  S3File {
    path = "/seatunnel/text"
    fs.s3a.endpoint="s3.cn-north-1.amazonaws.com.cn"
    fs.s3a.aws.credentials.provider = "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider"
    access_key = "xxxxxxxxxxxxxxxxx"
    secret_key = "xxxxxxxxxxxxxxxxx"
    bucket = "s3a://seatunnel-test"
    file_format_type = "orc"
  }

```

```hocon

  S3File {
    path = "/seatunnel/json"
    bucket = "s3a://seatunnel-test"
    fs.s3a.endpoint="s3.cn-north-1.amazonaws.com.cn"
    fs.s3a.aws.credentials.provider="com.amazonaws.auth.InstanceProfileCredentialsProvider"
    file_format_type = "json"
    schema {
      fields {
        id = int 
        name = string
      }
    }
  }

```

### file_filter_pattern [string]

Filter pattern, which used for filtering files.

## Changelog

### 2.3.0-beta 2022-10-20

- Add S3File Source Connector

### Next version

- [Feature] Support S3A protocol ([3632](https://github.com/apache/seatunnel/pull/3632))
  - Allow user to add additional hadoop-s3 parameters
  - Allow the use of the s3a protocol
  - Decouple hadoop-aws dependencies
- [Feature]Set S3 AK to optional ([3688](https://github.com/apache/seatunnel/pull/))

