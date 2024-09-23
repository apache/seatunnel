# S3File

> S3 File Source Connector

## Support Those Engines

> Spark<br/>
> Flink<br/>
> SeaTunnel Zeta<br/>

## Key Features

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
    - [x] xml
    - [x] binary

## Description

Read data from aws s3 file system.

## Supported DataSource Info

| Datasource | Supported versions |
|------------|--------------------|
| S3         | current            |

## Dependency

> If you use spark/flink, In order to use this connector, You must ensure your spark/flink cluster already integrated hadoop. The tested hadoop version is 2.x.<br/>
>
> If you use SeaTunnel Zeta, It automatically integrated the hadoop jar when you download and install SeaTunnel Zeta. You can check the jar package under ${SEATUNNEL_HOME}/lib to confirm this.<br/>
> To use this connector you need put hadoop-aws-3.1.4.jar and aws-java-sdk-bundle-1.12.692.jar in ${SEATUNNEL_HOME}/lib dir.

## Data Type Mapping

Data type mapping is related to the type of file being read, We supported as the following file types:

`text` `csv` `parquet` `orc` `json` `excel` `xml`

### JSON File Type

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

### Text Or CSV File Type

If you assign file type to `text` `csv`, you can choose to specify the schema information or not.

For example, upstream data is the following:

```text

tyrantlucifer#26#male

```

If you do not assign data schema connector will treat the upstream data as the following:

|        content        |
|-----------------------|
| tyrantlucifer#26#male |

If you assign data schema, you should also assign the option `field_delimiter` too except CSV file type

you should assign schema and delimiter as the following:

```hocon

field_delimiter = "#"
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

### Orc File Type

If you assign file type to `parquet` `orc`, schema option not required, connector can find the schema of upstream data automatically.

|          Orc Data type           |                      SeaTunnel Data type                       |
|----------------------------------|----------------------------------------------------------------|
| BOOLEAN                          | BOOLEAN                                                        |
| INT                              | INT                                                            |
| BYTE                             | BYTE                                                           |
| SHORT                            | SHORT                                                          |
| LONG                             | LONG                                                           |
| FLOAT                            | FLOAT                                                          |
| DOUBLE                           | DOUBLE                                                         |
| BINARY                           | BINARY                                                         |
| STRING<br/>VARCHAR<br/>CHAR<br/> | STRING                                                         |
| DATE                             | LOCAL_DATE_TYPE                                                |
| TIMESTAMP                        | LOCAL_DATE_TIME_TYPE                                           |
| DECIMAL                          | DECIMAL                                                        |
| LIST(STRING)                     | STRING_ARRAY_TYPE                                              |
| LIST(BOOLEAN)                    | BOOLEAN_ARRAY_TYPE                                             |
| LIST(TINYINT)                    | BYTE_ARRAY_TYPE                                                |
| LIST(SMALLINT)                   | SHORT_ARRAY_TYPE                                               |
| LIST(INT)                        | INT_ARRAY_TYPE                                                 |
| LIST(BIGINT)                     | LONG_ARRAY_TYPE                                                |
| LIST(FLOAT)                      | FLOAT_ARRAY_TYPE                                               |
| LIST(DOUBLE)                     | DOUBLE_ARRAY_TYPE                                              |
| Map<K,V>                         | MapType, This type of K and V will transform to SeaTunnel type |
| STRUCT                           | SeaTunnelRowType                                               |

### Parquet File Type

If you assign file type to `parquet` `orc`, schema option not required, connector can find the schema of upstream data automatically.

|    Orc Data type     |                      SeaTunnel Data type                       |
|----------------------|----------------------------------------------------------------|
| INT_8                | BYTE                                                           |
| INT_16               | SHORT                                                          |
| DATE                 | DATE                                                           |
| TIMESTAMP_MILLIS     | TIMESTAMP                                                      |
| INT64                | LONG                                                           |
| INT96                | TIMESTAMP                                                      |
| BINARY               | BYTES                                                          |
| FLOAT                | FLOAT                                                          |
| DOUBLE               | DOUBLE                                                         |
| BOOLEAN              | BOOLEAN                                                        |
| FIXED_LEN_BYTE_ARRAY | TIMESTAMP<br/> DECIMAL                                         |
| DECIMAL              | DECIMAL                                                        |
| LIST(STRING)         | STRING_ARRAY_TYPE                                              |
| LIST(BOOLEAN)        | BOOLEAN_ARRAY_TYPE                                             |
| LIST(TINYINT)        | BYTE_ARRAY_TYPE                                                |
| LIST(SMALLINT)       | SHORT_ARRAY_TYPE                                               |
| LIST(INT)            | INT_ARRAY_TYPE                                                 |
| LIST(BIGINT)         | LONG_ARRAY_TYPE                                                |
| LIST(FLOAT)          | FLOAT_ARRAY_TYPE                                               |
| LIST(DOUBLE)         | DOUBLE_ARRAY_TYPE                                              |
| Map<K,V>             | MapType, This type of K and V will transform to SeaTunnel type |
| STRUCT               | SeaTunnelRowType                                               |

## Options

|              name               |  type   | required |                     default value                     | Description                                                                                                                                                                                                                                                                                                                                                                                                |
|---------------------------------|---------|----------|-------------------------------------------------------|------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| path                            | string  | yes      | -                                                     | The s3 path that needs to be read can have sub paths, but the sub paths need to meet certain format requirements. Specific requirements can be referred to "parse_partition_from_path" option                                                                                                                                                                                                              |
| file_format_type                | string  | yes      | -                                                     | File type, supported as the following file types: `text` `csv` `parquet` `orc` `json` `excel` `xml` `binary`                                                                                                                                                                                                                                                                                               |
| bucket                          | string  | yes      | -                                                     | The bucket address of s3 file system, for example: `s3n://seatunnel-test`, if you use `s3a` protocol, this parameter should be `s3a://seatunnel-test`.                                                                                                                                                                                                                                                     |
| fs.s3a.endpoint                 | string  | yes      | -                                                     | fs s3a endpoint                                                                                                                                                                                                                                                                                                                                                                                            |
| fs.s3a.aws.credentials.provider | string  | yes      | com.amazonaws.auth.InstanceProfileCredentialsProvider | The way to authenticate s3a. We only support `org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider` and `com.amazonaws.auth.InstanceProfileCredentialsProvider` now. More information about the credential provider you can see [Hadoop AWS Document](https://hadoop.apache.org/docs/stable/hadoop-aws/tools/hadoop-aws/index.html#Simple_name.2Fsecret_credentials_with_SimpleAWSCredentialsProvider.2A) |
| read_columns                    | list    | no       | -                                                     | The read column list of the data source, user can use it to implement field projection. The file type supported column projection as the following shown: `text` `csv` `parquet` `orc` `json` `excel` `xml` . If the user wants to use this feature when reading `text` `json` `csv` files, the "schema" option must be configured.                                                                        |
| access_key                      | string  | no       | -                                                     | Only used when `fs.s3a.aws.credentials.provider = org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider `                                                                                                                                                                                                                                                                                                  |
| access_secret                   | string  | no       | -                                                     | Only used when `fs.s3a.aws.credentials.provider = org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider `                                                                                                                                                                                                                                                                                                  |
| hadoop_s3_properties            | map     | no       | -                                                     | If you need to add other option, you could add it here and refer to this [link](https://hadoop.apache.org/docs/stable/hadoop-aws/tools/hadoop-aws/index.html)                                                                                                                                                                                                                                              |
| delimiter/field_delimiter       | string  | no       | \001                                                  | Field delimiter, used to tell connector how to slice and dice fields when reading text files. Default `\001`, the same as hive's default delimiter.                                                                                                                                                                                                                                                        |
| parse_partition_from_path       | boolean | no       | true                                                  | Control whether parse the partition keys and values from file path. For example if you read a file from path `s3n://hadoop-cluster/tmp/seatunnel/parquet/name=tyrantlucifer/age=26`. Every record data from file will be added these two fields: name="tyrantlucifer", age=16                                                                                                                              |
| date_format                     | string  | no       | yyyy-MM-dd                                            | Date type format, used to tell connector how to convert string to date, supported as the following formats:`yyyy-MM-dd` `yyyy.MM.dd` `yyyy/MM/dd`. default `yyyy-MM-dd`                                                                                                                                                                                                                                    |
| datetime_format                 | string  | no       | yyyy-MM-dd HH:mm:ss                                   | Datetime type format, used to tell connector how to convert string to datetime, supported as the following formats:`yyyy-MM-dd HH:mm:ss` `yyyy.MM.dd HH:mm:ss` `yyyy/MM/dd HH:mm:ss` `yyyyMMddHHmmss`                                                                                                                                                                                                      |
| time_format                     | string  | no       | HH:mm:ss                                              | Time type format, used to tell connector how to convert string to time, supported as the following formats:`HH:mm:ss` `HH:mm:ss.SSS`                                                                                                                                                                                                                                                                       |
| skip_header_row_number          | long    | no       | 0                                                     | Skip the first few lines, but only for the txt and csv. For example, set like following:`skip_header_row_number = 2`. Then SeaTunnel will skip the first 2 lines from source files                                                                                                                                                                                                                         |
| schema                          | config  | no       | -                                                     | The schema of upstream data.                                                                                                                                                                                                                                                                                                                                                                               |
| sheet_name                      | string  | no       | -                                                     | Reader the sheet of the workbook,Only used when file_format is excel.                                                                                                                                                                                                                                                                                                                                      |
| xml_row_tag                     | string  | no       | -                                                     | Specifies the tag name of the data rows within the XML file, only valid for XML files.                                                                                                                                                                                                                                                                                                                     |
| xml_use_attr_format             | boolean | no       | -                                                     | Specifies whether to process data using the tag attribute format, only valid for XML files.                                                                                                                                                                                                                                                                                                                |
| compress_codec                  | string  | no       | none                                                  |                                                                                                                                                                                                                                                                                                                                                                                                            |
| archive_compress_codec          | string  | no       | none                                                  |                                                                                                                                                                                                                                                                                                                                                                                                            |
| encoding                        | string  | no       | UTF-8                                                 |                                                                                                                                                                                                                                                                                                                                                                                                            |
| common-options                  |         | no       | -                                                     | Source plugin common parameters, please refer to [Source Common Options](../source-common-options.md) for details.                                                                                                                                                                                                                                                                                         |

### delimiter/field_delimiter [string]

**delimiter** parameter will deprecate after version 2.3.5, please use **field_delimiter** instead.

### compress_codec [string]

The compress codec of files and the details that supported as the following shown:

- txt: `lzo` `none`
- json: `lzo` `none`
- csv: `lzo` `none`
- orc/parquet:  
  automatically recognizes the compression type, no additional settings required.

### archive_compress_codec [string]

The compress codec of archive files and the details that supported as the following shown:

| archive_compress_codec | file_format        | archive_compress_suffix |
|------------------------|--------------------|-------------------------|
| ZIP                    | txt,json,excel,xml | .zip                    |
| TAR                    | txt,json,excel,xml | .tar                    |
| TAR_GZ                 | txt,json,excel,xml | .tar.gz                 |
| NONE                   | all                | .*                      |

### encoding [string]

Only used when file_format_type is json,text,csv,xml.
The encoding of the file to read. This param will be parsed by `Charset.forName(encoding)`.

## Example

1. In this example, We read data from s3 path `s3a://seatunnel-test/seatunnel/text` and the file type is orc in this path.
   We use `org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider` to authentication so `access_key` and `secret_key` is required.
   All columns in the file will be read and send to sink.

```
# Defining the runtime environment
env {
  parallelism = 1
  job.mode = "BATCH"
}

source {
  S3File {
    path = "/seatunnel/text"
    fs.s3a.endpoint="s3.cn-north-1.amazonaws.com.cn"
    fs.s3a.aws.credentials.provider = "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider"
    access_key = "xxxxxxxxxxxxxxxxx"
    secret_key = "xxxxxxxxxxxxxxxxx"
    bucket = "s3a://seatunnel-test"
    file_format_type = "orc"
  }
}

transform {
  # If you would like to get more information about how to configure seatunnel and see full list of transform plugins,
    # please go to https://seatunnel.apache.org/docs/category/transform-v2
}

sink {
  Console {}
}
```

2. Use `InstanceProfileCredentialsProvider` to authentication
   The file type in S3 is json, so need config schema option.

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

3. Use `InstanceProfileCredentialsProvider` to authentication
   The file type in S3 is json and has five fields (`id`, `name`, `age`, `sex`, `type`), so need config schema option.
   In this job, we only need send `id` and `name` column to mysql.

```
# Defining the runtime environment
env {
  parallelism = 1
  job.mode = "BATCH"
}

source {
  S3File {
    path = "/seatunnel/json"
    bucket = "s3a://seatunnel-test"
    fs.s3a.endpoint="s3.cn-north-1.amazonaws.com.cn"
    fs.s3a.aws.credentials.provider="com.amazonaws.auth.InstanceProfileCredentialsProvider"
    file_format_type = "json"
    read_columns = ["id", "name"]
    schema {
      fields {
        id = int 
        name = string
        age = int
        sex = int
        type = string
      }
    }
  }
}

transform {
  # If you would like to get more information about how to configure seatunnel and see full list of transform plugins,
    # please go to https://seatunnel.apache.org/docs/category/transform-v2
}

sink {
  Console {}
}
```

## Changelog

### 2.3.0-beta 2022-10-20

- Add S3File Source Connector

### Next version

- [Feature] Support S3A protocol ([3632](https://github.com/apache/seatunnel/pull/3632))
    - Allow user to add additional hadoop-s3 parameters
    - Allow the use of the s3a protocol
    - Decouple hadoop-aws dependencies
- [Feature]Set S3 AK to optional ([3688](https://github.com/apache/seatunnel/pull/))

