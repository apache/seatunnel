# OssFile

> Oss file source connector

## Support Those Engines

> Spark<br/>
> Flink<br/>
> SeaTunnel Zeta<br/>

## Usage Dependency

### For Spark/Flink Engine

1. You must ensure your spark/flink cluster already integrated hadoop. The tested hadoop version is 2.x.
2. You must ensure `hadoop-aliyun-xx.jar`, `aliyun-sdk-oss-xx.jar` and `jdom-xx.jar` in `${SEATUNNEL_HOME}/plugins/` dir and the version of `hadoop-aliyun` jar need equals your hadoop version which used in spark/flink and `aliyun-sdk-oss-xx.jar` and `jdom-xx.jar` version needs to be the version corresponding to the `hadoop-aliyun` version. Eg: `hadoop-aliyun-3.1.4.jar` dependency `aliyun-sdk-oss-3.4.1.jar` and `jdom-1.1.jar`.

### For SeaTunnel Zeta Engine

1. You must ensure `seatunnel-hadoop3-3.1.4-uber.jar`, `aliyun-sdk-oss-3.4.1.jar`, `hadoop-aliyun-3.1.4.jar` and `jdom-1.1.jar` in `${SEATUNNEL_HOME}/lib/` dir.

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
  - [x] xml
  - [x] binary

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

|           name            |  type   | required |    default value    |                                                                                                                                                             Description                                                                                                                                                             |
|---------------------------|---------|----------|---------------------|-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| path                      | string  | yes      | -                   | The Oss path that needs to be read can have sub paths, but the sub paths need to meet certain format requirements. Specific requirements can be referred to "parse_partition_from_path" option                                                                                                                                      |
| file_format_type          | string  | yes      | -                   | File type, supported as the following file types: `text` `csv` `parquet` `orc` `json` `excel` `xml` `binary`                                                                                                                                                                                                                        |
| bucket                    | string  | yes      | -                   | The bucket address of oss file system, for example: `oss://seatunnel-test`.                                                                                                                                                                                                                                                         |
| endpoint                  | string  | yes      | -                   | fs oss endpoint                                                                                                                                                                                                                                                                                                                     |
| read_columns              | list    | no       | -                   | The read column list of the data source, user can use it to implement field projection. The file type supported column projection as the following shown: `text` `csv` `parquet` `orc` `json` `excel` `xml` . If the user wants to use this feature when reading `text` `json` `csv` files, the "schema" option must be configured. |
| access_key                | string  | no       | -                   |                                                                                                                                                                                                                                                                                                                                     |
| access_secret             | string  | no       | -                   |                                                                                                                                                                                                                                                                                                                                     |
| delimiter                 | string  | no       | \001                | Field delimiter, used to tell connector how to slice and dice fields when reading text files. Default `\001`, the same as hive's default delimiter.                                                                                                                                                                                 |
| parse_partition_from_path | boolean | no       | true                | Control whether parse the partition keys and values from file path. For example if you read a file from path `oss://hadoop-cluster/tmp/seatunnel/parquet/name=tyrantlucifer/age=26`. Every record data from file will be added these two fields: name="tyrantlucifer", age=16                                                       |
| date_format               | string  | no       | yyyy-MM-dd          | Date type format, used to tell connector how to convert string to date, supported as the following formats:`yyyy-MM-dd` `yyyy.MM.dd` `yyyy/MM/dd`. default `yyyy-MM-dd`                                                                                                                                                             |
| datetime_format           | string  | no       | yyyy-MM-dd HH:mm:ss | Datetime type format, used to tell connector how to convert string to datetime, supported as the following formats:`yyyy-MM-dd HH:mm:ss` `yyyy.MM.dd HH:mm:ss` `yyyy/MM/dd HH:mm:ss` `yyyyMMddHHmmss`                                                                                                                               |
| time_format               | string  | no       | HH:mm:ss            | Time type format, used to tell connector how to convert string to time, supported as the following formats:`HH:mm:ss` `HH:mm:ss.SSS`                                                                                                                                                                                                |
| skip_header_row_number    | long    | no       | 0                   | Skip the first few lines, but only for the txt and csv. For example, set like following:`skip_header_row_number = 2`. Then SeaTunnel will skip the first 2 lines from source files                                                                                                                                                  |
| schema                    | config  | no       | -                   | The schema of upstream data.                                                                                                                                                                                                                                                                                                        |
| sheet_name                | string  | no       | -                   | Reader the sheet of the workbook,Only used when file_format is excel.                                                                                                                                                                                                                                                               |
| xml_row_tag               | string  | no       | -                   | Specifies the tag name of the data rows within the XML file, only used when file_format is xml.                                                                                                                                                                                                                                     |
| xml_use_attr_format       | boolean | no       | -                   | Specifies whether to process data using the tag attribute format, only used when file_format is xml.                                                                                                                                                                                                                                |
| compress_codec            | string  | no       | none                | Which compress codec the files used.                                                                                                                                                                                                                                                                                                |
| encoding                  | string  | no       | UTF-8               |
| file_filter_pattern       | string  | no       |                     | `*.txt` means you only need read the files end with `.txt`                                                                                                                                                                                                                                                                          |
| common-options            | config  | no       | -                   | Source plugin common parameters, please refer to [Source Common Options](../source-common-options.md) for details.                                                                                                                                                                                                                  |

### compress_codec [string]

The compress codec of files and the details that supported as the following shown:

- txt: `lzo` `none`
- json: `lzo` `none`
- csv: `lzo` `none`
- orc/parquet:  
  automatically recognizes the compression type, no additional settings required.

### encoding [string]

Only used when file_format_type is json,text,csv,xml.
The encoding of the file to read. This param will be parsed by `Charset.forName(encoding)`.

### file_filter_pattern [string]

Filter pattern, which used for filtering files.

### schema [config]

Only need to be configured when the file_format_type are text, json, excel, xml or csv ( Or other format we can't read the schema from metadata).

#### fields [Config]

The schema of upstream data.

## How to Create a Oss Data Synchronization Jobs

The following example demonstrates how to create a data synchronization job that reads data from Oss and prints it on the local client:

```bash
# Set the basic configuration of the task to be performed
env {
  parallelism = 1
  job.mode = "BATCH"
}

# Create a source to connect to Oss
source {
  OssFile {
    path = "/seatunnel/orc"
    bucket = "oss://tyrantlucifer-image-bed"
    access_key = "xxxxxxxxxxxxxxxxx"
    access_secret = "xxxxxxxxxxxxxxxxxxxxxx"
    endpoint = "oss-cn-beijing.aliyuncs.com"
    file_format_type = "orc"
  }
}

# Console printing of the read Oss data
sink {
  Console {
  }
}
```

```bash
# Set the basic configuration of the task to be performed
env {
  parallelism = 1
  job.mode = "BATCH"
}

# Create a source to connect to Oss
source {
  OssFile {
    path = "/seatunnel/json"
    bucket = "oss://tyrantlucifer-image-bed"
    access_key = "xxxxxxxxxxxxxxxxx"
    access_secret = "xxxxxxxxxxxxxxxxxxxxxx"
    endpoint = "oss-cn-beijing.aliyuncs.com"
    file_format_type = "json"
    schema {
      fields {
        id = int 
        name = string
      }
    }
  }
}

# Console printing of the read Oss data
sink {
  Console {
  }
}
```

### Multiple Table

No need to config schema file type, eg: `orc`.

```
env {
  parallelism = 1
  spark.app.name = "SeaTunnel"
  spark.executor.instances = 2
  spark.executor.cores = 1
  spark.executor.memory = "1g"
  spark.master = local
  job.mode = "BATCH"
}

source {
  OssFile {
    tables_configs = [
      {
          schema = {
              table = "fake01"
          }
          bucket = "oss://whale-ops"
          access_key = "xxxxxxxxxxxxxxxxxxx"
          access_secret = "xxxxxxxxxxxxxxxxxxx"
          endpoint = "https://oss-accelerate.aliyuncs.com"
          path = "/test/seatunnel/read/orc"
          file_format_type = "orc"
      },
      {
          schema = {
              table = "fake02"
          }
          bucket = "oss://whale-ops"
          access_key = "xxxxxxxxxxxxxxxxxxx"
          access_secret = "xxxxxxxxxxxxxxxxxxx"
          endpoint = "https://oss-accelerate.aliyuncs.com"
          path = "/test/seatunnel/read/orc"
          file_format_type = "orc"
      }
    ]
    result_table_name = "fake"
  }
}

sink {
  Assert {
    rules {
        table-names = ["fake01", "fake02"]
    }
  }
}
```

Need config schema file type, eg: `json`

```

env {
  execution.parallelism = 1
  spark.app.name = "SeaTunnel"
  spark.executor.instances = 2
  spark.executor.cores = 1
  spark.executor.memory = "1g"
  spark.master = local
  job.mode = "BATCH"
}

source {
  OssFile {
    tables_configs = [
      {
          bucket = "oss://whale-ops"
          access_key = "xxxxxxxxxxxxxxxxxxx"
          access_secret = "xxxxxxxxxxxxxxxxxxx"
          endpoint = "https://oss-accelerate.aliyuncs.com"
          path = "/test/seatunnel/read/json"
          file_format_type = "json"
          schema = {
            table = "fake01"
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
                C_MAP = "map<string, string>"
                C_ARRAY = "array<int>"
                C_STRING = string
                C_BOOLEAN = boolean
                C_TINYINT = tinyint
                C_SMALLINT = smallint
                C_INT = int
                C_BIGINT = bigint
                C_FLOAT = float
                C_DOUBLE = double
                C_BYTES = bytes
                C_DATE = date
                C_DECIMAL = "decimal(38, 18)"
                C_TIMESTAMP = timestamp
              }
            }
          }
      },
      {
          bucket = "oss://whale-ops"
          access_key = "xxxxxxxxxxxxxxxxxxx"
          access_secret = "xxxxxxxxxxxxxxxxxxx"
          endpoint = "https://oss-accelerate.aliyuncs.com"
          path = "/test/seatunnel/read/json"
          file_format_type = "json"
          schema = {
            table = "fake02"
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
                C_MAP = "map<string, string>"
                C_ARRAY = "array<int>"
                C_STRING = string
                C_BOOLEAN = boolean
                C_TINYINT = tinyint
                C_SMALLINT = smallint
                C_INT = int
                C_BIGINT = bigint
                C_FLOAT = float
                C_DOUBLE = double
                C_BYTES = bytes
                C_DATE = date
                C_DECIMAL = "decimal(38, 18)"
                C_TIMESTAMP = timestamp
              }
            }
          }
      }
    ]
    result_table_name = "fake"
  }
}

sink {
  Assert {
    rules {
      table-names = ["fake01", "fake02"]
    }
  }
}
```

## Changelog

### 2.2.0-beta 2022-09-26

- Add OSS File Source Connector

### 2.3.0-beta 2022-10-20

- [BugFix] Fix the bug of incorrect path in windows environment ([2980](https://github.com/apache/seatunnel/pull/2980))
- [Improve] Support extract partition from SeaTunnelRow fields ([3085](https://github.com/apache/seatunnel/pull/3085))
- [Improve] Support parse field from file path ([2985](https://github.com/apache/seatunnel/pull/2985))

### Tips

> 1.[SeaTunnel Deployment Document](../../start-v2/locally/deployment.md).

