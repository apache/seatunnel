# OssFile

> Oss file source connector

## Support Those Engines

> Spark<br/>
> Flink<br/>
> SeaTunnel Zeta<br/>

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

## Description

Read data from aliyun oss file system.

## Supported DataSource Info

In order to use the OssFile connector, the following dependencies are required.
They can be downloaded via install-plugin.sh or from the Maven central repository.

| Datasource | Supported Versions |                                       Dependency                                       |
|------------|--------------------|----------------------------------------------------------------------------------------|
| OssFile    | universal          | [Download](https://mvnrepository.com/artifact/org.apache.seatunnel/connector-file-oss) |

:::tip

If you use spark/flink, In order to use this connector, You must ensure your spark/flink cluster already integrated hadoop. The tested hadoop version is 2.x.

If you use SeaTunnel Engine, It automatically integrated the hadoop jar when you download and install SeaTunnel Engine. You can check the jar package under ${SEATUNNEL_HOME}/lib to confirm this.

We made some trade-offs in order to support more file types, so we used the HDFS protocol for internal access to OSS and this connector need some hadoop dependencies.
It only supports hadoop version **2.9.X+**.

:::

## Data Type Mapping

The File does not have a specific type list, and we can indicate which SeaTunenl data type the corresponding data needs to be converted to by specifying the Schema in the config.

| SeaTunnel Data type |
|---------------------|
| STRING              |
| SHORT               |
| INT                 |
| BIGINT              |
| BOOLEAN             |
| DOUBLE              |
| DECIMAL             |
| FLOAT               |
| DATE                |
| TIME                |
| TIMESTAMP           |
| BYTES               |
| ARRAY               |
| MAP                 |

## Source Options

|           Name            |  Type   | Required |    default value    |                                                                                                                                                                                    Description                                                                                                                                                                                     |
|---------------------------|---------|----------|---------------------|------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| path                      | String  | Yes      | -                   | The source file path.                                                                                                                                                                                                                                                                                                                                                              |
| file_format_type          | String  | Yes      | -                   | Please check #file_format_type below                                                                                                                                                                                                                                                                                                                                               |
| bucket                    | String  | Yes      | -                   | The bucket address of oss file system, for example: `oss://tyrantlucifer-image-bed`                                                                                                                                                                                                                                                                                                |
| endpoint                  | String  | Yes      | -                   | The endpoint of oss file system.                                                                                                                                                                                                                                                                                                                                                   |
| read_columns              | List    | No       | -                   | The read column list of the data source, user can use it to implement field projection. <br/> The file type supported column projection as the following shown: <br/> - text <br/> - json <br/> - csv <br/> - orc <br/> - parquet <br/> - excel <br/> **Tips: If the user wants to use this feature when reading `text` `json` `csv` files, the schema option must be configured** |
| access_key                | String  | No       | -                   | The access key of oss file system.                                                                                                                                                                                                                                                                                                                                                 |
| access_secret             | String  | No       | -                   | The access secret of oss file system.                                                                                                                                                                                                                                                                                                                                              |
| file_filter_pattern       | String  | No       | -                   | Filter pattern, which used for filtering files.                                                                                                                                                                                                                                                                                                                                    |
| delimiter/field_delimiter | String  | No       | \001                | **delimiter** parameter will deprecate after version 2.3.5, please use **field_delimiter** instead. <br/> Field delimiter, used to tell connector how to slice and dice fields when reading text files. <br/> Default `\001`, the same as hive's default delimiter                                                                                                                 |
| parse_partition_from_path | Boolean | No       | true                | Control whether parse the partition keys and values from file path <br/> For example if you read a file from path `oss://hadoop-cluster/tmp/seatunnel/parquet/name=tyrantlucifer/age=26` <br/> Every record data from file will be added these two fields: <br/>      name       age  <br/> tyrantlucifer  26   <br/> Tips: **Do not define partition fields in schema option**    |
| date_format               | String  | No       | yyyy-MM-dd          | Date type format, used to tell connector how to convert string to date, supported as the following formats: <br/> `yyyy-MM-dd` `yyyy.MM.dd` `yyyy/MM/dd` <br/> default `yyyy-MM-dd`                                                                                                                                                                                                |
| datetime_format           | String  | No       | yyyy-MM-dd HH:mm:ss | Datetime type format, used to tell connector how to convert string to datetime, supported as the following formats: <br/> `yyyy-MM-dd HH:mm:ss` `yyyy.MM.dd HH:mm:ss` `yyyy/MM/dd HH:mm:ss` `yyyyMMddHHmmss` <br/> default `yyyy-MM-dd HH:mm:ss`                                                                                                                                   |
| time_format               | String  | No       | HH:mm:ss            | Time type format, used to tell connector how to convert string to time, supported as the following formats: <br/> `HH:mm:ss` `HH:mm:ss.SSS` <br/> default `HH:mm:ss`                                                                                                                                                                                                               |
| skip_header_row_number    | Long    | No       | 0                   | Skip the first few lines, but only for the txt and csv. <br/> For example, set like following: <br/> `skip_header_row_number = 2` <br/> then SeaTunnel will skip the first 2 lines from source files                                                                                                                                                                               |
| sheet_name                | String  | No       | -                   | Reader the sheet of the workbook,Only used when file_format is excel.                                                                                                                                                                                                                                                                                                              |
| schema                    | Config  | No       | -                   | Please check #schema below                                                                                                                                                                                                                                                                                                                                                         |
| file_filter_pattern       | string  | no       | -                   | Filter pattern, which used for filtering files.                                                                                                                                                                                                                                                                                                                                    |
| compress_codec            | string  | no       | none                | The compress codec of files and the details that supported as the following shown: <br/> - txt: `lzo` `none` <br/> - json: `lzo` `none` <br/> - csv: `lzo` `none` <br/> - orc/parquet: automatically recognizes the compression type, no additional settings required.                                                                                                             |
| common-options            |         | No       | -                   | Source plugin common parameters, please refer to [Source Common Options](common-options.md) for details.                                                                                                                                                                                                                                                                           |

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

### schema [config]

Only need to be configured when the file_format_type are text, json, excel or csv ( Or other format we can't read the schema from metadata).

#### fields [Config]

The schema of upstream data.

## How to Create a Oss Data Synchronization Jobs

The following example demonstrates how to create a data synchronization job that reads data from Oss and prints it on the local client:

```bash
# Set the basic configuration of the task to be performed
env {
  execution.parallelism = 1
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
    parallelism = 1
  }
}
```

```bash
# Set the basic configuration of the task to be performed
env {
  execution.parallelism = 1
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
    parallelism = 1
  }
}
```

### Tips

> 1.[SeaTunnel Deployment Document](../../start-v2/locally/deployment.md).

