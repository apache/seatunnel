# SftpFile

> Sftp file source connector

## Support Those Engines

> Spark<br/>
> Flink<br/>
> SeaTunnel Zeta<br/>

## Key Features

- [x] [batch](../../concept/connector-v2-features.md)
- [ ] [stream](../../concept/connector-v2-features.md)
- [ ] [exactly-once](../../concept/connector-v2-features.md)
- [x] [column projection](../../concept/connector-v2-features.md)
- [x] [parallelism](../../concept/connector-v2-features.md)
- [ ] [support user-defined split](../../concept/connector-v2-features.md)
- [x] file format type
  - [x] text
  - [x] csv
  - [x] json
  - [x] excel
  - [x] xml
  - [x] binary

## Description

Read data from sftp file server.

## Supported DataSource Info

In order to use the SftpFile connector, the following dependencies are required.
They can be downloaded via install-plugin.sh or from the Maven central repository.

| Datasource | Supported Versions |                                       Dependency                                        |
|------------|--------------------|-----------------------------------------------------------------------------------------|
| SftpFile   | universal          | [Download](https://mvnrepository.com/artifact/org.apache.seatunnel/connector-file-sftp) |

:::tip

If you use spark/flink, In order to use this connector, You must ensure your spark/flink cluster already integrated hadoop. The tested hadoop version is 2.x.

If you use SeaTunnel Engine, It automatically integrated the hadoop jar when you download and install SeaTunnel Engine. You can check the jar package under ${SEATUNNEL_HOME}/lib to confirm this.

We made some trade-offs in order to support more file types, so we used the HDFS protocol for internal access to Sftp and this connector need some hadoop dependencies.
It only supports hadoop version **2.9.X+**.

:::

## Data Type Mapping

The File does not have a specific type list, and we can indicate which SeaTunnel data type the corresponding data needs to be converted to by specifying the Schema in the config.

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

|           Name            |  Type   | Required |    default value    |                                                                                                                                                                                   Description                                                                                                                                                                                   |
|---------------------------|---------|----------|---------------------|---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| host                      | String  | Yes      | -                   | The target sftp host is required                                                                                                                                                                                                                                                                                                                                                |
| port                      | Int     | Yes      | -                   | The target sftp port is required                                                                                                                                                                                                                                                                                                                                                |
| user                      | String  | Yes      | -                   | The target sftp username is required                                                                                                                                                                                                                                                                                                                                            |
| password                  | String  | Yes      | -                   | The target sftp password is required                                                                                                                                                                                                                                                                                                                                            |
| path                      | String  | Yes      | -                   | The source file path.                                                                                                                                                                                                                                                                                                                                                           |
| file_format_type          | String  | Yes      | -                   | Please check #file_format_type below                                                                                                                                                                                                                                                                                                                                            |
| file_filter_pattern       | String  | No       | -                   | Filter pattern, which used for filtering files.                                                                                                                                                                                                                                                                                                                                 |
| delimiter/field_delimiter | String  | No       | \001                | **delimiter** parameter will deprecate after version 2.3.5, please use **field_delimiter** instead. <br/> Field delimiter, used to tell connector how to slice and dice fields when reading text files. <br/> Default `\001`, the same as hive's default delimiter                                                                                                              |
| parse_partition_from_path | Boolean | No       | true                | Control whether parse the partition keys and values from file path <br/> For example if you read a file from path `oss://hadoop-cluster/tmp/seatunnel/parquet/name=tyrantlucifer/age=26` <br/> Every record data from file will be added these two fields: <br/>      name       age  <br/> tyrantlucifer  26   <br/> Tips: **Do not define partition fields in schema option** |
| date_format               | String  | No       | yyyy-MM-dd          | Date type format, used to tell connector how to convert string to date, supported as the following formats: <br/> `yyyy-MM-dd` `yyyy.MM.dd` `yyyy/MM/dd` <br/> default `yyyy-MM-dd`                                                                                                                                                                                             |
| datetime_format           | String  | No       | yyyy-MM-dd HH:mm:ss | Datetime type format, used to tell connector how to convert string to datetime, supported as the following formats: <br/> `yyyy-MM-dd HH:mm:ss` `yyyy.MM.dd HH:mm:ss` `yyyy/MM/dd HH:mm:ss` `yyyyMMddHHmmss` <br/> default `yyyy-MM-dd HH:mm:ss`                                                                                                                                |
| time_format               | String  | No       | HH:mm:ss            | Time type format, used to tell connector how to convert string to time, supported as the following formats: <br/> `HH:mm:ss` `HH:mm:ss.SSS` <br/> default `HH:mm:ss`                                                                                                                                                                                                            |
| skip_header_row_number    | Long    | No       | 0                   | Skip the first few lines, but only for the txt and csv. <br/> For example, set like following: <br/> `skip_header_row_number = 2` <br/> then SeaTunnel will skip the first 2 lines from source files                                                                                                                                                                            |
| read_columns              | list    | no       | -                   | The read column list of the data source, user can use it to implement field projection.                                                                                                                                                                                                                                                                                         |
| sheet_name                | String  | No       | -                   | Reader the sheet of the workbook,Only used when file_format is excel.                                                                                                                                                                                                                                                                                                           |
| xml_row_tag               | string  | no       | -                   | Specifies the tag name of the data rows within the XML file, only used when file_format is xml.                                                                                                                                                                                                                                                                                 |
| xml_use_attr_format       | boolean | no       | -                   | Specifies whether to process data using the tag attribute format, only used when file_format is xml.                                                                                                                                                                                                                                                                            |
| schema                    | Config  | No       | -                   | Please check #schema below                                                                                                                                                                                                                                                                                                                                                      |
| compress_codec            | String  | No       | None                | The compress codec of files and the details that supported as the following shown: <br/> - txt: `lzo` `None` <br/> - json: `lzo` `None` <br/> - csv: `lzo` `None` <br/> - orc: `lzo` `snappy` `lz4` `zlib` `None` <br/> - parquet: `lzo` `snappy` `lz4` `gzip` `brotli` `zstd` `None` <br/> Tips: excel type does Not support any compression format                            |
| archive_compress_codec    | string  | no       | none                |
| encoding                  | string  | no       | UTF-8               |
| common-options            |         | No       | -                   | Source plugin common parameters, please refer to [Source Common Options](../source-common-options.md) for details.                                                                                                                                                                                                                                                              |

### file_format_type [string]

File type, supported as the following file types:
`text` `csv` `parquet` `orc` `json` `excel` `xml` `binary`
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

If you assign file type to `binary`, SeaTunnel can synchronize files in any format,
such as compressed packages, pictures, etc. In short, any files can be synchronized to the target place.
Under this requirement, you need to ensure that the source and sink use `binary` format for file synchronization
at the same time.

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

### schema [config]

#### fields [Config]

The schema of upstream data.

## How to Create a Sftp Data Synchronization Jobs

The following example demonstrates how to create a data synchronization job that reads data from sftp and prints it on the local client:

```bash
# Set the basic configuration of the task to be performed
env {
  parallelism = 1
  job.mode = "BATCH"
}

# Create a source to connect to sftp
source {
  SftpFile {
    host = "sftp"
    port = 22
    user = seatunnel
    password = pass
    path = "tmp/seatunnel/read/json"
    file_format_type = "json"
    result_table_name = "sftp"
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
}

# Console printing of the read sftp data
sink {
  Console {
    parallelism = 1
  }
}
```

