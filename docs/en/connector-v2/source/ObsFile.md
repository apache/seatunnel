# ObsFile

> Obs file source connector

## Support those engines

> Spark
>
> Flink
>
> Seatunnel Zeta

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

Read data from huawei cloud obs file system.

If you use spark/flink, In order to use this connector, You must ensure your spark/flink cluster already integrated hadoop. The tested hadoop version is 2.x.

If you use SeaTunnel Engine, It automatically integrated the hadoop jar when you download and install SeaTunnel Engine. You can check the jar package under ${SEATUNNEL_HOME}/lib to confirm this.

We made some trade-offs in order to support more file types, so we used the HDFS protocol for internal access to OBS and this connector need some hadoop dependencies.
It only supports hadoop version **2.9.X+**.

## Required Jar List

|        jar         |     supported versions      |                                                     maven                                                      |
|--------------------|-----------------------------|----------------------------------------------------------------------------------------------------------------|
| hadoop-huaweicloud | support version >= 3.1.1.29 | [Download](https://repo.huaweicloud.com/repository/maven/huaweicloudsdk/org/apache/hadoop/hadoop-huaweicloud/) |
| esdk-obs-java      | support version >= 3.19.7.3 | [Download](https://repo.huaweicloud.com/repository/maven/huaweicloudsdk/com/huawei/storage/esdk-obs-java/)     |
| okhttp             | support version >= 3.11.0   | [Download](https://repo1.maven.org/maven2/com/squareup/okhttp3/okhttp/)                                        |
| okio               | support version >= 1.14.0   | [Download](https://repo1.maven.org/maven2/com/squareup/okio/okio/)                                             |

> Please download the support list corresponding to 'Maven' and copy them to the '$SEATNUNNEL_HOME/plugins/jdbc/lib/' working directory.
>
> And copy all jars to $SEATNUNNEL_HOME/lib/

## Options

|           name            |  type   | required |       default       |                                                 description                                                  |
|---------------------------|---------|----------|---------------------|--------------------------------------------------------------------------------------------------------------|
| path                      | string  | yes      | -                   | The target dir path                                                                                          |
| file_format_type          | string  | yes      | -                   | File type.[Tips](#file_format_type)                                                                          |
| bucket                    | string  | yes      | -                   | The bucket address of obs file system, for example: `obs://obs-bucket-name`                                  |
| access_key                | string  | yes      | -                   | The access key of obs file system                                                                            |
| access_secret             | string  | yes      | -                   | The access secret of obs file system                                                                         |
| endpoint                  | string  | yes      | -                   | The endpoint of obs file system                                                                              |
| read_columns              | list    | yes      | -                   | The read column list of the data source, user can use it to implement field projection.[Tips](#read_columns) |
| delimiter                 | string  | no       | \001                | Field delimiter, used to tell connector how to slice and dice fields when reading text files                 |
| parse_partition_from_path | boolean | no       | true                | Control whether parse the partition keys and values from file path. [Tips](#parse_partition_from_path)       |
| skip_header_row_number    | long    | no       | 0                   | Skip the first few lines, but only for the txt and csv.                                                      |
| date_format               | string  | no       | yyyy-MM-dd          | Date type format, used to tell the connector how to convert string to date.[Tips](#date_format)              |
| datetime_format           | string  | no       | yyyy-MM-dd HH:mm:ss | Datetime type format, used to tell the connector how to convert string to datetime.[Tips](#datetime_format)  |
| time_format               | string  | no       | HH:mm:ss            | Time type format, used to tell the connector how to convert string to time.[Tips](#time_format)              |
| schema                    | config  | no       | -                   | [Tips](#schema)                                                                                              |
| common-options            |         | no       | -                   | [Tips](#common_options)                                                                                      |
| sheet_name                | string  | no       | -                   | Reader the sheet of the workbook,Only used when file_format is excel.                                        |

### Tips

#### <span id="parse_partition_from_path"> parse_partition_from_path </span>

> Control whether parse the partition keys and values from file path
>
> For example if you read a file from path `obs://hadoop-cluster/tmp/seatunnel/parquet/name=tyrantlucifer/age=26`
>
> Every record data from the file will be added these two fields:

|     name      | age |
|---------------|-----|
| tyrantlucifer | 26  |

> Do not define partition fields in schema option

#### <span id="date_format"> date_format </span>

> Date type format, used to tell the connector how to convert string to date, supported as the following formats:
>
> `yyyy-MM-dd` `yyyy.MM.dd` `yyyy/MM/dd`
>
> default `yyyy-MM-dd`

### <span id="datetime_format"> datetime_format </span>

> Datetime type format, used to tell the connector how to convert string to datetime, supported as the following formats:
>
> `yyyy-MM-dd HH:mm:ss` `yyyy.MM.dd HH:mm:ss` `yyyy/MM/dd HH:mm:ss` `yyyyMMddHHmmss`
>
> default `yyyy-MM-dd HH:mm:ss`

### <span id="time_format"> time_format </span>

> Time type format, used to tell the connector how to convert string to time, supported as the following formats:
>
> `HH:mm:ss` `HH:mm:ss.SSS`
>
> default `HH:mm:ss`

### <span id="skip_header_row_number"> skip_header_row_number </span>

> Skip the first few lines, but only for the txt and csv.
>
> For example, set like following:
>
> `skip_header_row_number = 2`
>
> Then Seatunnel will skip the first 2 lines from source files

### <span id="file_format_type"> file_format_type </span>

> File type, supported as the following file types:
>
> `text` `csv` `parquet` `orc` `json` `excel`
>
> If you assign file type to `json`, you should also assign schema option to tell the connector how to parse data to the row you want.
>
> For example,upstream data is the following:
>
> ```json
>
> ```

{"code":  200, "data":  "get success", "success":  true}

```

> You can also save multiple pieces of data in one file and split them by one newline:

```json lines

{"code":  200, "data":  "get success", "success":  true}
{"code":  300, "data":  "get failed", "success":  false}

```

> you should assign schema as the following:

```hocon

schema {
    fields {
        code = int
        data = string
        success = boolean
    }
}

```

> connector will generate data as the following:

| code |    data     | success |
|------|-------------|---------|
| 200  | get success | true    |

> If you assign file type to `parquet` `orc`, schema option not required, connector can find the schema of upstream data automatically.
>
> If you assign file type to `text` `csv`, you can choose to specify the schema information or not.
>
> For example, upstream data is the following:

```text

tyrantlucifer#26#male

```

> If you do not assign data schema connector will treat the upstream data as the following:

|        content        |
|-----------------------|
| tyrantlucifer#26#male |

> If you assign data schema, you should also assign the option `delimiter` too except CSV file type
>
> you should assign schema and delimiter as the following:

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

> connector will generate data as the following:

|     name      | age | gender |
|---------------|-----|--------|
| tyrantlucifer | 26  | male   |

#### <span id="schema"> schema  </span>

##### fields

> The schema of upstream data.

#### <span id="schema"> read_columns </span>

> The read column list of the data source, user can use it to implement field projection.
>
> The file type supported column projection as the following shown:

- text
- json
- csv
- orc
- parquet
- excel

> If the user wants to use this feature when reading `text` `json` `csv` files, the schema option must be configured

#### <span id="common_options "> common options </span>

> Source plugin common parameters, please refer to [Source Common Options](../source-common-options.md) for details.

## Task Example

### text file

> For text file format simple config

```hocon

  ObsFile {
    path = "/seatunnel/text"
    bucket = "obs://obs-bucket-name"
    access_key = "xxxxxxxxxxxxxxxxx"
    access_secret = "xxxxxxxxxxxxxxxxxxxxxx"
    endpoint = "obs.xxxxxx.myhuaweicloud.com"
    file_format_type = "text"
  }

```

### parquet file

> For parquet file format simple config

```hocon

  ObsFile {
    path = "/seatunnel/parquet"
    bucket = "obs://obs-bucket-name"
    access_key = "xxxxxxxxxxxxxxxxx"
    access_secret = "xxxxxxxxxxxxxxxxxxxxxx"
    endpoint = "obs.xxxxxx.myhuaweicloud.com"
    file_format_type = "parquet"
  }

```

### orc file

> For orc file format simple config

```hocon

  ObsFile {
    path = "/seatunnel/orc"
    bucket = "obs://obs-bucket-name"
    access_key = "xxxxxxxxxxxxxxxxx"
    access_secret = "xxxxxxxxxxxxxxxxxxxxxx"
    endpoint = "obs.xxxxxx.myhuaweicloud.com"
    file_format_type = "orc"
  }

```

### json file

> For json file format simple config

```hocon

  ObsFile {
    path = "/seatunnel/json"
    bucket = "obs://obs-bucket-name"
    access_key = "xxxxxxxxxxxxxxxxx"
    access_secret = "xxxxxxxxxxxxxxxxxxxxxx"
    endpoint = "obs.xxxxxx.myhuaweicloud.com"
    file_format_type = "json"
  }

```

### excel file

> For excel file format simple config

```hocon

  ObsFile {
    path = "/seatunnel/excel"
    bucket = "obs://obs-bucket-name"
    access_key = "xxxxxxxxxxxxxxxxx"
    access_secret = "xxxxxxxxxxxxxxxxxxxxxx"
    endpoint = "obs.xxxxxx.myhuaweicloud.com"
    file_format_type = "excel"
  }

```

### csv file

> For csv file format simple config

```hocon

  ObsFile {
    path = "/seatunnel/csv"
    bucket = "obs://obs-bucket-name"
    access_key = "xxxxxxxxxxxxxxxxx"
    access_secret = "xxxxxxxxxxxxxxxxxxxxxx"
    endpoint = "obs.xxxxxx.myhuaweicloud.com"
    file_format_type = "csv"
    delimiter = ","
  }

```

## Changelog

### next version

- Add Obs File Source Connector

