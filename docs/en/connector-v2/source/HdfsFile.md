# HdfsFile

> Hdfs File Source Connector

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
- [x] file format file
  - [x] text
  - [x] csv
  - [x] parquet
  - [x] orc
  - [x] json
  - [x] excel

## Description

Read data from hdfs file system.

## Supported DataSource Info

| Datasource | Supported Versions |
|------------|--------------------|
| HdfsFile   | hadoop 2.x and 3.x |

## Source Options

|           Name            |  Type   | Required |       Default       |                                                                                                                                                                  Description                                                                                                                                                                  |
|---------------------------|---------|----------|---------------------|-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| path                      | string  | yes      | -                   | The source file path.                                                                                                                                                                                                                                                                                                                         |
| file_format_type          | string  | yes      | -                   | We supported as the following file types:`text` `json` `csv` `orc` `parquet` `excel`.Please note that, The final file name will end with the file_format's suffix, the suffix of the text file is `txt`.                                                                                                                                      |
| fs.defaultFS              | string  | yes      | -                   | The hadoop cluster address that start with `hdfs://`, for example: `hdfs://hadoopcluster`                                                                                                                                                                                                                                                     |
| read_columns              | list    | yes      | -                   | The read column list of the data source, user can use it to implement field projection.The file type supported column projection as the following shown:[text,json,csv,orc,parquet,excel].Tips: If the user wants to use this feature when reading `text` `json` `csv` files, the schema option must be configured.                           |
| hdfs_site_path            | string  | no       | -                   | The path of `hdfs-site.xml`, used to load ha configuration of namenodes                                                                                                                                                                                                                                                                       |
| delimiter                 | string  | no       | \001                | Field delimiter, used to tell connector how to slice and dice fields when reading text files. default `\001`, the same as hive's default delimiter                                                                                                                                                                                            |
| parse_partition_from_path | boolean | no       | true                | Control whether parse the partition keys and values from file path. For example if you read a file from path `hdfs://hadoop-cluster/tmp/seatunnel/parquet/name=tyrantlucifer/age=26`. Every record data from file will be added these two fields:[name:tyrantlucifer,age:26].Tips:Do not define partition fields in schema option.            |
| date_format               | string  | no       | yyyy-MM-dd          | Date type format, used to tell connector how to convert string to date, supported as the following formats:`yyyy-MM-dd` `yyyy.MM.dd` `yyyy/MM/dd` default `yyyy-MM-dd`.Date type format, used to tell connector how to convert string to date, supported as the following formats:`yyyy-MM-dd` `yyyy.MM.dd` `yyyy/MM/dd` default `yyyy-MM-dd` |
| datetime_format           | string  | no       | yyyy-MM-dd HH:mm:ss | Datetime type format, used to tell connector how to convert string to datetime, supported as the following formats:`yyyy-MM-dd HH:mm:ss` `yyyy.MM.dd HH:mm:ss` `yyyy/MM/dd HH:mm:ss` `yyyyMMddHHmmss` .default `yyyy-MM-dd HH:mm:ss`                                                                                                          |
| time_format               | string  | no       | HH:mm:ss            | Time type format, used to tell connector how to convert string to time, supported as the following formats:`HH:mm:ss` `HH:mm:ss.SSS`.default `HH:mm:ss`                                                                                                                                                                                       |
| kerberos_principal        | string  | no       | -                   | The principal of kerberos                                                                                                                                                                                                                                                                                                                     |
| kerberos_keytab_path      | string  | no       | -                   | The keytab path of kerberos                                                                                                                                                                                                                                                                                                                   |
| skip_header_row_number    | long    | no       | 0                   | Skip the first few lines, but only for the txt and csv.For example, set like following:`skip_header_row_number = 2`.then Seatunnel will skip the first 2 lines from source files                                                                                                                                                              |
| schema                    | config  | no       | -                   | the schema fields of upstream data                                                                                                                                                                                                                                                                                                            |
| common-options            |         | no       | -                   | Source plugin common parameters, please refer to [Source Common Options](common-options.md) for details.                                                                                                                                                                                                                                      |
| sheet_name                | string  | no       | -                   | Reader the sheet of the workbook,Only used when file_format is excel.                                                                                                                                                                                                                                                                         |
| compress_codec            | string  | no       | none                | The compress codec of files                                                                                                                                                                                                                                                                                                                   |

### compress_codec [string]

The compress codec of files and the details that supported as the following shown:

- txt: `lzo` `none`
- json: `lzo` `none`
- csv: `lzo` `none`
- orc/parquet:  
  automatically recognizes the compression type, no additional settings required.

### Tips

> If you use spark/flink, In order to use this connector, You must ensure your spark/flink cluster already integrated hadoop. The tested hadoop version is 2.x. If you use SeaTunnel Engine, It automatically integrated the hadoop jar when you download and install SeaTunnel Engine. You can check the jar package under ${SEATUNNEL_HOME}/lib to confirm this.

## Task Example

### Simple:

> This example defines a SeaTunnel synchronization task that  read data from Hdfs and sends it to Hdfs.

```
# Defining the runtime environment
env {
  # You can set flink configuration here
  execution.parallelism = 1
  job.mode = "BATCH"
}

source {
  HdfsFile {
  schema {
    fields {
      name = string
      age = int
    }
  }
  path = "/apps/hive/demo/student"
  type = "json"
  fs.defaultFS = "hdfs://namenode001"
  }
  # If you would like to get more information about how to configure seatunnel and see full list of source plugins,
  # please go to https://seatunnel.apache.org/docs/category/source-v2
}

transform {
  # If you would like to get more information about how to configure seatunnel and see full list of transform plugins,
    # please go to https://seatunnel.apache.org/docs/category/transform-v2
}

sink {
    HdfsFile {
      fs.defaultFS = "hdfs://hadoopcluster"
      path = "/tmp/hive/warehouse/test2"
      file_format = "orc"
    }
  # If you would like to get more information about how to configure seatunnel and see full list of sink plugins,
  # please go to https://seatunnel.apache.org/docs/category/sink-v2
}
```

