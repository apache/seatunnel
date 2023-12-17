# OssFile

> Oss file sink connector

## Support Those Engines

> Spark<br/>
> Flink<br/>
> SeaTunnel Zeta<br/>

## Key features

- [x] [exactly-once](../../concept/connector-v2-features.md)

By default, we use 2PC commit to ensure `exactly-once`

- [x] file format type
  - [x] text
  - [x] csv
  - [x] parquet
  - [x] orc
  - [x] json
  - [x] excel

## Description

Output data to oss file system.

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

SeaTunnel will write the data into the file in String format according to the SeaTunnel data type and file_format_type.

## Options

|               Name               |  Type   | Required |               Default value                |                                                                                                                                                                                                                                              Description                                                                                                                                                                                                                                               |
|----------------------------------|---------|----------|--------------------------------------------|--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| path                             | String  | Yes      | -                                          | The target dir path is required.                                                                                                                                                                                                                                                                                                                                                                                                                                                                       |
| tmp_path                         | string  | no       | /tmp/seatunnel                             | The result file will write to a tmp path first and then use `mv` to submit tmp dir to target dir. Need a OSS dir.                                                                                                                                                                                                                                                                                                                                                                                      |
| bucket                           | String  | Yes      | -                                          | The bucket address of oss file system, for example: `oss://tyrantlucifer-image-bed`                                                                                                                                                                                                                                                                                                                                                                                                                    |
| access_key                       | String  | No       | -                                          | The access key of oss file system.                                                                                                                                                                                                                                                                                                                                                                                                                                                                     |
| access_secret                    | String  | No       | -                                          | The access secret of oss file system.                                                                                                                                                                                                                                                                                                                                                                                                                                                                  |
| endpoint                         | String  | Yes      | -                                          | The endpoint of oss file system.                                                                                                                                                                                                                                                                                                                                                                                                                                                                       |
| custom_filename                  | Boolean | No       | false                                      | Whether you need custom the filename                                                                                                                                                                                                                                                                                                                                                                                                                                                                   |
| file_name_expression             | String  | No       | "${transactionId}"                         | Only used when `custom_filename` is `true`. <br/> `file_name_expression` describes the file expression which will be created into the `path`. We can add the variable `${Now}` or `${uuid}` in the `file_name_expression`, like `test_${uuid}_${Now}`, `${Now}` represents the current time, and its format can be defined by specifying the option `filename_time_format`. <br/>Please Note that, If `is_enable_transaction` is `true`, we will auto add `${transactionId}_` in the head of the file. |
| filename_time_format             | String  | No       | "yyyy.MM.dd"                               | Please check #filename_time_format below                                                                                                                                                                                                                                                                                                                                                                                                                                                               |
| file_format_type                 | String  | No       | "csv"                                      | We supported as the following file types: <br/> `text` `json` `csv` `orc` `parquet` `excel` <br/> Please Note that, The final file name will end with the file_format's suffix, the suffix of the text file is `txt`.                                                                                                                                                                                                                                                                                  |
| field_delimiter                  | String  | No       | '\001'                                     | The separator between columns in a row of data. Only needed by `text` file format.                                                                                                                                                                                                                                                                                                                                                                                                                     |
| row_delimiter                    | String  | No       | "\n"                                       | The separator between rows in a file. Only needed by `text` file format.                                                                                                                                                                                                                                                                                                                                                                                                                               |
| have_partition                   | Boolean | No       | false                                      | Whether you need processing partitions.                                                                                                                                                                                                                                                                                                                                                                                                                                                                |
| partition_by                     | Array   | No       | -                                          | Only used when `have_partition` is `true`. <br/> Partition data based on selected fields.                                                                                                                                                                                                                                                                                                                                                                                                              |
| partition_dir_expression         | String  | No       | "${k0}=${v0}/${k1}=${v1}/.../${kn}=${vn}/" | Only used when `have_partition` is `true`. <br/> If the `partition_by` is specified, we will generate the corresponding partition directory based on the partition information, and the final file will be placed in the partition directory. <br/> Default `partition_dir_expression` is `${k0}=${v0}/${k1}=${v1}/.../${kn}=${vn}/`. `k0` is the first partition field and `v0` is the value of the first partition field.                                                                            |
| is_partition_field_write_in_file | Boolean | No       | false                                      | Only used when `have_partition` is `true`. <br/> If `is_partition_field_write_in_file` is `true`, the partition field and the value of it will be write into data file. <br/> For example, if you want to write a Hive Data File, Its value should be `false`.                                                                                                                                                                                                                                         |
| sink_columns                     | Array   | No       |                                            | Which columns need be written to file, default value is all the columns get from `Transform` or `Source`. <br/> The order of the fields determines the order in which the file is actually written.                                                                                                                                                                                                                                                                                                    |
| is_enable_transaction            | Boolean | No       | true                                       | If `is_enable_transaction` is true, we will ensure that data will Not be lost or duplicated when it is written to the target directory. <br/> Please Note that, If `is_enable_transaction` is `true`, we will auto add `${transactionId}_` in the head of the file. <br/> Only support `true` Now.                                                                                                                                                                                                     |
| batch_size                       | Int     | No       | 1000000                                    | The maximum number of rows in a file. For SeaTunnel Engine, the number of lines in the file is determined by `batch_size` and `checkpoint.interval` jointly decide. If the value of `checkpoint.interval` is large eNough, sink writer will write rows in a file until the rows in the file larger than `batch_size`. If `checkpoint.interval` is small, the sink writer will create a new file when a new checkpoint trigger.                                                                         |
| compress_codec                   | String  | No       | None                                       | The compress codec of files and the details that supported as the following shown: <br/> - txt: `lzo` `None` <br/> - json: `lzo` `None` <br/> - csv: `lzo` `None` <br/> - orc: `lzo` `snappy` `lz4` `zlib` `None` <br/> - parquet: `lzo` `snappy` `lz4` `gzip` `brotli` `zstd` `None` <br/> Tips: excel type does Not support any compression format                                                                                                                                                   |
| max_rows_in_memory               | Int     | No       | -                                          | When File Format is Excel,The maximum number of data items that can be cached in the memory.                                                                                                                                                                                                                                                                                                                                                                                                           |
| sheet_name                       | String  | No       | Sheet${Random number}                      | Writer the sheet of the workbook                                                                                                                                                                                                                                                                                                                                                                                                                                                                       |
| common-options                   | Config  | No       | -                                          | Sink plugin common parameters, please refer to [Sink Common Options](common-options.md) for details.                                                                                                                                                                                                                                                                                                                                                                                                   |

### filename_time_format [String]

Only used when `custom_filename` is `true`

When the format in the `file_name_expression` parameter is `xxxx-${Now}` , `filename_time_format` can specify the time format of the path, and the default value is `yyyy.MM.dd` . The commonly used time formats are listed as follows:

| Symbol |    Description     |
|--------|--------------------|
| y      | Year               |
| M      | Month              |
| d      | Day of month       |
| H      | Hour in day (0-23) |
| m      | Minute in hour     |
| s      | Second in minute   |

## How to Create a Oss Data Synchronization Jobs

The following example demonstrates how to create a data synchronization job that reads data from Fake Source and writes it to the Oss:

For text file format with `have_partition` and `custom_filename` and `sink_columns`

```bash
# Set the basic configuration of the task to be performed
env {
  execution.parallelism = 1
  job.mode = "BATCH"
}

# Create a source to product data
source {
  FakeSource {
    schema = {
      fields {
        name = string
        age = int
      }
    }
  }
}

# write data to Oss
sink {
  OssFile {
    path="/seatunnel/sink"
    bucket = "oss://tyrantlucifer-image-bed"
    access_key = "xxxxxxxxxxx"
    access_secret = "xxxxxxxxxxx"
    endpoint = "oss-cn-beijing.aliyuncs.com"
    file_format_type = "text"
    field_delimiter = "\t"
    row_delimiter = "\n"
    have_partition = true
    partition_by = ["age"]
    partition_dir_expression = "${k0}=${v0}"
    is_partition_field_write_in_file = true
    custom_filename = true
    file_name_expression = "${transactionId}_${now}"
    filename_time_format = "yyyy.MM.dd"
    sink_columns = ["name","age"]
    is_enable_transaction = true
  }
}
```

For parquet file format with `have_partition` and `sink_columns`

```bash
# Set the basic configuration of the task to be performed
env {
  execution.parallelism = 1
  job.mode = "BATCH"
}

# Create a source to product data
source {
  FakeSource {
    schema = {
      fields {
        name = string
        age = int
      }
    }
  }
}

# Write data to Oss
sink {
  OssFile {
    path = "/seatunnel/sink"
    bucket = "oss://tyrantlucifer-image-bed"
    access_key = "xxxxxxxxxxx"
    access_secret = "xxxxxxxxxxxxxxxxx"
    endpoint = "oss-cn-beijing.aliyuncs.com"
    have_partition = true
    partition_by = ["age"]
    partition_dir_expression = "${k0}=${v0}"
    is_partition_field_write_in_file = true
    file_format_type = "parquet"
    sink_columns = ["name","age"]
  }
}
```

For orc file format simple config

```bash
# Set the basic configuration of the task to be performed
env {
  execution.parallelism = 1
  job.mode = "BATCH"
}

# Create a source to product data
source {
  FakeSource {
    schema = {
      fields {
        name = string
        age = int
      }
    }
  }
}

# Write data to Oss
sink {
  OssFile {
    path="/seatunnel/sink"
    bucket = "oss://tyrantlucifer-image-bed"
    access_key = "xxxxxxxxxxx"
    access_secret = "xxxxxxxxxxx"
    endpoint = "oss-cn-beijing.aliyuncs.com"
    file_format_type = "orc"
  }
}
```

### Tips

> 1.[SeaTunnel Deployment Document](../../start-v2/locally/deployment.md).

