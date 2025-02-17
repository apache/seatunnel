# S3File

> S3 File Sink Connector

## Support Those Engines

> Spark<br/>
> Flink<br/>
> SeaTunnel Zeta<br/>

## Key Features

- [x] [exactly-once](../../concept/connector-v2-features.md)
- [ ] [cdc](../../concept/connector-v2-features.md)
- [x] [support multiple table write](../../concept/connector-v2-features.md)

By default, we use 2PC commit to ensure `exactly-once`

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

Output data to aws s3 file system.

## Supported DataSource Info

| Datasource | Supported Versions |
|------------|--------------------|
| S3         | current            |

## Database Dependency

> If you use spark/flink, In order to use this connector, You must ensure your spark/flink cluster already integrated hadoop. The tested hadoop version is 2.x.
>
> If you use SeaTunnel Engine, It automatically integrated the hadoop jar when you download and install SeaTunnel Engine. You can check the jar package under `${SEATUNNEL_HOME}/lib` to confirm this.
> To use this connector you need put `hadoop-aws-3.1.4.jar` and `aws-java-sdk-bundle-1.12.692.jar` in `${SEATUNNEL_HOME}/lib` dir.

## Data Type Mapping

If write to `csv`, `text` file type, All column will be string.

### Orc File Type

| SeaTunnel Data type  | Orc Data type         |
|----------------------|-----------------------|
| STRING               | STRING                |
| BOOLEAN              | BOOLEAN               |
| TINYINT              | BYTE                  |
| SMALLINT             | SHORT                 |
| INT                  | INT                   |
| BIGINT               | LONG                  |
| FLOAT                | FLOAT                 |
| FLOAT                | FLOAT                 |
| DOUBLE               | DOUBLE                |
| DECIMAL              | DECIMAL               |
| BYTES                | BINARY                |
| DATE                 | DATE                  |
| TIME <br/> TIMESTAMP | TIMESTAMP             |
| ROW                  | STRUCT                |
| NULL                 | UNSUPPORTED DATA TYPE |
| ARRAY                | LIST                  |
| Map                  | Map                   |

### Parquet File Type

| SeaTunnel Data type  | Parquet Data type     |
|----------------------|-----------------------|
| STRING               | STRING                |
| BOOLEAN              | BOOLEAN               |
| TINYINT              | INT_8                 |
| SMALLINT             | INT_16                |
| INT                  | INT32                 |
| BIGINT               | INT64                 |
| FLOAT                | FLOAT                 |
| FLOAT                | FLOAT                 |
| DOUBLE               | DOUBLE                |
| DECIMAL              | DECIMAL               |
| BYTES                | BINARY                |
| DATE                 | DATE                  |
| TIME <br/> TIMESTAMP | TIMESTAMP_MILLIS      |
| ROW                  | GroupType             |
| NULL                 | UNSUPPORTED DATA TYPE |
| ARRAY                | LIST                  |
| Map                  | Map                   |

## Sink Options

| name                                  | type    | required | default value                                         | Description                                                                                                                                                            |
|---------------------------------------|---------|----------|-------------------------------------------------------|------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| path                                  | string  | yes      | -                                                     |                                                                                                                                                                        |
| tmp_path                              | string  | no       | /tmp/seatunnel                                        | The result file will write to a tmp path first and then use `mv` to submit tmp dir to target dir. Need a S3 dir.                                                       |
| bucket                                | string  | yes      | -                                                     |                                                                                                                                                                        |
| fs.s3a.endpoint                       | string  | yes      | -                                                     |                                                                                                                                                                        |
| fs.s3a.aws.credentials.provider       | string  | yes      | com.amazonaws.auth.InstanceProfileCredentialsProvider | The way to authenticate s3a. We only support `org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider` and `com.amazonaws.auth.InstanceProfileCredentialsProvider` now.  |
| access_key                            | string  | no       | -                                                     | Only used when fs.s3a.aws.credentials.provider = org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider                                                                 |
| access_secret                         | string  | no       | -                                                     | Only used when fs.s3a.aws.credentials.provider = org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider                                                                 |
| custom_filename                       | boolean | no       | false                                                 | Whether you need custom the filename                                                                                                                                   |
| file_name_expression                  | string  | no       | "${transactionId}"                                    | Only used when custom_filename is true                                                                                                                                 |
| filename_time_format                  | string  | no       | "yyyy.MM.dd"                                          | Only used when custom_filename is true                                                                                                                                 |
| file_format_type                      | string  | no       | "csv"                                                 |                                                                                                                                                                        |
| field_delimiter                       | string  | no       | '\001'                                                | Only used when file_format is text                                                                                                                                     |
| row_delimiter                         | string  | no       | "\n"                                                  | Only used when file_format is text                                                                                                                                     |
| have_partition                        | boolean | no       | false                                                 | Whether you need processing partitions.                                                                                                                                |
| partition_by                          | array   | no       | -                                                     | Only used when have_partition is true                                                                                                                                  |
| partition_dir_expression              | string  | no       | "${k0}=${v0}/${k1}=${v1}/.../${kn}=${vn}/"            | Only used when have_partition is true                                                                                                                                  |
| is_partition_field_write_in_file      | boolean | no       | false                                                 | Only used when have_partition is true                                                                                                                                  |
| sink_columns                          | array   | no       |                                                       | When this parameter is empty, all fields are sink columns                                                                                                              |
| is_enable_transaction                 | boolean | no       | true                                                  |                                                                                                                                                                        |
| batch_size                            | int     | no       | 1000000                                               |                                                                                                                                                                        |
| compress_codec                        | string  | no       | none                                                  |                                                                                                                                                                        |
| common-options                        | object  | no       | -                                                     |                                                                                                                                                                        |
| max_rows_in_memory                    | int     | no       | -                                                     | Only used when file_format is excel.                                                                                                                                   |
| sheet_name                            | string  | no       | Sheet${Random number}                                 | Only used when file_format is excel.                                                                                                                                   |
| csv_string_quote_mode                 | enum    | no       | MINIMAL                                               | Only used when file_format is csv.                                                                                                                                     |
| xml_root_tag                          | string  | no       | RECORDS                                               | Only used when file_format is xml, specifies the tag name of the root element within the XML file.                                                                     |
| xml_row_tag                           | string  | no       | RECORD                                                | Only used when file_format is xml, specifies the tag name of the data rows within the XML file                                                                         |
| xml_use_attr_format                   | boolean | no       | -                                                     | Only used when file_format is xml, specifies Whether to process data using the tag attribute format.                                                                   |
| single_file_mode                      | boolean | no       | false                                                 | Each parallelism will only output one file. When this parameter is turned on, batch_size will not take effect. The output file name does not have a file block suffix. |
| create_empty_file_when_no_data        | boolean | no       | false                                                 | When there is no data synchronization upstream, the corresponding data files are still generated.                                                                      |
| parquet_avro_write_timestamp_as_int96 | boolean | no       | false                                                 | Only used when file_format is parquet.                                                                                                                                 |
| parquet_avro_write_fixed_as_int96     | array   | no       | -                                                     | Only used when file_format is parquet.                                                                                                                                 |
| hadoop_s3_properties                  | map     | no       |                                                       | If you need to add a other option, you could add it here and refer to this [link](https://hadoop.apache.org/docs/stable/hadoop-aws/tools/hadoop-aws/index.html)        |
| schema_save_mode                      | Enum    | no       | CREATE_SCHEMA_WHEN_NOT_EXIST                          | Before turning on the synchronous task, do different treatment of the target path                                                                                      |
| data_save_mode                        | Enum    | no       | APPEND_DATA                                           | Before opening the synchronous task, the data file in the target path is differently processed                                                                         |
| enable_header_write                   | boolean | no       | false                                                 | Only used when file_format_type is text,csv.<br/> false:don't write header,true:write header.                                                                          |
| encoding                              | string  | no       | "UTF-8"                                               | Only used when file_format_type is json,text,csv,xml.                                                                                                                  |

### path [string]

Store the path of the data file to support variable replacement. For example: path=/test/${database_name}/${schema_name}/${table_name}

### hadoop_s3_properties [map]

If you need to add a other option, you could add it here and refer to this [link](https://hadoop.apache.org/docs/stable/hadoop-aws/tools/hadoop-aws/index.html)

```
hadoop_s3_properties {
      "fs.s3a.buffer.dir" = "/data/st_test/s3a"
      "fs.s3a.fast.upload.buffer" = "disk"
   }
```

### custom_filename [boolean]

Whether custom the filename

### file_name_expression [string]

Only used when `custom_filename` is `true`

`file_name_expression` describes the file expression which will be created into the `path`. We can add the variable `${now}` or `${uuid}` in the `file_name_expression`, like `test_${uuid}_${now}`,
`${now}` represents the current time, and its format can be defined by specifying the option `filename_time_format`.

Please note that, If `is_enable_transaction` is `true`, we will auto add `${transactionId}_` in the head of the file.

### filename_time_format [string]

Only used when `custom_filename` is `true`

When the format in the `file_name_expression` parameter is `xxxx-${now}` , `filename_time_format` can specify the time format of the path, and the default value is `yyyy.MM.dd` . The commonly used time formats are listed as follows:

| Symbol |    Description     |
|--------|--------------------|
| y      | Year               |
| M      | Month              |
| d      | Day of month       |
| H      | Hour in day (0-23) |
| m      | Minute in hour     |
| s      | Second in minute   |

### file_format_type [string]

We supported as the following file types:

`text` `csv` `parquet` `orc` `json` `excel` `xml` `binary`

Please note that, The final file name will end with the file_format_type's suffix, the suffix of the text file is `txt`.

### field_delimiter [string]

The separator between columns in a row of data. Only needed by `text` file format.

### row_delimiter [string]

The separator between rows in a file. Only needed by `text` file format.

### have_partition [boolean]

Whether you need processing partitions.

### partition_by [array]

Only used when `have_partition` is `true`.

Partition data based on selected fields.

### partition_dir_expression [string]

Only used when `have_partition` is `true`.

If the `partition_by` is specified, we will generate the corresponding partition directory based on the partition information, and the final file will be placed in the partition directory.

Default `partition_dir_expression` is `${k0}=${v0}/${k1}=${v1}/.../${kn}=${vn}/`. `k0` is the first partition field and `v0` is the value of the first partition field.

### is_partition_field_write_in_file [boolean]

Only used when `have_partition` is `true`.

If `is_partition_field_write_in_file` is `true`, the partition field and the value of it will be write into data file.

For example, if you want to write a Hive Data File, Its value should be `false`.

### sink_columns [array]

Which columns need be written to file, default value is all the columns get from `Transform` or `Source`.
The order of the fields determines the order in which the file is actually written.

### is_enable_transaction [boolean]

If `is_enable_transaction` is true, we will ensure that data will not be lost or duplicated when it is written to the target directory.

Please note that, If `is_enable_transaction` is `true`, we will auto add `${transactionId}_` in the head of the file.

Only support `true` now.

### batch_size [int]

The maximum number of rows in a file. For SeaTunnel Engine, the number of lines in the file is determined by `batch_size` and `checkpoint.interval` jointly decide. If the value of `checkpoint.interval` is large enough, sink writer will write rows in a file until the rows in the file larger than `batch_size`. If `checkpoint.interval` is small, the sink writer will create a new file when a new checkpoint trigger.

### compress_codec [string]

The compress codec of files and the details that supported as the following shown:

- txt: `lzo` `none`
- json: `lzo` `none`
- csv: `lzo` `none`
- orc: `lzo` `snappy` `lz4` `zlib` `none`
- parquet: `lzo` `snappy` `lz4` `gzip` `brotli` `zstd` `none`

Tips: excel type does not support any compression format

### common options

Sink plugin common parameters, please refer to [Sink Common Options](../sink-common-options.md) for details.

### max_rows_in_memory [int]

When File Format is Excel,The maximum number of data items that can be cached in the memory.

### sheet_name [string]

Writer the sheet of the workbook

### csv_string_quote_mode [string]

When File Format is CSV,The string quote mode of CSV.

- ALL: All String fields will be quoted.
- MINIMAL: Quotes fields which contain special characters such as a the field delimiter, quote character or any of the characters in the line separator string.
- NONE: Never quotes fields. When the delimiter occurs in data, the printer prefixes it with the escape character. If the escape character is not set, format validation throws an exception.

### xml_root_tag [string]

Specifies the tag name of the root element within the XML file.

### xml_row_tag [string]

Specifies the tag name of the data rows within the XML file.

### xml_use_attr_format [boolean]

Specifies Whether to process data using the tag attribute format.

### parquet_avro_write_timestamp_as_int96 [boolean]

Support writing Parquet INT96 from a timestamp, only valid for parquet files.

### parquet_avro_write_fixed_as_int96 [array]

Support writing Parquet INT96 from a 12-byte field, only valid for parquet files.

### schema_save_mode[Enum]

Before turning on the synchronous task, do different treatment of the target path.  
Option introduction：  
`RECREATE_SCHEMA` ：Will be created when the path does not exist. If the path already exists, delete the path and recreate it.         
`CREATE_SCHEMA_WHEN_NOT_EXIST` ：Will Created when the path does not exist, use the path when the path is existed.        
`ERROR_WHEN_SCHEMA_NOT_EXIST` ：Error will be reported when the path does not exist  
`IGNORE` ：Ignore the treatment of the table

### data_save_mode[Enum]

Before opening the synchronous task, the data file in the target path is differently processed.
Option introduction：  
`DROP_DATA`： use the path but delete data files in the path.
`APPEND_DATA`：use the path, and add new files in the path for write data.   
`ERROR_WHEN_DATA_EXISTS`：When there are some data files in the path, an error will is reported.

### encoding [string]

Only used when file_format_type is json,text,csv,xml.
The encoding of the file to write. This param will be parsed by `Charset.forName(encoding)`.

## Example

### Simple:

> This example defines a SeaTunnel synchronization task that automatically generates data through FakeSource and sends it to S3File Sink. FakeSource generates a total of 16 rows of data (row.num=16), with each row having two fields, name (string type) and age (int type). The final target s3 dir will also create a file and all of the data in write in it.
> Before run this job, you need create s3 path: /seatunnel/text. And if you have not yet installed and deployed SeaTunnel, you need to follow the instructions in [Install SeaTunnel](../../start-v2/locally/deployment.md) to install and deploy SeaTunnel. And then follow the instructions in [Quick Start With SeaTunnel Engine](../../start-v2/locally/quick-start-seatunnel-engine.md) to run this job.

```
# Defining the runtime environment
env {
  parallelism = 1
  job.mode = "BATCH"
}

source {
  # This is a example source plugin **only for test and demonstrate the feature source plugin**
  FakeSource {
    parallelism = 1
    plugin_output = "fake"
    row.num = 16
    schema = {
      fields {
        c_map = "map<string, array<int>>"
        c_array = "array<int>"
        name = string
        c_boolean = boolean
        age = tinyint
        c_smallint = smallint
        c_int = int
        c_bigint = bigint
        c_float = float
        c_double = double
        c_decimal = "decimal(16, 1)"
        c_null = "null"
        c_bytes = bytes
        c_date = date
        c_timestamp = timestamp
      }
    }
  }
  # If you would like to get more information about how to configure seatunnel and see full list of source plugins,
  # please go to https://seatunnel.apache.org/docs/connector-v2/source
}

transform {
  # If you would like to get more information about how to configure seatunnel and see full list of transform plugins,
    # please go to https://seatunnel.apache.org/docs/transform-v2
}

sink {
    S3File {
      bucket = "s3a://seatunnel-test"
      tmp_path = "/tmp/seatunnel"
      path="/seatunnel/text"
      fs.s3a.endpoint="s3.cn-north-1.amazonaws.com.cn"
      fs.s3a.aws.credentials.provider="com.amazonaws.auth.InstanceProfileCredentialsProvider"
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
      is_enable_transaction=true
      hadoop_s3_properties {
        "fs.s3a.buffer.dir" = "/data/st_test/s3a"
        "fs.s3a.fast.upload.buffer" = "disk"
      }
  }
  # If you would like to get more information about how to configure seatunnel and see full list of sink plugins,
  # please go to https://seatunnel.apache.org/docs/connector-v2/sink
}
```

For text file format with `have_partition` and `custom_filename` and `sink_columns`
and `com.amazonaws.auth.InstanceProfileCredentialsProvider`

```hocon

  S3File {
    bucket = "s3a://seatunnel-test"
    tmp_path = "/tmp/seatunnel"
    path="/seatunnel/text"
    fs.s3a.endpoint="s3.cn-north-1.amazonaws.com.cn"
    fs.s3a.aws.credentials.provider="com.amazonaws.auth.InstanceProfileCredentialsProvider"
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
    is_enable_transaction=true
    hadoop_s3_properties {
      "fs.s3a.buffer.dir" = "/data/st_test/s3a"
      "fs.s3a.fast.upload.buffer" = "disk"
    }
  }

```

For parquet file format simple config with `org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider`

```hocon

  S3File {
    bucket = "s3a://seatunnel-test"
    tmp_path = "/tmp/seatunnel"
    path="/seatunnel/parquet"
    fs.s3a.endpoint="s3.cn-north-1.amazonaws.com.cn"
    fs.s3a.aws.credentials.provider="org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider"
    access_key = "xxxxxxxxxxxxxxxxx"
    secret_key = "xxxxxxxxxxxxxxxxx"
    file_format_type = "parquet"
    hadoop_s3_properties {
      "fs.s3a.buffer.dir" = "/data/st_test/s3a"
      "fs.s3a.fast.upload.buffer" = "disk"
    }
  }

```

For orc file format simple config with `org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider`

```hocon

  S3File {
    bucket = "s3a://seatunnel-test"
    tmp_path = "/tmp/seatunnel"
    path="/seatunnel/orc"
    fs.s3a.endpoint="s3.cn-north-1.amazonaws.com.cn"
    fs.s3a.aws.credentials.provider="org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider"
    access_key = "xxxxxxxxxxxxxxxxx"
    secret_key = "xxxxxxxxxxxxxxxxx"
    file_format_type = "orc"
    schema_save_mode = "CREATE_SCHEMA_WHEN_NOT_EXIST"
    data_save_mode="APPEND_DATA"
  }

```

Multi-table writing and saveMode

```hocon
env {
  "job.name"="SeaTunnel_job"
  "job.mode"=STREAMING
}
source {
  MySQL-CDC {
      database-names=[
          "wls_t1"
      ]
      table-names=[
          "wls_t1.mysqlcdc_to_s3_t3",
          "wls_t1.mysqlcdc_to_s3_t4",
          "wls_t1.mysqlcdc_to_s3_t5",
          "wls_t1.mysqlcdc_to_s3_t1",
          "wls_t1.mysqlcdc_to_s3_t2"
      ]
      password="xxxxxx"
      username="xxxxxxxxxxxxx"
      base-url="jdbc:mysql://localhost:3306/qa_source"
  }
}

transform {
}

sink {
  S3File {
    bucket = "s3a://seatunnel-test"
    tmp_path = "/tmp/seatunnel/${table_name}"
    path="/test/${table_name}"
    fs.s3a.endpoint="s3.cn-north-1.amazonaws.com.cn"
    fs.s3a.aws.credentials.provider="org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider"
    access_key = "xxxxxxxxxxxxxxxxx"
    secret_key = "xxxxxxxxxxxxxxxxx"
    file_format_type = "orc"
    schema_save_mode = "CREATE_SCHEMA_WHEN_NOT_EXIST"
    data_save_mode="APPEND_DATA"
  }
}
```

### enable_header_write [boolean]

Only used when file_format_type is text,csv.false:don't write header,true:write header.