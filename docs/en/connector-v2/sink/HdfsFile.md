# HdfsFile

> HDFS File Sink Connector

## Support Those Engines

> Spark<br/>
> Flink<br/>
> SeaTunnel Zeta<br/>

## Key Features

- [x] [exactly-once](../../concept/connector-v2-features.md)

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
- [x] compress codec
  - [x] lzo

## Description

Output data to hdfs file

## Supported DataSource Info

| Datasource | Supported Versions |
|------------|--------------------|
| HdfsFile   | hadoop 2.x and 3.x |

## Sink Options

| Name                                  | Type    | Required | Default                                    | Description                                                                                                                                                                                                                                                                                                                                                                                                                                                                              |
|---------------------------------------|---------|----------|--------------------------------------------|------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| fs.defaultFS                          | string  | yes      | -                                          | The hadoop cluster address that start with `hdfs://`, for example: `hdfs://hadoopcluster`                                                                                                                                                                                                                                                                                                                                                                                                |
| path                                  | string  | yes      | -                                          | The target dir path is required.                                                                                                                                                                                                                                                                                                                                                                                                                                                         |
| tmp_path                              | string  | yes      | /tmp/seatunnel                             | The result file will write to a tmp path first and then use `mv` to submit tmp dir to target dir. Need a hdfs path.                                                                                                                                                                                                                                                                                                                                                                      |
| hdfs_site_path                        | string  | no       | -                                          | The path of `hdfs-site.xml`, used to load ha configuration of namenodes                                                                                                                                                                                                                                                                                                                                                                                                                  |
| custom_filename                       | boolean | no       | false                                      | Whether you need custom the filename                                                                                                                                                                                                                                                                                                                                                                                                                                                     |
| file_name_expression                  | string  | no       | "${transactionId}"                         | Only used when `custom_filename` is `true`.`file_name_expression` describes the file expression which will be created into the `path`. We can add the variable `${now}` or `${uuid}` in the `file_name_expression`, like `test_${uuid}_${now}`,`${now}` represents the current time, and its format can be defined by specifying the option `filename_time_format`.Please note that, If `is_enable_transaction` is `true`, we will auto add `${transactionId}_` in the head of the file. |
| filename_time_format                  | string  | no       | "yyyy.MM.dd"                               | Only used when `custom_filename` is `true`.When the format in the `file_name_expression` parameter is `xxxx-${now}` , `filename_time_format` can specify the time format of the path, and the default value is `yyyy.MM.dd` . The commonly used time formats are listed as follows:[y:Year,M:Month,d:Day of month,H:Hour in day (0-23),m:Minute in hour,s:Second in minute]                                                                                                              |
| file_format_type                      | string  | no       | "csv"                                      | We supported as the following file types:`text` `csv` `parquet` `orc` `json` `excel` `xml` `binary`.Please note that, The final file name will end with the file_format's suffix, the suffix of the text file is `txt`.                                                                                                                                                                                                                                                                  |
| field_delimiter                       | string  | no       | '\001'                                     | Only used when file_format is text,The separator between columns in a row of data. Only needed by `text` file format.                                                                                                                                                                                                                                                                                                                                                                    |
| row_delimiter                         | string  | no       | "\n"                                       | Only used when file_format is text,The separator between rows in a file. Only needed by `text` file format.                                                                                                                                                                                                                                                                                                                                                                              |
| have_partition                        | boolean | no       | false                                      | Whether you need processing partitions.                                                                                                                                                                                                                                                                                                                                                                                                                                                  |
| partition_by                          | array   | no       | -                                          | Only used then have_partition is true,Partition data based on selected fields.                                                                                                                                                                                                                                                                                                                                                                                                           |
| partition_dir_expression              | string  | no       | "${k0}=${v0}/${k1}=${v1}/.../${kn}=${vn}/" | Only used then have_partition is true,If the `partition_by` is specified, we will generate the corresponding partition directory based on the partition information, and the final file will be placed in the partition directory. Default `partition_dir_expression` is `${k0}=${v0}/${k1}=${v1}/.../${kn}=${vn}/`. `k0` is the first partition field and `v0` is the value of the first partition field.                                                                               |
| is_partition_field_write_in_file      | boolean | no       | false                                      | Only used when `have_partition` is `true`. If `is_partition_field_write_in_file` is `true`, the partition field and the value of it will be write into data file.For example, if you want to write a Hive Data File, Its value should be `false`.                                                                                                                                                                                                                                        |
| sink_columns                          | array   | no       |                                            | When this parameter is empty, all fields are sink columns.Which columns need be write to file, default value is all of the columns get from `Transform` or `Source`. The order of the fields determines the order in which the file is actually written.                                                                                                                                                                                                                                 |
| is_enable_transaction                 | boolean | no       | true                                       | If `is_enable_transaction` is true, we will ensure that data will not be lost or duplicated when it is written to the target directory.Please note that, If `is_enable_transaction` is `true`, we will auto add `${transactionId}_` in the head of the file.Only support `true` now.                                                                                                                                                                                                     |
| batch_size                            | int     | no       | 1000000                                    | The maximum number of rows in a file. For SeaTunnel Engine, the number of lines in the file is determined by `batch_size` and `checkpoint.interval` jointly decide. If the value of `checkpoint.interval` is large enough, sink writer will write rows in a file until the rows in the file larger than `batch_size`. If `checkpoint.interval` is small, the sink writer will create a new file when a new checkpoint trigger.                                                           |
| compress_codec                        | string  | no       | none                                       | The compress codec of files and the details that supported as the following shown:[txt: `lzo` `none`,json: `lzo` `none`,csv: `lzo` `none`,orc: `lzo` `snappy` `lz4` `zlib` `none`,parquet: `lzo` `snappy` `lz4` `gzip` `brotli` `zstd` `none`].Tips: excel type does not support any compression format.                                                                                                                                                                                 |
| krb5_path                             | string  | no       | /etc/krb5.conf                             | The krb5 path of kerberos                                                                                                                                                                                                                                                                                                                                                                                                                                                                |
| kerberos_principal                    | string  | no       | -                                          | The principal of kerberos                                                                                                                                                                                                                                                                                                                                                                                                                                                                |
| kerberos_keytab_path                  | string  | no       | -                                          | The keytab path of kerberos                                                                                                                                                                                                                                                                                                                                                                                                                                                              |
| compress_codec                        | string  | no       | none                                       | compress codec                                                                                                                                                                                                                                                                                                                                                                                                                                                                           |
| common-options                        | object  | no       | -                                          | Sink plugin common parameters, please refer to [Sink Common Options](../sink-common-options.md) for details                                                                                                                                                                                                                                                                                                                                                                              |
| max_rows_in_memory                    | int     | no       | -                                          | Only used when file_format is excel.When File Format is Excel,The maximum number of data items that can be cached in the memory.                                                                                                                                                                                                                                                                                                                                                         |
| sheet_name                            | string  | no       | Sheet${Random number}                      | Only used when file_format is excel.Writer the sheet of the workbook                                                                                                                                                                                                                                                                                                                                                                                                                     |
| xml_root_tag                          | string  | no       | RECORDS                                    | Only used when file_format is xml, specifies the tag name of the root element within the XML file.                                                                                                                                                                                                                                                                                                                                                                                       |
| xml_row_tag                           | string  | no       | RECORD                                     | Only used when file_format is xml, specifies the tag name of the data rows within the XML file                                                                                                                                                                                                                                                                                                                                                                                           |
| xml_use_attr_format                   | boolean | no       | -                                          | Only used when file_format is xml, specifies Whether to process data using the tag attribute format.                                                                                                                                                                                                                                                                                                                                                                                     |
| single_file_mode                      | boolean | no       | false                                      | Each parallelism will only output one file. When this parameter is turned on, batch_size will not take effect. The output file name does not have a file block suffix.                                                                                                                                                                                                                                                                                                                   |
| create_empty_file_when_no_data        | boolean | no       | false                                      | When there is no data synchronization upstream, the corresponding data files are still generated.                                                                                                                                                                                                                                                                                                                                                                                        |
| parquet_avro_write_timestamp_as_int96 | boolean | no       | false                                      | Only used when file_format is parquet.                                                                                                                                                                                                                                                                                                                                                                                                                                                   |
| parquet_avro_write_fixed_as_int96     | array   | no       | -                                          | Only used when file_format is parquet.                                                                                                                                                                                                                                                                                                                                                                                                                                                   |
| enable_header_write                   | boolean | no       | false                                      | Only used when file_format_type is text,csv.<br/> false:don't write header,true:write header.                                                                                                                                                                                                                                                                                                                                                                                            |
| encoding                              | string  | no       | "UTF-8"                                    | Only used when file_format_type is json,text,csv,xml.                                                                                                                                                                                                                                                                                                                                                                                                                                    |
| remote_user                           | string  | no       | -                                          | The remote user name of hdfs.                                                                                                                                                                                                                                                                                                                                                                                                                                                            |

### Tips

> If you use spark/flink, In order to use this connector, You must ensure your spark/flink cluster already integrated hadoop. The tested hadoop version is 2.x. If you use SeaTunnel Engine, It automatically integrated the hadoop jar when you download and install SeaTunnel Engine. You can check the jar package under ${SEATUNNEL_HOME}/lib to confirm this.

## Task Example

### Simple:

> This example defines a SeaTunnel synchronization task that automatically generates data through FakeSource and sends it to Hdfs.

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
        c_map = "map<string, smallint>"
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
    # please go to https://seatunnel.apache.org/docs/category/transform-v2
}

sink {
    HdfsFile {
      fs.defaultFS = "hdfs://hadoopcluster"
      path = "/tmp/hive/warehouse/test2"
      file_format_type = "orc"
    }
  # If you would like to get more information about how to configure seatunnel and see full list of sink plugins,
  # please go to https://seatunnel.apache.org/docs/connector-v2/sink
}
```

### For orc file format simple config

```
HdfsFile {
    fs.defaultFS = "hdfs://hadoopcluster"
    path = "/tmp/hive/warehouse/test2"
    file_format_type = "orc"
}
```

### For text file format with `have_partition` and `custom_filename` and `sink_columns`

```
HdfsFile {
    fs.defaultFS = "hdfs://hadoopcluster"
    path = "/tmp/hive/warehouse/test2"
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
```

### For parquet file format with `have_partition` and `custom_filename` and `sink_columns`

```
HdfsFile {
    fs.defaultFS = "hdfs://hadoopcluster"
    path = "/tmp/hive/warehouse/test2"
    have_partition = true
    partition_by = ["age"]
    partition_dir_expression = "${k0}=${v0}"
    is_partition_field_write_in_file = true
    custom_filename = true
    file_name_expression = "${transactionId}_${now}"
    filename_time_format = "yyyy.MM.dd"
    file_format_type = "parquet"
    sink_columns = ["name","age"]
    is_enable_transaction = true
}
```

### For kerberos simple config

```
HdfsFile {
    fs.defaultFS = "hdfs://hadoopcluster"
    path = "/tmp/hive/warehouse/test2"
    hdfs_site_path = "/path/to/your/hdfs_site_path"
    kerberos_principal = "your_principal@EXAMPLE.COM"
    kerberos_keytab_path = "/path/to/your/keytab/file.keytab"
}
```
### enable_header_write [boolean]

Only used when file_format_type is text,csv.false:don't write header,true:write header.

### For compress simple config

```
HdfsFile {
    fs.defaultFS = "hdfs://hadoopcluster"
    path = "/tmp/hive/warehouse/test2"
    compress_codec = "lzo"
}
```

