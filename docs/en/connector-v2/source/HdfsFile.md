import ChangeLog from '../changelog/connector-file-hadoop.md';

# HdfsFile

> Hdfs File Source Connector

## Support Those Engines

> Spark<br/>
> Flink<br/>
> SeaTunnel Zeta<br/>

## Key Features

- [x] [batch](../../concept/connector-v2-features.md)
- [ ] [stream](../../concept/connector-v2-features.md)
- [x] [multimodal](../../concept/connector-v2-features.md#multimodal)

  Use binary file format to read and write files in any format, such as videos, pictures, etc. In short, any files can be synchronized to the target place.

- [x] [exactly-once](../../concept/connector-v2-features.md)

  Read all the data in a split in a pollNext call. What splits are read will be saved in snapshot.

- [x] [column projection](../../concept/connector-v2-features.md)
- [x] [parallelism](../../concept/connector-v2-features.md)
- [ ] [support user-defined split](../../concept/connector-v2-features.md)
- [x] [support multiple table read](../../concept/connector-v2-features.md)
- [x] file format file
  - [x] text
  - [x] csv
  - [x] parquet
  - [x] orc
  - [x] json
  - [x] excel
  - [x] xml
  - [x] binary
  - [x] markdown

## Description

Read data from hdfs file system.

## Supported DataSource Info

| Datasource | Supported Versions |
|------------|--------------------|
| HdfsFile   | hadoop 2.x and 3.x |

## Source Options

| Name                       | Type    | Required | Default                     | Description                                                                                                                                                                                                                                                                                                                                   |
|----------------------------|---------|----------|-----------------------------|-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| path                       | string  | yes      | -                           | The source file path.                                                                                                                                                                                                                                                                                                                         |
| file_format_type           | string  | yes      | -                           | We supported as the following file types:`text` `csv` `parquet` `orc` `json` `excel` `xml` `binary` `markdown`.Please note that, The final file name will end with the file_format's suffix, the suffix of the text file is `txt`.                                                                                                            |
| fs.defaultFS               | string  | yes      | -                           | The hadoop cluster address that start with `hdfs://`, for example: `hdfs://hadoopcluster`                                                                                                                                                                                                                                                     |
| read_columns               | list    | no       | -                           | The read column list of the data source, user can use it to implement field projection.The file type supported column projection as the following shown:[text,json,csv,orc,parquet,excel,xml].Tips: If the user wants to use this feature when reading `text` `json` `csv` files, the schema option must be configured.                       |
| hdfs_site_path             | string  | no       | -                           | The path of `hdfs-site.xml`, used to load ha configuration of namenodes                                                                                                                                                                                                                                                                       |
| delimiter/field_delimiter  | string  | no       | \001 for text and , for csv | Field delimiter, used to tell connector how to slice and dice fields when reading text files. default `\001`, the same as hive's default delimiter                                                                                                                                                                                            |
| row_delimiter              | string  | no       | \n                          | Row delimiter, used to tell connector how to slice and dice rows when reading text files. default `\n`                                                                                                                                                                                                                                        |
| parse_partition_from_path  | boolean | no       | true                        | Control whether parse the partition keys and values from file path. For example if you read a file from path `hdfs://hadoop-cluster/tmp/seatunnel/parquet/name=tyrantlucifer/age=26`. Every record data from file will be added these two fields:[name:tyrantlucifer,age:26].Tips:Do not define partition fields in schema option.            |
| date_format                | string  | no       | yyyy-MM-dd                  | Date type format, used to tell connector how to convert string to date, supported as the following formats:`yyyy-MM-dd` `yyyy.MM.dd` `yyyy/MM/dd` default `yyyy-MM-dd`.Date type format, used to tell connector how to convert string to date, supported as the following formats:`yyyy-MM-dd` `yyyy.MM.dd` `yyyy/MM/dd` default `yyyy-MM-dd` |
| datetime_format            | string  | no       | yyyy-MM-dd HH:mm:ss         | Datetime type format, used to tell connector how to convert string to datetime, supported as the following formats:`yyyy-MM-dd HH:mm:ss` `yyyy.MM.dd HH:mm:ss` `yyyy/MM/dd HH:mm:ss` `yyyyMMddHHmmss` .default `yyyy-MM-dd HH:mm:ss`                                                                                                          |
| time_format                | string  | no       | HH:mm:ss                    | Time type format, used to tell connector how to convert string to time, supported as the following formats:`HH:mm:ss` `HH:mm:ss.SSS`.default `HH:mm:ss`                                                                                                                                                                                       |
| remote_user                | string  | no       | -                           | The login user used to connect to hadoop login name. It is intended to be used for remote users in RPC, it won't have any credentials.                                                                                                                                                                                                        |
| krb5_path                  | string  | no       | /etc/krb5.conf              | The krb5 path of kerberos                                                                                                                                                                                                                                                                                                                     |
| kerberos_principal         | string  | no       | -                           | The principal of kerberos                                                                                                                                                                                                                                                                                                                     |
| kerberos_keytab_path       | string  | no       | -                           | The keytab path of kerberos                                                                                                                                                                                                                                                                                                                   |
| skip_header_row_number     | long    | no       | 0                           | Skip the first few lines, but only for the txt and csv.For example, set like following:`skip_header_row_number = 2`.then Seatunnel will skip the first 2 lines from source files                                                                                                                                                              |
| schema                     | config  | no       | -                           | the schema fields of upstream data                                                                                                                                                                                                                                                                                                            |
| sheet_name                 | string  | no       | -                           | Reader the sheet of the workbook,Only used when file_format is excel.                                                                                                                                                                                                                                                                         |
| xml_row_tag                | string  | no       | -                           | Specifies the tag name of the data rows within the XML file, only used when file_format is xml.                                                                                                                                                                                                                                               |
| xml_use_attr_format        | boolean | no       | -                           | Specifies whether to process data using the tag attribute format, only used when file_format is xml.                                                                                                                                                                                                                                          |
| csv_use_header_line        | boolean | no       | false                       | Whether to use the header line to parse the file, only used when the file_format is `csv` and the file contains the header line that match RFC 4180                                                                                                                                                                                           |
| file_filter_pattern        | string  | no       |                             | Filter pattern, which used for filtering files.                                                                                                                                                                                                                                                                                               |
| filename_extension         | string  | no       | -                           | Filter filename extension, which used for filtering files with specific extension. Example: `csv` `.txt` `json` `.xml`.                                                                                                                                                                                                                       |
| compress_codec             | string  | no       | none                        | The compress codec of files                                                                                                                                                                                                                                                                                                                   |
| archive_compress_codec     | string  | no       | none                        |
| encoding                   | string  | no       | UTF-8                       |                                                                                                                                                                                                                                                                                                                                               |
| null_format                | string  | no       | -                           | Only used when file_format_type is text. null_format to define which strings can be represented as null. e.g: `\N`                                                                                                                                                                                                                            |
| binary_chunk_size          | int     | no       | 1024                        | Only used when file_format_type is binary. The chunk size (in bytes) for reading binary files. Default is 1024 bytes. Larger values may improve performance for large files but use more memory.                                                                                                                                              |
| binary_complete_file_mode  | boolean | no       | false                       | Only used when file_format_type is binary. Whether to read the complete file as a single chunk instead of splitting into chunks. When enabled, the entire file content will be read into memory at once. Default is false.                                                                                                                    |
| common-options             |         | no       | -                           | Source plugin common parameters, please refer to [Source Common Options](../source-common-options.md) for details.                                                                                                                                                                                                                            |
| file_filter_modified_start | string  | no       | -                           | File modification time filter. The connector will filter some files base on the last modification start time (include start time). The default data format is `yyyy-MM-dd HH:mm:ss`.                                                                                                                                                          |
| file_filter_modified_end   | string  | no       | -                           | File modification time filter. The connector will filter some files base on the last modification end time (not include end time). The default data format is `yyyy-MM-dd HH:mm:ss`.                                                                                                                                                          |

### file_format_type [string]

File type, supported as the following file types:

`text` `csv` `parquet` `orc` `json` `excel` `xml` `binary` `markdown`

If you assign file type to `markdown`, SeaTunnel can parse markdown files and extract structured data.
The markdown parser extracts various elements including headings, paragraphs, lists, code blocks, tables, and more.
Each element is converted to a row with the following schema:
- `element_id`: Unique identifier for the element
- `element_type`: Type of the element (Heading, Paragraph, ListItem, etc.)
- `heading_level`: Level of heading (1-6, null for non-heading elements)
- `text`: Text content of the element
- `page_number`: Page number (default: 1)
- `position_index`: Position index within the document
- `parent_id`: ID of the parent element
- `child_ids`: Comma-separated list of child element IDs

Note: Markdown format only supports reading, not writing.

### delimiter/field_delimiter [string]

**delimiter** parameter will deprecate after version 2.3.5, please use **field_delimiter** instead.

### row_delimiter [string]

Only need to be configured when file_format is text

Row delimiter, used to tell connector how to slice and dice rows

default `\n`

### file_filter_pattern [string]

Filter pattern, which used for filtering files.

The pattern follows standard regular expressions. For details, please refer to https://en.wikipedia.org/wiki/Regular_expression.
There are some examples.

File Structure Example:
```
/data/seatunnel/20241001/report.txt
/data/seatunnel/20241007/abch202410.csv
/data/seatunnel/20241002/abcg202410.csv
/data/seatunnel/20241005/old_data.csv
/data/seatunnel/20241012/logo.png
```
Matching Rules Example:

**Example 1**: *Match all .txt files*，Regular Expression:
```
/data/seatunnel/20241001/.*\.txt
```
The result of this example matching is:
```
/data/seatunnel/20241001/report.txt
```
**Example 2**: *Match all file starting with abc*，Regular Expression:
```
/data/seatunnel/20241002/abc.*
```
The result of this example matching is:
```
/data/seatunnel/20241007/abch202410.csv
/data/seatunnel/20241002/abcg202410.csv
```
**Example 3**: *Match all file starting with abc，And the fourth character is either h or g*, the Regular Expression:
```
/data/seatunnel/20241007/abc[h,g].*
```
The result of this example matching is:
```
/data/seatunnel/20241007/abch202410.csv
```
**Example 4**: *Match third level folders starting with 202410 and files ending with .csv*, the Regular Expression:
```
/data/seatunnel/202410\d*/.*\.csv
```
The result of this example matching is:
```
/data/seatunnel/20241007/abch202410.csv
/data/seatunnel/20241002/abcg202410.csv
/data/seatunnel/20241005/old_data.csv
```

### compress_codec [string]

The compress codec of files and the details that supported as the following shown:

- txt: `lzo` `none`
- json: `lzo` `none`
- csv: `lzo` `none`
- orc/parquet:  
  automatically recognizes the compression type, no additional settings required.

### archive_compress_codec [string]

The compress codec of archive files and the details that supported as the following shown:

| archive_compress_codec | file_format       | archive_compress_suffix |
|------------------------|-------------------|-------------------------|
| ZIP                    | txt,json,excel,xml | .zip                    |
| TAR                    | txt,json,excel,xml | .tar                    |
| TAR_GZ                 | txt,json,excel,xml | .tar.gz                 |
| GZ                     | txt,json,excel,xml | .gz                     |
| NONE                   | all                | .*                      |

Note: gz compressed excel file needs to compress the original file or specify the file suffix, such as e2e.xls ->e2e_test.xls.gz

### encoding [string]

Only used when file_format_type is json,text,csv,xml.
The encoding of the file to read. This param will be parsed by `Charset.forName(encoding)`.

### binary_chunk_size [int]

Only used when file_format_type is binary.

The chunk size (in bytes) for reading binary files. Default is 1024 bytes. Larger values may improve performance for large files but use more memory.

### binary_complete_file_mode [boolean]

Only used when file_format_type is binary.

Whether to read the complete file as a single chunk instead of splitting into chunks. When enabled, the entire file content will be read into memory at once. Default is false.

### Tips

> If you use spark/flink, In order to use this connector, You must ensure your spark/flink cluster already integrated hadoop. The tested hadoop version is 2.x. If you use SeaTunnel Engine, It automatically integrated the hadoop jar when you download and install SeaTunnel Engine. You can check the jar package under ${SEATUNNEL_HOME}/lib to confirm this.

## Task Example

### Simple

> This example defines a SeaTunnel synchronization task that  read data from Hdfs and sends it to Hdfs.

```
# Defining the runtime environment
env {
  parallelism = 1
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
  file_format_type = "json"
  fs.defaultFS = "hdfs://namenode001"
  }
  # If you would like to get more information about how to configure seatunnel and see full list of source plugins,
  # please go to https://seatunnel.apache.org/docs/connector-v2/source
}

transform {
  # If you would like to get more information about how to configure seatunnel and see full list of transform plugins,
    # please go to https://seatunnel.apache.org/docs/transform-v2
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

### Filter File

```hocon
env {
  parallelism = 1
  job.mode = "BATCH"
}

source {
  HdfsFile {
    path = "/apps/hive/demo/student"
    file_format_type = "json"
    fs.defaultFS = "hdfs://namenode001"
    // file example abcD2024.csv
    file_filter_pattern = "abc[DX]*.*"
  }
}

sink {
  Console {
  }
}
```

### Multiple Table
```hocon
env {
  parallelism = 1
  job.mode = "BATCH"
}

source {
  HdfsFile {
    tables_configs = [
      {
        schema = {
          table = "student"
        }
        path = "/apps/hive/demo/student"
        file_format_type = "json"
        fs.defaultFS = "hdfs://namenode001"
      },
      {
        schema = {
          table = "teacher"
        }
        path = "/apps/hive/demo/teacher"
        file_format_type = "json"
        fs.defaultFS = "hdfs://namenode001"
      }
    ]
  }
}

sink {
    HdfsFile {
      fs.defaultFS = "hdfs://hadoopcluster"
      path = "/tmp/hive/warehouse/${table_name}"
      file_format_type = "orc"
    }
}

```

## Changelog

<ChangeLog />