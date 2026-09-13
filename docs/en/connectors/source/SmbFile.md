import ChangeLog from '../changelog/connector-file-smb.md';

# SmbFile

> SMB file source connector

## Support Those Engines

> Spark<br/>
> Flink<br/>
> SeaTunnel Zeta<br/>

## Key Features

- [x] [batch](../../introduction/concepts/connector-v2-features.md)
- [ ] [stream](../../introduction/concepts/connector-v2-features.md)
- [x] [multimodal](../../introduction/concepts/connector-v2-features.md#multimodal)

  Use binary file format to read and write files in any format, such as videos, pictures, etc. In short, any files can be synchronized to the target place.

- [ ] [exactly-once](../../introduction/concepts/connector-v2-features.md)
- [x] [column projection](../../introduction/concepts/connector-v2-features.md)
- [x] [parallelism](../../introduction/concepts/connector-v2-features.md)
- [ ] [support user-defined split](../../introduction/concepts/connector-v2-features.md)
- [x] file format type
  - [x] text
  - [x] csv
  - [x] json
  - [x] excel
  - [x] xml
  - [x] binary
  - [x] markdown
  - [x] pdf

## Description

Read data from SMB (Server Message Block) file shares. SMB is a network file sharing protocol that allows applications to read and write files on remote servers.

:::tip

If you use spark/flink, In order to use this connector, You must ensure your spark/flink cluster already integrated hadoop. The tested hadoop version is 2.x.

If you use SeaTunnel Engine, It automatically integrated the hadoop jar when you download and install SeaTunnel Engine. You can check the jar package under ${SEATUNNEL_HOME}/lib to confirm this.

:::

## Supported DataSource Info

| Datasource | Supported Versions |                                      Dependency                                       |
|------------|--------------------|----------------------------------------------------------------------------------------|
| SmbFile    | SMB2/SMB3          | [Download](https://mvnrepository.com/artifact/org.apache.seatunnel/connector-file-smb) |

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

| Name                       | Type    | Required | default value                 | Description                                                                                                                                    |
|----------------------------|---------|----------|-------------------------------|------------------------------------------------------------------------------------------------------------------------------------------------|
| host                       | String  | Yes      | -                             | The SMB server host                                                                                                                            |
| port                       | Int     | No       | 445                           | The SMB server port                                                                                                                            |
| user                       | String  | Yes      | -                             | The SMB authentication username                                                                                                                |
| password                   | String  | No       | -                             | The SMB authentication password                                                                                                                |
| domain                     | String  | No       | (empty)                       | The SMB authentication domain (e.g. WORKGROUP)                                                                                                 |
| share                      | String  | Yes      | -                             | The SMB share name to connect to                                                                                                               |
| path                       | String  | Yes      | -                             | The source file path within the share                                                                                                          |
| file_format_type           | String  | Yes      | -                             | Supported file types: `text` `csv` `parquet` `orc` `json` `excel` `xml` `binary` `markdown` `pdf`                                             |
| file_filter_pattern        | String  | No       | -                             | Filter pattern, which used for filtering files.                                                                                                |
| delimiter/field_delimiter  | String  | No       | \001 for text and ',' for csv | Field delimiter, used to tell connector how to slice and dice fields when reading text files.                                                   |
| row_delimiter              | String  | No       | \n                            | Row delimiter, used to tell connector how to slice and dice rows when reading text files.                                                       |
| parse_partition_from_path  | Boolean | No       | true                          | Control whether parse the partition keys and values from file path                                                                              |
| date_format                | String  | No       | yyyy-MM-dd                    | Date type format                                                                                                                               |
| datetime_format            | String  | No       | yyyy-MM-dd HH:mm:ss          | Datetime type format                                                                                                                           |
| time_format                | String  | No       | HH:mm:ss                      | Time type format                                                                                                                               |
| skip_header_row_number     | Long    | No       | 0                             | Skip the first few lines, but only for the txt and csv.                                                                                        |
| schema                     | Config  | No       | -                             | The schema of upstream data                                                                                                                    |
| read_columns               | List    | No       | -                             | The read column list of the data source, user can use it to implement field projection.                                                        |
| sheet_name                 | String  | No       | -                             | Reader the sheet of the workbook, Only used when file_format is excel.                                                                         |
| xml_row_tag                | String  | No       | -                             | Specifies the tag name of the data rows within the XML file, only used when file_format is xml.                                                |
| xml_use_attr_format        | Boolean | No       | -                             | Specifies whether to process data using the tag attribute format, only used when file_format is xml.                                           |
| compress_codec             | String  | No       | None                          | The compress codec of files                                                                                                                    |
| encoding                   | String  | No       | UTF-8                         | The encoding of the file to read                                                                                                               |
| common-options             |         | No       | -                             | Source plugin common parameters, please refer to [Source Common Options](../common-options/source-common-options.md) for details.               |

## How to Create a SMB Data Synchronization Job

The following example demonstrates how to create a data synchronization job that reads data from a SMB share and prints it on the local client:

```bash
# Set the basic configuration of the task to be performed
env {
  parallelism = 1
  job.mode = "BATCH"
}

# Create a source to connect to SMB
source {
  SmbFile {
    host = "192.168.1.100"
    port = 445
    user = seatunnel
    password = pass
    domain = "WORKGROUP"
    share = "data"
    path = "/reports/json"
    file_format_type = "json"
    plugin_output = "smb"
    schema = {
      fields {
        c_string = string
        c_boolean = boolean
        c_int = int
        c_bigint = bigint
        c_float = float
        c_double = double
        c_date = date
        c_timestamp = timestamp
      }
    }
  }
}

# Console printing of the read SMB data
sink {
  Console {
    parallelism = 1
  }
}
```

### Multiple Table

```hocon
SmbFile {
  tables_configs = [
    {
      schema {
        table = "student"
        fields {
          name = string
          age = int
        }
      }
      path = "/data/student"
      host = "192.168.1.100"
      port = 445
      user = seatunnel
      password = pass
      share = "data"
      file_format_type = "parquet"
    },
    {
      schema {
        table = "teacher"
        fields {
          name = string
          age = int
        }
      }
      path = "/data/teacher"
      host = "192.168.1.100"
      port = 445
      user = seatunnel
      password = pass
      share = "data"
      file_format_type = "parquet"
    }
  ]
}
```

## Changelog

<ChangeLog />
