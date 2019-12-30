## Output plugin : File

* Author: InterestingLab
* Homepage: https://interestinglab.github.io/waterdrop
* Version: 1.0.0

### Description

Write Rows to local file system.

### Options

| name | type | required | default value |
| --- | --- | --- | --- |
| [options](#options-object) | object | no | - |
| [partition_by](#partition_by-array) | array | no | - |
| [path](#path-string) | string | yes | - |
| [path_time_format](#path_time_format-string) | string | no | yyyyMMddHHmmss |
| [save_mode](#save_mode-string) | string | no | error |
| [serializer](#serializer-string) | string | no | json |

##### options [object]

Custom parameters.

##### partition_by [array]

Partition the data based on the fields.

##### path [string]

Output File path. Start with `file://`.

##### path_time_format [string]

If `path` contains time variable, such as `xxxx-${now}`, `path_time_format` can be used to specify the format of path, default is `yyyy.MM.dd`. The commonly used time formats are listed below:


| Symbol | Description |
| --- | --- |
| y | Year |
| M | Month |
| d | Day of month |
| H | Hour in day (0-23) |
| m | Minute in hour |
| s | Second in minute |

The detailed time format syntax:[Java SimpleDateFormat](https://docs.oracle.com/javase/tutorial/i18n/format/simpleDateFormat.html).

##### save_mode [string]

Save mode, supports `overwrite`, `append`, `ignore` and `error`. The detail of save_mode see [save-modes](http://spark.apache.org/docs/2.2.0/sql-programming-guide.html#save-modes).

##### serializer [string]

Serializer, supports `csv`, `json`, `parquet` and `text`.


### Example

```
file {
    path = "file:///var/logs"
    serializer = "text"
}
```
