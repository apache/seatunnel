## Sink plugin : File [Flink]

* Author: InterestingLab
* Homepage: https://interestinglab.github.io/waterdrop
* Version: 2.0.0

### Description
写入数据到文件系统

### Options

### Options
| name | type | required | default value |
| --- | --- | --- | --- |
| [format](#format-string) | string | yes | - |
| [path](#path-string) | string | yes | - |
| [write_mode](#write_mode-string)| string | no | - |
| [common-options](#common-options-string)| string | no | - |

##### format [string]

目前支持`csv`、`json`、和 `text`。streaming模式目前只支持`text`

##### path [string]

需要文件路径，hdfs文件以hdfs://开头，本地文件以file://开头。

##### write_mode [string]

- NO_OVERWRITE 
  - 不覆盖，路径存在报错
- OVERWRITE 
  - 覆盖，路径存在则先删除再写入
  
##### common options [string]

`Sink` 插件通用参数，详情参照 [Sink Plugin](/zh-cn/v2/flink/configuration/sink-plugins/)

### Examples

```
  FileSink {
    format = "json"
    path = "hdfs://localhost:9000/flink/output/"
    write_mode = "OVERWRITE"
  }
```