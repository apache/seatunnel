## Input plugin : Kudu

* Author: InterestingLab
* Homepage: https://interestinglab.github.io/waterdrop
* Version: 1.1.2

### Description

从[Apache Kudu](https://kudu.apache.org) 表中读取数据.

### Options

| name | type | required | default value |
| --- | --- | --- | --- |
| [kudu_master](#kudu_master-string) | string | yes | - |
| [kudu_table](#kudu_table) | string | yes | - |
| [common-options](#common-options-string)| string | yes | - |


##### kudu_master [string]

kudu的master，多个master以逗号隔开

##### kudu_table [string]

kudu中要读取的表名

##### common options [string]

`Input` 插件通用参数，详情参照 [Input Plugin](/zh-cn/configuration/input-plugin)


### Example

```
kudu{
   kudu_master="hadoop01:7051,hadoop02:7051,hadoop03:7051"
   kudu_table="my_kudu_table"
   result_table_name="reg_table"
 }
```
