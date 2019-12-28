## Output plugin : Kudu

* Author: InterestingLab
* Homepage: https://interestinglab.github.io/waterdrop
* Version: 1.1.2

### Description

写入数据到[Apache Kudu](https://kudu.apache.org)表中

### Options

| name | type | required | default value |
| --- | --- | --- | --- |
| [kudu_master](#kudu_master-string) | string | yes | - |
| [kudu_table](#kudu_table) | string | yes | - |
| [mode](#mode-string) | string | no | insert |
| [common-options](#common-options-string)| string | no | - |


##### kudu_master [string]

kudu的master，多个master以逗号隔开

##### kudu_table [string]

kudu中要写入的表名,表必须已经存在

##### mode [string]

写入kudu中采取的模式,支持 insert|update|upsert|insertIgnore,默认为insert
insert和insertIgnore :insert在遇见主键冲突将会报错，insertIgnore不会报错，将会舍弃这条数据
update和upsert :update找不到要更新的主键将会报错，upsert不会，将会把这条数据插入

##### common options [string]

`Output` 插件通用参数，详情参照 [Output Plugin](/zh-cn/configuration/output-plugin)


### Example

```
kudu{
   kudu_master="hadoop01:7051,hadoop02:7051,hadoop03:7051"
   kudu_table="my_kudu_table"
   mode="upsert"
 }
```
