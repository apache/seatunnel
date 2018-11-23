## Input plugin : Kudu

* Author: InterestingLab
* Homepage: https://interestinglab.github.io/waterdrop
* Version: 1.0.0

### Description

从[Apache Kudu](https://kudu.apache.org) 表中读取数据.

### Options

| name | type | required | default value |
| --- | --- | --- | --- |
| [kudu_master](#kudu_master-string) | string | yes | - |
| [kudu_table](#kudu_table) | string | yes | - |
| [table_name](#table_name-string) | string | yes | - |


##### kudu_master [string]

kudu的master，多个master以逗号隔开

##### kudu_table [string]

kudu中要读取的表名

##### table_name [string]

获取到的数据，注册成临时表的表名



### Example

```
kudu{
   kudu_master="hadoop01:7051,hadoop02:7051,hadoop03:7051"
   kudu_table="my_kudu_table"
   table_name="reg_table"
 }
```
