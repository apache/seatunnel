## Filter plugin : Repartition

* Author: InterestingLab
* Homepage: https://interestinglab.github.io/waterdrop
* Version: 1.0.0

### Description

调整数据处理的分区个数，并行度。这个filter主要是为了调节数据处理性能，不对数据本身做任何处理。

### Options

| name | type | required | default value |
| --- | --- | --- | --- |
| [num_partitions](#num_partitions-number) | number | yes | - |
| [common-options](#common-options-string)| string | no | - |


##### num_partitions [number]

目标分区个数

##### common options [string]

`Filter` 插件通用参数，详情参照 [Filter Plugin](/zh-cn/configuration/filter-plugin)


### Examples

```
repartition {
    num_partitions = 8
}
```