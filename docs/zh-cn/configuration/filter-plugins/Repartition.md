## Filter plugin : Kv

* Author: InterestingLab
* Homepage: https://interestinglab.github.io/waterdrop
* Version: 1.0.0

### Description

重新给Dataframe分区

### Options

| name | type | required | default value |
| --- | --- | --- | --- |
| [num_partitions](#num_partitions-number) | number | yes | - |

##### num_partitions [number]

分区个数

### Examples

```
Repartition {
    num_partitions = 8
}
```