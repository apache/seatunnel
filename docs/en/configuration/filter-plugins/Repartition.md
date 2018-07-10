## Filter plugin : Repartition

* Author: InterestingLab
* Homepage: https://interestinglab.github.io/waterdrop
* Version: 1.0.0

### Description

Adjust the number of partition for data processing. This filter is mainly to adjust the data processing performance, do not do any processing on the Rows.


### Options

| name | type | required | default value |
| --- | --- | --- | --- |
| [num_partitions](#num_partitions-number) | number | yes | - |

##### num_partitions [number]

Partition number.

### Examples

```
repartition {
    num_partitions = 8
}
```