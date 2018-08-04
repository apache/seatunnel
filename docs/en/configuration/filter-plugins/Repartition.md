## Filter plugin : Repartition

* Author: InterestingLab
* Homepage: https://interestinglab.github.io/waterdrop
* Version: 1.0.0

### Description

Adjust the number of underlying spark rdd partition to increase or decrease degree of parallelism. This filter is mainly to adjust the data processing performance.


### Options

| name | type | required | default value |
| --- | --- | --- | --- |
| [num_partitions](#num_partitions-number) | number | yes | - |

##### num_partitions [number]

Target partition number.

### Examples

```
repartition {
    num_partitions = 8
}
```
