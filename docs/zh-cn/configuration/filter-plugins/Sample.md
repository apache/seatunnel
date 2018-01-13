## Filter plugin : Sample

* Author: InterestingLab
* Homepage: https://interestinglab.github.io/waterdrop
* Version: 1.0.0

### Description

对原始数据集进行抽样

### Options

| name | type | required | default value |
| --- | --- | --- | --- |
| [fraction](#fraction-number) | number | no | 0.1 |
| [limit](#limit-number) | number | no | -1 |

##### fraction [number]

数据抽取的比例，例如fraction=0.8，就是抽取其中80%

##### limit [number]

数据抽取后，限制数据条数，其中`-1`代表全量抽样数据
