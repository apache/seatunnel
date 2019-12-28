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
| [common-options](#common-options-string)| string | no | - |


##### fraction [number]

数据采样的比例，例如fraction=0.8，就是抽取其中80%的数据

##### limit [number]

数据采样后的条数，其中`-1`代表不限制

##### common options [string]

`Filter` 插件通用参数，详情参照 [Filter Plugin](/zh-cn/configuration/filter-plugin)


### Examples

```
sample {
    fraction = 0.8
}
```

> 抽取80%的数据
