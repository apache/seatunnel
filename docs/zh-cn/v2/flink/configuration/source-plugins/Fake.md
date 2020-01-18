## Source plugin : Fake [Flink]

* Author: InterestingLab
* Homepage: https://interestinglab.github.io/waterdrop
* Version: 2.0.0

### Description
> Fake Source主要用于自动生成数据，数据只有两列，第一列为String类型，内容为["Gary", "Ricky Huo", "Kid Xiong"]中随机一个，第二列为Long类型，为当前的13位时间戳，以此作为输入来对Waterdrop进行功能验证，测试等。

### Options
| name | type | required | default value |
| --- | --- | --- | --- |
| [common-options](#common-options-string)| string | no | - |

##### common options [string]

`Source` 插件通用参数，详情参照 [Source Plugin](README.md)


### Examples
```
source {
    FakeSourceStream {
      result_table_name = "fake"
      field_name = "name,age"
    }
}
```
