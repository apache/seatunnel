## Filter plugin : Join

* Author: InterestingLab
* Homepage: https://interestinglab.github.io/waterdrop
* Version: 1.3.0

### Description

和指定的临时表进行Join操作, 目前仅支持Stream-static Inner Joins

### Options

| name | type | required | default value |
| --- | --- | --- | --- |
| [source_field](#source_field-string) | string | no | raw_message |
| [source_table_name](#source_table_name-string) | string | yes | - |
| [common-options](#common-options-string)| string | no | - |


##### source_field [string]

源字段，若不配置默认为`raw_message`

##### source_table_name [string]

临时表表名

##### common options [string]

`Filter` 插件通用参数，详情参照 [Filter Plugin](/zh-cn/configuration/filter-plugin)


### Examples

```
input {
  fakestream {
    content = ["Hello World,Waterdrop"]
    rate = 1
  }

  mysql {
    url = "jdbc:mysql://localhost:3306/info"
    table = "project_info"
    result_table_name = "spark_project_info"
    user = "username"
    password = "password"
  }
}

filter {
  split {
    fields = ["msg", "project"]
    delimiter = ","
  }

  join {
    source_table_name = "spark_project_info"
    source_field = "project"
  }
}
```