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
| [table_name](#table_name-string) | string | yes | - |

##### source_field [string]

源字段，若不配置默认为`raw_message`

##### table_name [string]

临时表表名。

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
    table_name = "spark_project_info"
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
    table_name = "spark_project_info"
    source_field = "project"
  }
}
```