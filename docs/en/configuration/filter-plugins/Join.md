## Filter plugin : Join

* Author: InterestingLab
* Homepage: https://interestinglab.github.io/waterdrop
* Version: 1.3.0

### Description

Joining a streaming Dataset/DataFrame with a static Dataset/DataFrame.

### Options

| name | type | required | default value |
| --- | --- | --- | --- |
| [source_field](#source_field-string) | string | no | raw_message |
| [table_name](#table_name-string) | string | yes | - |

##### source_field [string]

Source field, default is `raw_message`.

##### table_name [string]

Static Dataset/DataFrame name.

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
    table_name = "access_log"
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
    table_name = "user_info"
    source_field = "project"
  }
}
```
