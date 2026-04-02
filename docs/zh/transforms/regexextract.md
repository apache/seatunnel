# 正则提取

> 正则提取转换插件

## 描述

`RegexExtract` 转换插件使用正则表达式从指定字段中提取数据，并将提取的值输出到新字段中。它支持正则表达式中的捕获组，并允许在模式不匹配时为每个输出字段设置默认值。

## 属性

| 名称              | 类型       | 是否必须     | 默认值   |
|-----------------|----------|----------|-------|
| source_field    | string   | yes      |       |
| regex_pattern   | string   | yes      |       |
| output_fields   | array    | yes      |       |
| default_values  | array    | no       |       |

### source_field [string]

要提取数据的源字段名称。

### regex_pattern [string]

带有捕获组的正则表达式模式。捕获组的数量必须与输出字段的数量匹配。

### output_fields [array]

提取值的输出字段名称。大小必须与正则表达式模式中的捕获组数量匹配。

### default_values [array]

当正则表达式模式不匹配或源字段为 null 时，输出字段的默认值。如果提供，大小必须与输出字段数量匹配。


## 示例

源端数据读取的表格如下：

| id | email              | log_entry                                            |
|----|--------------------|------------------------------------------------------|
| 1  | user1@example.com  | 2023-12-01 10:30:45 INFO User login successful       |
| 2  | admin@test.org     | 2023-12-01 11:15:22 ERROR Database connection failed |
| 3  | guest@domain.net   | 2023-12-01 12:00:00 WARN Memory usage high           |

我们想要从 `email` 字段中提取用户名、域名和顶级域名：

```
transform {
  RegexExtract {
    plugin_input = "fake"
    plugin_output = "regex_result"
    source_field = "email"
    regex_pattern = "([^@]+)@([^.]+)\\.(.+)"
    output_fields = ["username", "domain", "tld"]
    default_values = ["unknown", "unknown", "unknown"]
  }
}
```

那么结果表 `regex_result` 中的数据将会更新为：

| id | email              | log_entry                                            | username | domain  | tld |
|----|--------------------|------------------------------------------------------|----------|---------|-----|
| 1  | user1@example.com  | 2023-12-01 10:30:45 INFO User login successful       | user1    | example | com |
| 2  | admin@test.org     | 2023-12-01 11:15:22 ERROR Database connection failed | admin    | test    | org |
| 3  | guest@domain.net   | 2023-12-01 12:00:00 WARN Memory usage high           | guest    | domain  | net |

## 作业配置示例

```
env {
  job.mode = "BATCH"
}

source {
  FakeSource {
    plugin_output = "fake"
    row.num = 100
    schema = {
      fields {
        id = "int"
        email = "string"
        log_entry = "string"
      }
    }
    rows = [
      {
          kind = INSERT,
          fields = [1, "user1@example.com", "2023-12-01 10:30:45 INFO User login successful"]
      },
      {
        kind = INSERT,
        fields = [2, "admin@test.org", "2023-12-01 11:15:22 ERROR Database connection failed"]
      },
      {
        kind = INSERT,
        fields = [3, "guest@domain.net", "2023-12-01 12:00:00 WARN Memory usage high"]
      }
    ]
  }
}

transform {
  RegexExtract {
    plugin_input = "fake"
    plugin_output = "regex_result"
    source_field = "email"
    regex_pattern = "([^@]+)@([^.]+)\\.(.+)"
    output_fields = ["username", "domain", "tld"]
    default_values = ["unknown", "unknown", "unknown"]
  }
}

sink {
  Console {
    plugin_input = "regex_result"
  }
}
```

## 更新日志

