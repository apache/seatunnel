# 替换

> 替换转换插件

## 描述

检查给定字段中的字符串值，并用给定的替换项替换与给定字符串字面量或正则表达式匹配的字符串值的子字符串。

## 属性

|      名称       |   类型    | 是否必须 |  默认值  |
|---------------|---------|------|-------|
| replace_field | string  | yes  |       |
| pattern       | string  | yes  | -     |
| replacement   | string  | yes  | -     |
| is_regex      | boolean | no   | false |
| replace_first | boolean | no   | false |

### replace_field [string]

需要替换的字段

### pattern [string]

将被替换的旧字符串

### replacement [string]

用于替换的新字符串

### is_regex [boolean]

使用正则表达式进行字符串匹配

### replace_first [boolean]

是否替换第一个匹配字符串。仅在 `is_regex = true` 时使用。

### common options [string]

转换插件的常见参数, 请参考  [Transform Plugin](common-options.md) 了解详情

## 示例

源端数据读取的表格如下：

|   name   | age | card |
|----------|-----|------|
| Joy Ding | 20  | 123  |
| May Ding | 20  | 123  |
| Kin Dom  | 20  | 123  |
| Joy Dom  | 20  | 123  |

我们想要将 `name` 字段中的字符 ``替换为 `_`。然后我们可以添加一个 `Replace` 转换，像这样：

```
transform {
  Replace {
    plugin_input = "fake"
    plugin_output = "fake1"
    replace_field = "name"
    pattern = " "
    replacement = "_"
    is_regex = true
  }
}
```

那么结果表 `fake1` 中的数据将会更新为：

|   name   | age | card |
|----------|-----|------|
| Joy_Ding | 20  | 123  |
| May_Ding | 20  | 123  |
| Kin_Dom  | 20  | 123  |
| Joy_Dom  | 20  | 123  |

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
        name = "string"
      }
    }
  }
}

transform {
  Replace {
    plugin_input = "fake"
    plugin_output = "fake1"
    replace_field = "name"
    pattern = ".+"
    replacement = "b"
    is_regex = true
  }
}

sink {
  Console {
    plugin_input = "fake1"
  }
}
```

## 更新日志

### 新版本

- 添加替换转换连接器

