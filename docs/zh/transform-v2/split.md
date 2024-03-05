# 拆分

> 拆分转换插件

## 描述

拆分一个字段为多个字段。

## 属性

|      名称       |   类型   | 是否必须 | 默认值 |
|---------------|--------|------|-----|
| separator     | string | yes  |     |
| split_field   | string | yes  |     |
| output_fields | array  | yes  |     |

### separator [string]

拆分内容的分隔符

### split_field[string]

需要拆分的字段

### output_fields[array]

拆分后的结果字段

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

我们想要将 `name` 字段拆分为 `first_name` 和 `second_name`，我们可以像这样添加 `Split` 转换：

```
transform {
  Split {
    source_table_name = "fake"
    result_table_name = "fake1"
    separator = " "
    split_field = "name"
    output_fields = [first_name, second_name]
  }
}
```

那么结果表 `fake1` 中的数据将会像这样：

|   name   | age | card | first_name | last_name |
|----------|-----|------|------------|-----------|
| Joy Ding | 20  | 123  | Joy        | Ding      |
| May Ding | 20  | 123  | May        | Ding      |
| Kin Dom  | 20  | 123  | Kin        | Dom       |
| Joy Dom  | 20  | 123  | Joy        | Dom       |

## 更新日志

### 新版本

- 添加拆分转换连接器

