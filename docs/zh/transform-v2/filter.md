# 过滤器

> 过滤器转换插件

## 描述

过滤字段

## 属性

|   名称   |  类型   | 是否必须 | 默认值 |
|--------|-------|------|-----|
| fields | array | yes  |     |

### fields [array]

需要保留的字段列表。不在列表中的字段将被删除。

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

我们想要删除字段 `age`，我们可以像这样添加 `Filter` 转换

```
transform {
  Filter {
    source_table_name = "fake"
    result_table_name = "fake1"
    fields = [name, card]
  }
}
```

那么结果表 `fake1` 中的数据将会像这样：

|   name   | card |
|----------|------|
| Joy Ding | 123  |
| May Ding | 123  |
| Kin Dom  | 123  |
| Joy Dom  | 123  |

## 更新日志

### 新版本

- 添加过滤转器换连接器

