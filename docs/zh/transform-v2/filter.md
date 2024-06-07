# 过滤器

> 过滤器转换插件

## 描述

过滤字段

## 属性

|   名称   |  类型   | 是否必须 | 默认值  |
|--------|-------|------|------|
| fields | array | yes  |      |
| mode   | array | no   | KEEP |

### fields [array]

需要处理的字段列表。列表中的字段根据 `mode` 参数保留或删除。

### mode [string]

Filter 的过滤模式，默认值为 `KEEP`，表示只保留 `fields` 列表中的字段，不在列表中的字段将被删除。如果值为 `DELETE`，则 `fields` 列表中的字段将被删除，不在列表中的字段将被保留。

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

我们想要删除字段 `age`，我们可以像这样添加 `Filter` 转换:

```
transform {
  Filter {
    source_table_name = "fake"
    result_table_name = "fake1"
    fields = [name, card]
  }
}
```

我们也可以通过删除字段 `age` 来实现， 我们可以添加一个 `Filter` 转换，并将 `mode` 字段设置为 `DELETE`，像这样：

```
transform {
  Filter {
    source_table_name = "fake"
    result_table_name = "fake1"
    fields = [age]
    mode = "DELETE"
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

