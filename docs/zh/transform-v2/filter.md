# 过滤器

> 过滤器转换插件

## 描述

过滤字段

## 属性

|       名称       |  类型   | 是否必须 | 默认值 |
|----------------|-------|------|-----|
| include_fields | array | no   |     |
| exclude_fields | array | no   |     |

### include_fields [array]

需要保留的字段列表。不在列表中的字段将被删除。

### exclude_fields [array]

需要删除的字段列表。不在列表中的字段将被保留。

注意，`include_fields` 和 `exclude_fields` 两个属性中，必须设置一个且只能设置一个

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

我们想要保留字段 `name`, `card`，我们可以像这样添加 `Filter` 转换:

```
transform {
  Filter {
    plugin_input = "fake"
    plugin_output = "fake1"
    include_fields = [name, card]
  }
}
```

我们也可以通过删除字段 `age` 来实现， 我们可以添加一个 `Filter` 转换，并设置exclude_fields：

```
transform {
  Filter {
    plugin_input = "fake"
    plugin_output = "fake1"
    exclude_fields = [age]
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

