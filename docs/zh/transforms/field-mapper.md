# 字段映射

> 字段映射转换插件

## 描述

添加输入模式和输出模式映射

## 属性

|      名称      |   类型   | 是否必须 | 默认值 |
|--------------|--------|------|-----|
| field_mapper | Object | yes  |     |

### field_mapper [config]

指定输入和输出之间的字段映射关系

### common options [config]

转换插件的常见参数, 请参考  [Transform Plugin](common-options.md) 了解详情

## 示例

源端数据读取的表格如下：

| id |   name   | age | card |
|----|----------|-----|------|
| 1  | Joy Ding | 20  | 123  |
| 2  | May Ding | 20  | 123  |
| 3  | Kin Dom  | 20  | 123  |
| 4  | Joy Dom  | 20  | 123  |

我们想要删除 `age` 字段，并更新字段顺序为 `id`、`card`、`name`，同时将 `name` 重命名为 `new_name`。我们可以像这样添加 `FieldMapper` 转换：

```
transform {
  FieldMapper {
    plugin_input = "fake"
    plugin_output = "fake1"
    field_mapper = {
        id = id
        card = card
        name = new_name
    }
  }
}
```

那么结果表 `fake1` 中的数据将会像这样：

| id | card | new_name |
|----|------|----------|
| 1  | 123  | Joy Ding |
| 2  | 123  | May Ding |
| 3  | 123  | Kin Dom  |
| 4  | 123  | Joy Dom  |

## 更新日志

### 新版本

- 添加复制转换连接器

