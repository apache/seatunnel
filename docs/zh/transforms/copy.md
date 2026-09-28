# 复制

> 复制转换插件

## 描述

将字段复制到一个新字段。

## 属性

|   名称   |   类型   | 是否必须 | 默认值 |
|--------|--------|------|-----|
| fields | Object | yes  |     |
| src_field | String | no  |     |
| dest_field | String | no  |     |

### fields [config]

指定输入和输出之间的字段复制关系

### src_field [string]（已废弃）

想要复制的源字段。这是 `fields` 的废弃单字段替代写法，新配置请使用 `fields`。

使用 `src_field` 时必须同时设置 `dest_field`，且两者不能与 `fields` 同时使用。

### dest_field [string]（已废弃）

将 `src_field` 复制到的目标字段。当配置了 `src_field` 时必须设置。

### 常见选项 [string]

转换插件的常见参数, 请参考  [Transform Plugin](common-options/common-options.md) 了解详情。

## 示例

从源读取的数据是这样的一个表:

|   name   | age | card |
|----------|-----|------|
| Joy Ding | 20  | 123  |
| May Ding | 20  | 123  |
| Kin Dom  | 20  | 123  |
| Joy Dom  | 20  | 123  |

想要将字段 `name`、`age` 复制到新的字段 `name1`、`name2`、`age1`，我们可以像这样添加 `Copy` 转换：

```
transform {
  Copy {
    plugin_input = "fake"
    plugin_output = "fake1"
    fields {
      name1 = name
      name2 = name
      age1 = age
    }
  }
}
```

那么结果表 `fake1` 中的数据将会像这样：

|   name   | age | card |  name1   |  name2   | age1 |
|----------|-----|------|----------|----------|------|
| Joy Ding | 20  | 123  | Joy Ding | Joy Ding | 20   |
| May Ding | 20  | 123  | May Ding | May Ding | 20   |
| Kin Dom  | 20  | 123  | Kin Dom  | Kin Dom  | 20   |
| Joy Dom  | 20  | 123  | Joy Dom  | Joy Dom  | 20   |

## 更新日志

### 新版本

- 添加复制转换连接器
- 支持将字段复制到新字段

