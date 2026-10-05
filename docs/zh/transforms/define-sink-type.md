# 定义写入字段类型

> DefineSinkType：为 sink 建表或写入阶段显式指定字段类型

## 描述

DefineSinkType 转换插件用于定义 sink 字段的目标存储类型，适用于开启 `savemode` 自动建表的场景。

## 参数

| 参数名  | 类型                      | 是否必填 | 默认值 | 说明 |
|:------:|---------------------------|----------|--------|------|
| columns | list<map<string, string>> | 是       |        | 需要定义的列，必须为每一列指定名称和类型 |

## 示例

### 指定部分字段的建表类型

```
transform {
  DefineSinkType {
    columns = [
        {
            column = "c1"
            type = "nvarchar2(10)"
        }
        {
            column = "c2"
            type = "datetime(6)"
        }
        {
            column = "c3"
            type = "your target type"
        }
    ]
  }
}
```
