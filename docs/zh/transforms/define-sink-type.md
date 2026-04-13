# Define Sink Type

> Define sink type transform plugin

## Description

用于定义 sink 字段存储类型，对于 savemode 开启自动建表时有效

## Options

|  name   | type                      | required | default value | Description        |
|:-------:|---------------------------|----------|---------------|--------------------|
| columns | list<map<string, string>> | yes      |               | 需要定义的列，必须设置列的名称和类型 |

## Examples

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
