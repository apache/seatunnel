# Maxcompute

> Maxcompute 源连接器

## 描述

用于从 Maxcompute 读取数据.

## 关键特性

- [x] [批处理](../../concept/connector-v2-features.md)
- [ ] [精确一次](../../concept/connector-v2-features.md)
- [ ] [列投影](../../concept/connector-v2-features.md)
- [x] [并行度](../../concept/connector-v2-features.md)
- [ ] [支持用户自定义分片](../../concept/connector-v2-features.md)

## 选项

| 名称           |  类型  | 必需 | 默认值 |
|----------------|--------|----|---------------|
| accessId       | string | 是  | -             |
| accesskey      | string | 是  | -             |
| endpoint       | string | 是  | -             |
| project        | string | 是  | -             |
| table_name     | string | 是  | -             |
| partition_spec | string | 否  | -             |
| split_row      | int    | 否 | 10000         |
| read_columns   | Array  | 否 | -             |
| table_list     | Array  | 否 | -             |
| common-options | string | 否 |               |
| schema         | config | 否 |               |

### accessId [string]

`accessId` 您的 Maxcompute 密钥 Id, 可以从阿里云访问哪个云.

### accesskey [string]

`accesskey` Your Maxcompute 密钥, 可以从阿里云访问哪个云.

### endpoint [string]

`endpoint` 您的 Maxcompute 端点以 http 开头.

### project [string]

`project` 您在阿里云中创建的Maxcompute项目.

### table_name [string]

`table_name` 目标Maxcompute表名，例如：fake.

### partition_spec [string]

`partition_spec` Maxcompute分区表的此规范，例如:ds='20220101'.

### split_row [int]

`split_row` 每次拆分的行数，默认值: 10000.

### read_columns [Array]

`read_columns` 要读取的列，如果未设置，则将读取所有列。例如. ["col1", "col2"]

### table_list [Array]

要读取的表列表，您可以使用此配置代替 `table_name`.

### common options

源插件常用参数, 详见 [源通用选项](../source-common-options.md) .

## 示例

### 表读取

```hocon
source {
  Maxcompute {
    accessId="<your access id>"
    accesskey="<your access Key>"
    endpoint="<http://service.odps.aliyun.com/api>"
    project="<your project>"
    table_name="<your table name>"
    #partition_spec="<your partition spec>"
    #split_row = 10000
    #read_columns = ["col1", "col2"]
  }
}
```

### 使用表列表读取

```hocon
source {
  Maxcompute {
    accessId="<your access id>"
    accesskey="<your access Key>"
    endpoint="<http://service.odps.aliyun.com/api>"
    project="<your project>" # default project
    table_list = [
      {
        table_name = "test_table"
        #partition_spec="<your partition spec>"
        #split_row = 10000
        #read_columns = ["col1", "col2"]
      },
      {
        project = "test_project"
        table_name = "test_table2"
        #partition_spec="<your partition spec>"
        #split_row = 10000
        #read_columns = ["col1", "col2"]
      }
    ]
  }
}
```

