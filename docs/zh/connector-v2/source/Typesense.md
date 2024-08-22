# Typesense

> Typesense 源连接器

## 描述

从 Typesense 读取数据。

## 主要功能

- [x] [批处理](../../concept/connector-v2-features.md)
- [ ] [流处理](../../concept/connector-v2-features.md)
- [ ] [精确一次](../../concept/connector-v2-features.md)
- [x] [Schema](../../concept/connector-v2-features.md)
- [x] [并行度](../../concept/connector-v2-features.md)
- [ ] [支持用户定义的拆分](../../concept/connector-v2-features.md)

## 选项

|     名称     |   类型   | 必填 | 默认值 |
|------------|--------|----|-----|
| hosts      | array  | 是  | -   |
| collection | string | 是  | -   |
| schema     | config | 是  | -   |
| api_key    | string | 否  | -   |
| query      | string | 否  | -   |
| batch_size | int    | 否  | 100 |

### hosts [array]

Typesense的访问地址，格式为 `host:port`，例如：["typesense-01:8108"]

### collection [string]

要写入的集合名，例如：“seatunnel”

### schema [config]

typesense 需要读取的列。有关更多信息，请参阅：[guide](../../concept/schema-feature.md#how-to-declare-type-supported)。

### api_key [config]

typesense 安全认证的 api_key。

### batch_size

读取数据时，每批次查询数量

### 常用选项

Source 插件常用参数，具体请参考 [Source 常用选项](../source-common-options.md)

## 示例

```bash
source {
   Typesense {
      hosts = ["localhost:8108"]
      collection = "companies"
      api_key = "xyz"
      query = "q=*&filter_by=num_employees:>9000"
      schema = {
            fields {
              company_name_list = array<string>
              company_name = string
              num_employees = long
              country = string
              id = string
              c_row = {
                c_int = int
                c_string = string
                c_array_int = array<int>
              }
            }
          }
    }
}
```

