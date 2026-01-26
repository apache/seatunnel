import ChangeLog from '../changelog/connector-tablestore.md';

# Tablestore

> Tablestore 源连接器

## 描述

从阿里云 Tablestore 读取数据，支持全量和 CDC。

## 关键特性

- [ ] [批](../../introduction/concepts/connector-v2-features.md)
- [X] [流](../../introduction/concepts/connector-v2-features.md)
- [ ] [精确一次](../../introduction/concepts/connector-v2-features.md)
- [ ] [列投影](../../introduction/concepts/connector-v2-features.md)
- [ ] [并行性](../../introduction/concepts/connector-v2-features.md)
- [ ] [支持用户自定义split](../../introduction/concepts/connector-v2-features.md)

## 选项

| 参数名               | 类型     | 必须 | 默认值 | 描述                                                                        |
|-------------------|--------|----|-----|---------------------------------------------------------------------------|
| end_point         | string | 是  | -   | Tablestore 的端点                                                            |
| instance_name     | string | 是  | -   | Tablestore 的实例名称                                                          |
| access_key_id     | string | 是  | -   | Tablestore 的访问 ID                                                         |
| access_key_secret | string | 是  | -   | Tablestore 的访问密钥                                                          |
| table             | string | 是  | -   | Tablestore 的表名                                                            |
| primary_keys      | array  | 是  | -   | 表的主键，只需添加一个唯一的主键                                                          |
| schema            | config | 是  | -   | 数据的结构。更多详情请参考 [Schema 特性](../../introduction/concepts/schema-feature.md)。 |

### end_point [string]

Tablestore 的端点。

### instance_name [string]

Tablestore 的实例名称。

### access_key_id [string]

Tablestore 的访问 ID。

### access_key_secret [string]

Tablestore 的访问密钥。

### table [string]

Tablestore 的表名。

### primary_keys [array]

表的主键，只需添加一个唯一的主键。

### schema [Config]

数据的结构。更多详情请参考 [Schema 特性](../../introduction/concepts/schema-feature.md)。

## 示例

```bash
env {
  parallelism = 1
  job.mode = "STREAMING"
}

source {
  # 这是一个示例源插件 **仅用于测试和演示源插件功能**
  Tablestore {
    end_point = "https://****.cn-zhangjiakou.tablestore.aliyuncs.com"
    instance_name = "****"
    access_key_id="***************2Ag5"
    access_key_secret="***********2Dok"
    table="test"
    primary_keys=["id"]
    schema={
        fields {
            id = string
            name = string
        }
    }
  }
}

sink {
  MongoDB{
    uri = "mongodb://localhost:27017"
    database = "test"
    collection = "test"
    primary-key = ["id"]
    schema = {
      fields {
        id = string
        name = string
      }
    }
  }
}
```

## 变更日志

<ChangeLog />

