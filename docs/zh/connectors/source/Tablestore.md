import ChangeLog from '../changelog/connector-tablestore.md';

# Tablestore

> Tablestore 源连接器

## 支持的引擎

> Spark<br/>
> Flink<br/>
> SeaTunnel Zeta<br/>

## 描述

从阿里云 Tablestore 读取全量和增量数据。该连接器使用 Tablestore Tunnel 的 `BaseAndStream` 模式，先读取已有数据，再继续消费后续变更。

## 主要特性

- [x] [批](../../introduction/concepts/connector-v2-features.md)
- [x] [流](../../introduction/concepts/connector-v2-features.md)
- [ ] [精确一次](../../introduction/concepts/connector-v2-features.md)
- [x] [cdc](../../introduction/concepts/connector-v2-features.md)
- [x] [列投影](../../introduction/concepts/connector-v2-features.md)
- [x] [并行度](../../introduction/concepts/connector-v2-features.md)
- [ ] [支持用户自定义切片](../../introduction/concepts/connector-v2-features.md)

## 选项

| 参数名            | 类型   | 是否必填 | 默认值 | 描述 |
|-------------------|--------|----------|--------|------|
| end_point         | string | 是       | -      | Tablestore 访问地址，例如 `https://<instance>.<region>.ots.aliyuncs.com`。 |
| instance_name     | string | 是       | -      | Tablestore 实例名称。 |
| access_key_id     | string | 是       | -      | 访问 Tablestore 使用的 AccessKey ID。 |
| access_key_secret | string | 是       | -      | 访问 Tablestore 使用的 AccessKey Secret。 |
| table             | string | 是       | -      | Tablestore 表名。读取多张表时，用英文逗号分隔。 |
| primary_keys      | array  | 是       | -      | 主键名。读取多张表时，需要按 `table` 的顺序为每张表配置一个主键名。 |
| schema            | config | 是       | -      | 输出数据结构。更多详情请参考 [Schema 特性](../../introduction/concepts/schema-feature.md)。 |

## 使用说明

- `job.mode = "BATCH"` 会读取有界数据；`job.mode = "STREAMING"` 会在读取已有数据后继续消费增量记录。
- 当 `table` 配置多张表时，`primary_keys` 的数量必须和表数量一致。例如 `table = "orders,users"` 且两张表都使用 `id` 作为主键字段时，可以配置 `primary_keys = ["id", "id"]`。
- 多表读取共用一个 `schema` 配置，因此这些表的输出字段需要保持兼容。
- 源连接器会根据 Tablestore 的变更记录输出 `INSERT`、`UPDATE_AFTER` 和 `DELETE` 类型的数据。
- 不建议把 `access_key_id` 和 `access_key_secret` 直接写入会提交到代码仓库的任务文件中，建议使用运行时变量替换或部署环境支持的密钥管理方式。

## 示例

```hocon
env {
  parallelism = 1
  job.mode = "STREAMING"
}

source {
  Tablestore {
    end_point = "https://<instance>.<region>.ots.aliyuncs.com"
    instance_name = "<instance-name>"
    access_key_id = "${ACCESS_KEY_ID}"
    access_key_secret = "${ACCESS_KEY_SECRET}"
    table = "orders"
    primary_keys = ["order_id"]
    schema = {
      fields {
        order_id = string
        user_id = string
        amount = double
        updated_at = string
      }
    }
  }
}

sink {
  Console {}
}
```

### 多表示例

```hocon
env {
  parallelism = 2
  job.mode = "STREAMING"
}

source {
  Tablestore {
    end_point = "https://<instance>.<region>.ots.aliyuncs.com"
    instance_name = "<instance-name>"
    access_key_id = "${ACCESS_KEY_ID}"
    access_key_secret = "${ACCESS_KEY_SECRET}"
    table = "orders,users"
    primary_keys = ["id", "id"]
    schema = {
      fields {
        id = string
        value = string
        updated_at = string
      }
    }
  }
}

sink {
  Console {}
}
```

## 变更日志

<ChangeLog />
