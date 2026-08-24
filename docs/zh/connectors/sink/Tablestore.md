import ChangeLog from '../changelog/connector-tablestore.md';

# Tablestore

> Tablestore Sink 连接器

## 支持的引擎

> Spark<br/>
> Flink<br/>
> SeaTunnel Zeta<br/>

## 描述

将 SeaTunnel 数据写入阿里云 Tablestore。

## 主要特性

- [ ] [精确一次](../../introduction/concepts/connector-v2-features.md)
- [ ] [cdc](../../introduction/concepts/connector-v2-features.md)
- [ ] [支持多表写入](../../introduction/concepts/connector-v2-features.md)
- [ ] [定时刷新](../../introduction/concepts/connector-v2-features.md)

## 数据类型映射

| SeaTunnel 类型                         | Tablestore 普通属性列类型 | Tablestore 主键列类型 |
|----------------------------------------|---------------------------|-----------------------|
| `INT`, `TINYINT`, `SMALLINT`, `BIGINT` | `INTEGER`                 | `INTEGER`             |
| `FLOAT`, `DOUBLE`, `DECIMAL`           | `DOUBLE`                  | `STRING`              |
| `STRING`, `DATE`, `TIME`, `TIMESTAMP`  | `STRING`                  | `STRING`              |
| `BOOLEAN`                              | `BOOLEAN`                 | `STRING`              |
| `BYTES`                                | `BINARY`                  | `BINARY`              |

## 选项

| 参数名            | 类型   | 是否必填 | 默认值 | 描述 |
|-------------------|--------|----------|--------|------|
| end_point         | string | 是       | -      | Tablestore 访问地址。 |
| instance_name     | string | 是       | -      | Tablestore 实例名称。 |
| access_key_id     | string | 是       | -      | 访问 Tablestore 使用的 AccessKey ID。 |
| access_key_secret | string | 是       | -      | 访问 Tablestore 使用的 AccessKey Secret。 |
| table             | string | 是       | -      | 目标 Tablestore 表名。 |
| primary_keys      | array  | 是       | -      | 目标表的主键字段名。 |
| schema            | config | 是       | -      | 输入数据结构。主键字段也必须包含在 `schema.fields` 中。 |
| batch_size        | int    | 否       | 25     | 单次批量写入的最大数据条数。 |
| common-options    | config | 否       | -      | Sink 通用选项。 |

## 使用说明

- `primary_keys` 可以包含一个或多个主键字段。这些字段会写为 Tablestore 主键列，其余字段会写为普通属性列。
- Sink 使用 Tablestore `RowPutChange` 写入，并使用 `RowExistenceExpectation.IGNORE`。当上游发送 `DELETE` 类型数据时，当前 Sink 不会删除 Tablestore 中的行。
- `batch_size` 控制缓存多少行后刷新；任务关闭时，写入器也会刷新剩余数据。
- 不建议把 `access_key_id` 和 `access_key_secret` 直接写入会提交到代码仓库的任务文件中，建议使用运行时变量替换或部署环境支持的密钥管理方式。

### common options [config]

Sink 插件通用参数，请参考 [Sink Common Options](../common-options/sink-common-options.md)。

## 示例

```hocon
env {
  parallelism = 1
  job.mode = "BATCH"
}

source {
  FakeSource {
    row.num = 2
    schema = {
      fields {
        order_id = string
        user_id = string
        amount = double
      }
    }
    rows = [
      {
        fields = ["order-1", "user-1", 99.5]
      },
      {
        fields = ["order-2", "user-2", 20.0]
      }
    ]
  }
}

sink {
  Tablestore {
    end_point = "https://<instance>.<region>.ots.aliyuncs.com"
    instance_name = "<instance-name>"
    access_key_id = "${ACCESS_KEY_ID}"
    access_key_secret = "${ACCESS_KEY_SECRET}"
    table = "orders"
    primary_keys = ["order_id"]
    batch_size = 25
    schema = {
      fields {
        order_id = string
        user_id = string
        amount = double
      }
    }
  }
}
```

## 变更日志

<ChangeLog />
