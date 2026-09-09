import ChangeLog from '../changelog/connector-firebase.md';

# Firebase

> Firebase Sink Connector

## 描述

Firebase Sink Connector 用于通过 REST API 将批处理和流数据写入 Google Firebase Realtime Database。它支持将 SeaTunnel 内部行数据转换为 JSON 节点结构，并处理 CDC 操作（`INSERT`、`UPDATE`、`DELETE`）。

## 主要特性

- [x] [batch](../../introduction/concepts/connector-v2-features.md)
- [x] [cdc](../../introduction/concepts/connector-v2-features.md)
- [x] [exactly-once](../../introduction/concepts/connector-v2-features.md)

---
## 参数配置

| 参数名称                   | 类型      | 是否必填 | 默认值     | 描述                                                                               |
|:-----------------------|:--------|:---|:--------|:---------------------------------------------------------------------------------|
| `url`                  | String  | 是 | -       | Firebase Realtime Database 的基础 URL（例如：`https://<DATABASE_NAME>.firebaseio.com`）。 |
| `path`                 | String  | 是 | -       | 要写入的 JSON 节点路径（例如：`users` 或 `logs/2026`）。                                        |
| `service_account_path` | String  | 否 | -       | Google 服务账号 JSON 密钥文件的路径。                                                        |
| `credentials`          | String  | 否 | -       | 经过 Base64 编码的服务账号 JSON 凭据内容。                                                     |
| `database_secret`      | String  | 否 | -       | 传统的 Firebase 数据库密钥或 Web API 令牌。                                                  |
| `timeout_ms`           | Integer | 否 | `10000` | HTTP 连接和读取超时时间（毫秒，必须大于 0）。                                                       |
| `primary_keys`         | List    | 否 | -       | 用于标识目标节点 Key 的字段名称列表。                                                            |
| `key_prefix`           | String  | 否 | -       | 拼接在 Key 字符串前的固定前缀表达式。                                                            |
| `key_postfix`          | String  | 否 | -       | 拼接在 Key 字符串后的固定后缀表达式。                                                            |
| `key_delimiter`        | String  | 否 | `"_"`   | 使用复合主键时连接各字段值的分隔符。                                                               |
| `batch_size`           | Integer | 否 | `100`   | 在发送多位置更新 REST 请求或批量写入之前聚合的记录数。                                                   |
| `retry_max`            | Integer | 否 | `3`     | HTTP 写入操作失败时的最大重试次数。                                                             |
| `ignore_null_values`   | Boolean | 否 | `false` | 如果设置为 true，JSON 载荷中将忽略值为 null 的字段。                                               |
| `support_deletes`      | Boolean | 否 | `true`  | 是否处理 `RowKind.DELETE` 和 `RowKind.UPDATE_BEFORE` 记录。                              |
| `common-options`       | Config  | 否 | -       | Sink 通用参数。详情请参考 [Sink Common Options](../common-options/sink-common-options.md)。 |

---
## 数据类型映射

Firebase Sink Connector 将 SeaTunnel 内部数据类型转换为 JSON 节点结构：

| SeaTunnel 数据类型     | Firebase / JSON 数据类型      |
|:-------------------|:--------------------------|
| `STRING`           | `String`                  |
| `INT` / `BIGINT`   | `Number` (Integer)        |
| `FLOAT` / `DOUBLE` | `Number` (Floating point) |
| `BOOLEAN`          | `Boolean`                 |
| `ROW` / `MAP`      | `Object` (Nested node)    |
| `ARRAY`            | `Array`                   |

---
## 配置指南

### 1. 身份验证选项
Connector 支持三种身份验证方式：
- **`service_account_path`**：指向从 **Firebase 控制台 > 项目设置 > 服务账号** 下载的本地服务账号 JSON 密钥文件。
- **`credentials`**：提供 Base64 编码后的服务账号 JSON 内容（适用于 CI/CD 或云端凭据注入）。
- **`database_secret`**：传递传统的数据库密钥或 Web API 令牌。

### 2. 主键与目标节点构建
- **主键 (`primary_keys`)**：Connector 使用指定主键字段的值来构建 Firebase 中的子节点 ID。
- **复合主键**：如果在 `primary_keys` 中指定了多个字段，其值将使用 `key_delimiter`（默认值：`_`）进行连接。
- **格式化节点 Key (`key_prefix` / `key_postfix`)**：可以为生成的 Key 附加自定义的前缀和后缀（例如：前缀为 `user_`，Key 为 `101`，生成的节点 Key 为 `user_101`）。

---

## 示例

### 写入集合节点 (Collection Node)

```hocon
env {
  parallelism = 1
  job.mode = "BATCH"
}

source {
  FakeSource {
    schema = {
      fields {
        id = "int"
        name = "string"
        role = "string"
      }
    }
    rows = [
      {
        kind = INSERT
        fields = [101, "Alice", "Admin"]
      },
      {
        kind = INSERT
        fields = [102, "Bob", "Developer"]
      }
    ]
    plugin_output = "fake_users"
  }
}

sink {
  Firebase {
    plugin_input = "fake_users"
    url = "[https://my-app-default-rtdb.firebaseio.com](https://my-app-default-rtdb.firebaseio.com)"
    path = "users"
    service_account_path = "/etc/seatunnel/firebase-credentials.json"
    primary_keys = ["id"]
    key_prefix = "user_"
    batch_size = 50
    timeout_ms = 5000
  }
}
```

