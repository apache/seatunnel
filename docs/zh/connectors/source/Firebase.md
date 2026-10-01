import ChangeLog from '../changelog/connector-firebase.md';

# Firebase

> Firebase Source Connector

## 描述

Firebase Source Connector 支持通过 REST API 从 Google Firebase Realtime Database 读取数据。它支持根据定义的 Schema 字段，将 JSON 节点结构提取并转换为 SeaTunnel 内部数据行。

## 关键特性

- [x] 批处理 (Batch)
- [ ] 流处理 (Stream)
- [ ] 精确一次 (Exactly-Once)
- [x] 列裁剪 (Column Projection)

---

## 参数项

| 参数名称 | 类型 | 是否必填 | 默认值 | 描述 |
| :--- | :--- | :--- | :--- | :--- |
| `url` | String | 是 | - | Firebase Realtime Database 的基础 URL（例如：`https://<DATABASE_NAME>.firebaseio.com`）。 |
| `path` | String | 是 | - | 要读取的 JSON 节点路径（例如：`users` 或 `logs/2026`）。 |
| `service_account_path` | String | 否 | - | 用于 OAuth2 服务认证的 Google 服务账号 JSON 密钥文件的绝对路径。 |
| `credentials` | String | 否 | - | Base64 编码后的服务账号 JSON 密钥文件内容。 |
| `database_secret` | String | 否 | - | 遗留的 Firebase 数据库密钥（Database Secret）或 Web API Token。 |
| `timeout_ms` | Integer | 否 | `10000` | HTTP 请求超时时间（毫秒）。 |
| `query_params` | Map | 否 | - | 随请求传递的附加 REST API 查询参数。 |
| `schema` | Config | 是 | - | 目标表 Schema 定义，将 JSON 键映射为 SeaTunnel 数据类型。 |
| `common-options` | Config | 否 | - | Source 端通用参数，详情请参考 [Source 通用参数](../common-options/source-common-options.md)。 |

---

## 数据类型映射

Firebase 连接器将传入的 JSON 节点结构转换为 SeaTunnel 内部数据类型：

| Firebase / JSON 类型 | SeaTunnel 数据类型 |
| :--- | :--- |
| `String` | `STRING` |
| `Number` (整数) | `INT` / `BIGINT` |
| `Number` (浮点数) | `FLOAT` / `DOUBLE` |
| `Boolean` | `BOOLEAN` |
| `Object` (嵌套节点) | `ROW` / `MAP` |
| `Array` | `ARRAY` |

---

## 如何配置

### 1. 认证方式
连接器支持三种认证方式：
- **`service_account_path`**：指向从 **Firebase 控制台 > 项目设置 > 服务账号** 下载的本地服务账号 JSON 密钥文件。
- **`credentials`**：提供 Base64 编码后的服务账号 JSON 文件内容（适用于 CI/CD 或云端 Secret 注入）。
- **`database_secret`**：传递遗留的数据库密钥（Database Secret）或 Web API Token。

### 2. 路径与记录结构化
- **集合路径 (`path = "users"`)**：连接器拉取子节点对象（例如 `user_101`、`user_102`），并将每个子节点对象作为单独的一行（Row）输出。
- **单条记录路径 (`path = "users/user_101"`)**：连接器会自动将子标量字段（例如 `name`、`role`）聚合为与 Schema 匹配的单行数据输出。

---

## 示例

### 读取集合路径

```hocon
env {
  parallelism = 1
  job.mode = "BATCH"
}

source {
  Firebase {
    url = "https://my-app-default-rtdb.firebaseio.com"
    path = "users"
    service_account_path = "/etc/seatunnel/firebase-credentials.json"
    timeout_ms = 5000

    schema {
      fields {
        name = "string"
        role = "string"
      }
    }
    plugin_output = "firebase_users"
  }
}

sink {
  Console {
    plugin_input = "firebase_users"
  }
}
```

## 变更日志

<ChangeLog />
