import ChangeLog from '../changelog/connector-couchbase.md';

# Couchbase

> Couchbase 接收器连接器

## 支持的引擎

> Spark<br/>
> Flink<br/>
> SeaTunnel Zeta<br/>

## 主要特性

- [ ] [exactly-once](../../introduction/concepts/connector-v2-features.md)
- [ ] [cdc](../../introduction/concepts/connector-v2-features.md)
- [ ] [支持多表写入](../../introduction/concepts/connector-v2-features.md)

## 描述

将数据写入 [Couchbase](https://www.couchbase.com/) 集合。
每行数据以 JSON 文档形式存储。文档键由 `primary-key` 字段的值使用**长度前缀规范编码**构建
（格式为 `<长度>:<值>`，各分量以 `#` 分隔，例如 `3:foo#3:bar`）。
此编码不会产生碰撞：包含分隔符或特殊字符的值不会与其他不同的元组生成相同的键。
未配置时使用随机 UUID 作为文档键。

连接器支持：

- **Upsert 模式** — 插入或替换已有文档。
- **批量刷写** — 在内存中缓冲数据，按行数或时间阈值刷写。
- **重试机制** — 写入失败时采用**线性退避**重试（第 n 次重试等待 `retry.interval × n` 毫秒）。

## 支持的数据源信息

使用 Couchbase 连接器需要以下依赖。

| 数据源     | 支持版本     | 依赖 |
|------------|--------------|------|
| Couchbase  | Server 7.x+  | [下载](https://mvnrepository.com/artifact/org.apache.seatunnel/connector-couchbase) |

## 数据库依赖

> 运行作业前请安装连接器插件：

```shell
sh bin/install-plugin.sh ${version}
```

## 数据类型映射

| SeaTunnel 数据类型             | Couchbase JSON 值         |
|-------------------------------|---------------------------|
| BOOLEAN                       | Boolean                   |
| TINYINT / SMALLINT / INT      | Number (整数)             |
| BIGINT                        | Number (长整数)           |
| FLOAT / DOUBLE                | Number (浮点数)           |
| DECIMAL                       | String (精确小数，如 `"123.456"`) |
| STRING                        | String                    |
| DATE / TIME / TIMESTAMP       | String (ISO-8601)         |
| BYTES                         | String (Base64 编码)      |
| ARRAY                         | Array（元素递归转换）     |
| MAP                           | Object（键强制转为 String，值递归转换）|
| ROW                           | Object（嵌套 JSON 文档）  |
| NULL                          | null                      |

## 接收器选项

| 名称                   | 类型           | 是否必填 | 默认值     | 描述 |
|------------------------|----------------|----------|------------|------|
| connection.string      | String         | 是       | -          | Couchbase 连接字符串，例如 `couchbase://localhost`。 |
| username               | String         | 是       | -          | Couchbase 用户名。 |
| password               | String         | 是       | -          | Couchbase 密码。 |
| bucket                 | String         | 是       | -          | 目标 Bucket 名称。 |
| scope                  | String         | 否       | `_default` | Bucket 中的目标 Scope 名称。 |
| collection             | String         | 是       | -          | 目标 Collection 名称。 |
| primary-key            | `List<String>`  | 否       | -          | 用于构建文档键的字段名列表（长度前缀编码：`<长度>:<值>` 分量以 `#` 分隔）。未设置时使用随机 UUID。 |
| upsert-enable          | Boolean        | 否       | `false`    | 是否启用 Upsert（插入或替换）模式。为 `false` 时，重复键将报错。 |
| buffer-flush.max-rows  | Integer        | 否       | `1000`     | 触发批量写入的最大缓冲行数。设为 `-1` 禁用。 |
| buffer-flush.interval  | Long           | 否       | `30000`    | 批量写入之间的最大间隔（毫秒）。设为 `-1` 禁用。 |
| retry.max              | Integer        | 否       | `3`        | 写入失败时的最大重试次数。 |
| retry.interval         | Long           | 否       | `1000`     | 线性退避基础间隔（毫秒）。第 n 次重试等待 `retry.interval × n` 毫秒。 |

## 安全性

### TLS / 加密传输

在生产环境中，请使用 `couchbases://` 协议（注意末尾的 **s**）来启用 TLS 加密传输。
如需配置 CA 证书或自定义信任库，可通过 Couchbase Java SDK 的 `ClusterEnvironment` 进行设置：

```hocon
sink {
  Couchbase {
    # 使用 couchbases://（末尾带 's'）以启用 TLS 加密传输
    connection.string = "couchbases://couchbase.example.com"
    username          = "seatunnel_writer"
    password          = "${env:COUCHBASE_PASSWORD}"
    bucket            = "my_bucket"
    collection        = "my_collection"
  }
}
```

有关 TLS 配置、证书固定、客户端证书和密码套件选项，请参阅
[Couchbase Java SDK — 安全连接](https://docs.couchbase.com/java-sdk/current/howtos/managing-connections.html#ssl)。

### 最小权限服务账户

生产环境中**不应**使用内置的 `Administrator` 账户。
请创建一个专用的 Couchbase 用户，并仅授予所需的最小权限：

- 针对目标 Bucket/Scope/Collection 的 `Data Writer` 角色（仅插入场景）。
- `Data Reader` + `Data Writer` 角色（Upsert 场景下可能需要读后写）。

### 凭据保护

请避免在 Job 配置文件中以明文存储密码。
SeaTunnel 支持加密配置值，详情请参阅
[SeaTunnel 凭据加密文档](../../introduction/configuration/config-encryption-decryption.md)，
了解如何在运行时动态替换密钥。

## 任务示例

### 简单示例 *(仅供开发使用)*

> ⚠️ 以下连接字符串和凭据**仅适用于本地开发环境**。
> 生产部署前请参阅上方的[安全性](#安全性)章节。

```hocon
sink {
  Couchbase {
    connection.string = "couchbase://127.0.0.1"
    username          = "Administrator"
    password          = "password"
    bucket            = "my_bucket"
    collection        = "my_collection"
  }
}
```

### 使用 Upsert 和复合文档键 *(仅供开发使用)*

> ⚠️ 以下连接字符串和凭据**仅适用于本地开发环境**。
> 生产部署前请参阅上方的[安全性](#安全性)章节。

```hocon
sink {
  Couchbase {
    connection.string      = "couchbase://127.0.0.1"
    username               = "Administrator"
    password               = "password"
    bucket                 = "my_bucket"
    scope                  = "_default"
    collection             = "my_collection"
    primary-key            = ["user_id", "order_id"]
    upsert-enable          = true
    buffer-flush.max-rows  = 500
    buffer-flush.interval  = 10000
    retry.max              = 5
    retry.interval         = 2000
  }
}
```

<ChangeLog />
