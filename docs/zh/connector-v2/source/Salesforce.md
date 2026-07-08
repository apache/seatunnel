# Salesforce

> Salesforce 源连接器

## 支持的引擎

> Spark<br/>
> Flink<br/>
> SeaTunnel Zeta<br/>

## 关键特性

- [x] [批处理](../../introduction/concepts/connector-v2-features.md)
- [ ] [流处理](../../introduction/concepts/connector-v2-features.md)
- [ ] [精确一次](../../introduction/concepts/connector-v2-features.md)
- [x] [列投影](../../introduction/concepts/connector-v2-features.md)
- [ ] [并行度](../../introduction/concepts/connector-v2-features.md)
- [x] [支持多表读取](../../introduction/concepts/connector-v2-features.md)

## 描述

使用 Salesforce Bulk API 2.0 从 Salesforce 对象读取数据。支持单对象模式以及通过
`tables_configs` 进行多对象批量读取。

身份验证使用 OAuth 2.0 用户名-密码模式。

## 支持的数据源信息

| 数据源     | 支持的版本        |
|------------|------------------|
| Salesforce | REST API v50.0+  |

## 源选项

| 名称               | 类型    | 必填  | 默认值  | 描述                                                                                          |
|--------------------|---------|-------|---------|-----------------------------------------------------------------------------------------------|
| client_id          | String  | 是    | -       | Salesforce Connected App 的客户端 ID (consumer key)。                                          |
| client_secret      | String  | 是    | -       | Salesforce Connected App 的客户端密钥 (consumer secret)。                                       |
| username           | String  | 是    | -       | Salesforce 用户名。                                                                            |
| password           | String  | 是    | -       | Salesforce 密码。                                                                              |
| security_token     | String  | 否    | ""      | 附加到密码后用于身份验证的安全令牌。如果组织 IP 受信任则可留空。                                |
| instance_url       | String  | 是    | -       | Salesforce 实例 URL，例如 `https://yourorg.salesforce.com`。                                   |
| api_version        | String  | 否    | v59.0   | Salesforce REST API 版本。                                                                    |
| object_name        | String  | 否\*  | -       | 单对象模式下要读取的 Salesforce 对象，例如 `Account`。与 `tables_configs` 互斥。                |
| tables_configs     | List    | 否\*  | -       | 多对象配置列表。每一项必须提供 `table_path`。与 `object_name` 互斥。                            |
| filter             | String  | 否    | -       | 附加到自动构造的 `SELECT FIELDS(ALL) FROM <object>` 查询上的 SOQL WHERE 条件。                 |
| request_timeout_ms | Integer | 否    | 60000   | HTTP 请求超时时间（毫秒）。                                                                    |
| poll_interval_ms   | Long    | 否    | 5000    | Bulk API 作业状态轮询间隔（毫秒）。                                                            |
| job_completion_timeout_ms | Long | 否  | 3600000 | 等待 Bulk API 作业达到终止状态的最长时间（毫秒）。默认 60 分钟。                                |

\* `object_name` 与 `tables_configs` 必须且只能提供其中一个。

连接器始终发起 `SELECT FIELDS(ALL) FROM <object> [WHERE <filter>]` 查询，以保证
发出的行与基于 `/describe` 构造的 schema 在位置上保持一致。自定义投影式查询暂不
属于此版本的支持范围。

### tables_configs 项选项

| 名称        | 类型   | 必填 | 描述                                                                |
|-------------|--------|------|---------------------------------------------------------------------|
| table_path  | String | 是   | 格式为 `database.ObjectName`，例如 `salesforce.Account`。            |
| filter      | String | 否   | 针对该对象的 SOQL WHERE 条件。                                       |

## 示例

### 单对象

```hocon
source {
  Salesforce {
    client_id     = "your_client_id"
    client_secret = "your_client_secret"
    username      = "user@company.com"
    password      = "yourpassword"
    instance_url  = "https://yourorg.salesforce.com"

    object_name = "Account"
    filter      = "AnnualRevenue > 1000000"
  }
}
```

### 多对象

```hocon
source {
  Salesforce {
    client_id     = "your_client_id"
    client_secret = "your_client_secret"
    username      = "user@company.com"
    password      = "yourpassword"
    instance_url  = "https://yourorg.salesforce.com"

    tables_configs = [
      {
        table_path = "salesforce.Account"
        filter     = "AnnualRevenue > 1000000"
      },
      {
        table_path = "salesforce.Contact"
        filter     = "IsDeleted = false"
      },
      {
        table_path = "salesforce.Opportunity"
      }
    ]
  }
}
```

## 变更日志

### 下一个版本

- 新增 Salesforce 源连接器，基于 Bulk API 2.0，支持多对象读取
