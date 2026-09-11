import ChangeLog from '../changelog/connector-salesforce.md';

# Salesforce

> 基于 Salesforce REST sObject Collections 的 upsert Sink

## 描述

将一个输入表写入一个已存在的 Salesforce 对象，根据外部 ID 更新或插入记录。
复用现有 Salesforce 模块，不改变 Source 配置和读取行为；不会创建对象、字段、外部 ID 或应用。

## 支持的引擎

> SeaTunnel Zeta<br/>
> Flink<br/>
> Spark<br/>

## 功能

- [x] 批处理
- [x] 流处理
- [ ] exactly-once
- [ ] 完整 CDC
- [ ] 多表写入

流处理仅支持 `INSERT` 和 `UPDATE_AFTER`。本连接器不是 CDC Sink：
`UPDATE_BEFORE` 和 `DELETE` 会导致作业失败。连接变更日志 Source 前请阅读下方的投递语义。

## 配置

| 名称 | 类型 | 必填 | 默认值 |
| --- | --- | --- | --- |
| client_id | string | 是 | - |
| client_secret | string | 是 | - |
| username | string | 是 | - |
| password | string | 是 | - |
| security_token | string | 否 | 空字符串 |
| instance_url | string | 是 | - |
| api_version | string | 否 | v59.0 |
| object_name | string | 是 | - |
| external_id_field | string | 是 | - |
| batch_size | int | 否 | 200 |
| batch_max_bytes | int | 否 | 1048576 |
| request_timeout_ms | int | 否 | 60000 |
| max_retries | int | 否 | 3 |
| retry_interval_ms | long | 否 | 1000 |
| common-options | | 否 | - |

### 认证

使用与 Source 相同的 OAuth 用户名密码流程，组织必须允许该认证方式。
集成用户需要 API、对象创建/更新以及所有目标字段的写权限。security_token 追加在密码后。
生产环境的 instance_url 应为 HTTPS 登录或组织地址，例如 [Salesforce 登录地址](https://login.salesforce.com)，
不包含末尾斜杠、路径、内嵌凭证、查询参数或片段。HTTP 仅适用于显式配置的可信测试端点，
会明文传输凭证，不应在生产环境使用。数据请求使用认证响应中的实例地址。
Sink 不跟随重定向，也不接受认证地址从 HTTPS 降级到 HTTP。

client_secret 和 security_token 自动在解析后的配置日志中掩码；这不是配置加密。
不要将真实凭证提交到配置文件。

### 字段映射

object_name 为 Salesforce API 对象名，例如 Account。输入列名应与可写 API 字段匹配。
不支持嵌套对象、数组、关系对象或自动创建表结构。
external_id_field 必须存在于输入结构和 Salesforce 中，并在 Salesforce 中配置为外部 ID，
建议同时启用 Unique。每行键必须为非空、非空白的 STRING、整数或 DECIMAL。
不接受 Salesforce Id 作为外部 ID 或输入列。

不预先查询远端字段元数据，权限、校验或字段不兼容由 Salesforce 返回并导致作业失败。
其他字段的 null 会明确清空目标值。布尔和数值使用 JSON 原生类型，DECIMAL 不经过 double 转换；
远端字段的精度限制仍适用。拒绝非有限浮点数。
DATE 使用 ISO 日期，TIME 使用 UTC 毫秒格式，无时区 TIMESTAMP 按 UTC 解释，
TIMESTAMP_TZ 保留显式偏移量。拒绝亚毫秒时间值而不是静默截断；BYTES 使用 Base64。

### 批次和错误

batch_size 范围为 1-200。batch_max_bytes 限制包含封装在内的 UTF-8 JSON 请求大小，
范围为 128-8388608 字节，这是客户端限制而不是 Salesforce 限制的声明。
单条记录超限时在发送前失败。

固定使用 allOrNone=true，逐条检查响应。任意失败、缺失结果或非法响应均导致任务失败，
阻止检查点完成。错误只包含结果索引和状态码，不包含原始响应或字段值。
单条记录错误（包括锁冲突、校验错误）不会在客户端局部重试。

max_retries 为额外尝试次数（0-10），仅重试 I/O 错误或 HTTP 429、500、502、503、504。
在同一预算内允许一次 HTTP 401 重新认证。包括配额限制 HTTP 403 在内的其他错误立即失败。
认证请求自身失败时不自动重试，避免使用被拒绝的凭证反复登录。
遵守不超过 60 秒的 Retry-After；无效或更大的值直接失败，不提前重试。
retry_interval_ms 范围 0-60000。request_timeout_ms 必须为正，是连接、连接池和 socket 超时，
不是整个任务的截止时间。并行度和重试次数需要符合组织 API 配额。
刷新是同步执行的，检查点超时需要覆盖完整刷新过程，包括请求超时、重试等待和可能发生的重新认证。
单次请求超时并不等于总刷新预算，尤其是在持续限流或服务故障期间。

达到批次数量/字节限制、检查点准备或正常关闭时刷新。
同时注册引擎刷新回调。Zeta 可通过 env.sink.flush.interval 启用周期刷新（毫秒，默认 0 表示禁用）。
未实现此回调的引擎依赖数量限制、检查点和输入结束刷新。
流作业应启用周期性检查点以限制低流量下的延迟。

## 投递语义

仅接受 `INSERT` 和 `UPDATE_AFTER`；`DELETE` 和 `UPDATE_BEFORE` 会失败而不是被忽略。
本连接器不处理删除或外部 ID 变更的旧记录清理。静默丢弃更新前镜像可能掩盖不受支持的变更日志输入，
并在键变更后将旧记录留在 Salesforce 中。

配合可回放 Source 和检查点提供至少一次语义。不确定的 HTTP 结果和恢复可能再次 upsert，
重复执行触发器或 Flow，因此不保证 exactly-once，也不能回滚之前成功的请求。

单个 writer 内重复外部 ID 按输入顺序分批发送。同键有序更新应使用并行度 1，
或确保同键更新有序路由到同一 writer；不同 writer 和恢复尝试之间不保证全局顺序。
删除、仅插入模式、Bulk API 和多对象路由不属于本次范围。

## 示例

先在 Account 中创建启用 Unique 的外部 ID 字段 External_Id__c，
输入包含 External_Id__c=string 和 Name=string：

```hocon
sink {
  Salesforce {
    instance_url = "https://login.salesforce.com"
    client_id = "${SALESFORCE_CLIENT_ID}"
    client_secret = "${SALESFORCE_CLIENT_SECRET}"
    username = "${SALESFORCE_USERNAME}"
    password = "${SALESFORCE_PASSWORD}"
    security_token = "${SALESFORCE_SECURITY_TOKEN}"
    object_name = "Account"
    external_id_field = "External_Id__c"
    batch_size = 200
  }
}
```

请使用部署环境的变量替换机制提供示例占位符。
通用配置请参阅 [Sink Common Options](../common-options/sink-common-options.md)。

## 更新日志

<ChangeLog />
