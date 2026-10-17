import ChangeLog from '../changelog/connector-mem0.md';

# Mem0

Mem0 Sink 将 SeaTunnel 行写入托管版 Mem0 Platform V3 异步添加接口。第一阶段
只支持 `ADD`。只有接口返回非空 `event_id` 时才确认该行；这表示“已接受的至少一次
投递”，不代表 Mem0 异步处理已经完成。

## 配置

```hocon
sink {
  Mem0 {
    api_key = "${MEM0_API_KEY}"
    messages_field = "messages"
    user_id_field = "user_id"
    api_base_url = "https://api.mem0.ai"
  }
}
```

`messages_field` 必须解析为数组。每行至少要有
`user_id_field`、`agent_id_field`、`app_id_field`、`run_id_field` 中的一个非空范围字段。
可选的 `metadata_field` 必须解析为 JSON 对象。

连接器使用 `POST /v3/memories/add/`，并发送 `Authorization: Token`、
`Content-Type: application/json` 和 `Accept: application/json`。删除、自托管 OSS
接口、事件轮询以及通用 Mem0 兼容协议不属于第一阶段范围。

## Sink 选项

| 名称 | 类型 | 是否必填 | 默认值 | 描述 |
| --- | --- | --- | --- | --- |
| api_key | String | 是 | - | Mem0 API Key，作为 `Authorization: Token` 凭证发送 |
| messages_field | String | 是 | - | 存放 messages 载荷的输入行字段，必须解析为数组 |
| api_base_url | String | 否 | https://api.mem0.ai | Mem0 Platform V3 API 基础地址 |
| user_id_field | String | 否 | - | 映射到 Mem0 `user_id` 的输入行字段 |
| agent_id_field | String | 否 | - | 映射到 Mem0 `agent_id` 的输入行字段 |
| app_id_field | String | 否 | - | 映射到 Mem0 `app_id` 的输入行字段 |
| run_id_field | String | 否 | - | 映射到 Mem0 `run_id` 的输入行字段 |
| metadata_field | String | 否 | - | 可选的、映射到 Mem0 `metadata` 的输入行字段，必须解析为 JSON 对象 |
| retry | Int | 否 | - | Http 请求抛出 `IOException` 时的最大重试次数 |
| retry_backoff_multiplier_ms | Int | 否 | 100 | Http 请求失败时重试退避时间（毫秒）的乘数 |
| retry_backoff_max_ms | Int | 否 | 10000 | Http 请求失败时重试退避的最大时间（毫秒） |
| common-options | | 否 | - | Sink 插件通用参数，详情请参考 [Sink Common Options](../common-options/sink-common-options.md) |

<ChangeLog />
