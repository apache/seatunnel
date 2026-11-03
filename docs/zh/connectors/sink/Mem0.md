import ChangeLog from '../changelog/connector-mem0.md';

# Mem0

Mem0 Sink 将 SeaTunnel 行写入托管版 Mem0 Platform V3 异步添加接口。第一阶段
只支持 `ADD`。只有接口返回非空 `event_id` 时才确认该行；这表示“已接受的至少一次
投递”，不代表 Mem0 异步处理已经完成。


## Sink 选项

| 名称             | 类型   | 是否必填 | 默认值                 | 描述                                                                                     |
|------------------|--------|----------|------------------------|------------------------------------------------------------------------------------------|
| api_key          | String | 是       | -                      | Mem0 Platform API Key，以 `Authorization: Token <api_key>` 发送。                          |
| api_base_url     | String | 否       | `https://api.mem0.ai`  | Mem0 Platform V3 API 基础地址。                                                          |
| messages_field   | String | 是       | -                      | 保存 messages 载荷的行字段，必须解析为数组。                                             |
| user_id_field    | String | 否       | -                      | 映射到 Mem0 `user_id` 的行字段。                                                         |
| agent_id_field   | String | 否       | -                      | 映射到 Mem0 `agent_id` 的行字段。                                                        |
| app_id_field     | String | 否       | -                      | 映射到 Mem0 `app_id` 的行字段。                                                          |
| run_id_field     | String | 否       | -                      | 映射到 Mem0 `run_id` 的行字段。                                                          |
| metadata_field   | String | 否       | -                      | 映射到 Mem0 `metadata` 的行字段，必须解析为 JSON 对象。                                  |
| common-options   |        | 否       | -                      | Sink 插件通用参数，请参考 [Sink 通用选项](../common-options/sink-common-options.md)。     |

`user_id_field`、`agent_id_field`、`app_id_field`、`run_id_field` 这四个范围字段中，至少有一个必须在每一行上配置且非空。

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

<ChangeLog />
