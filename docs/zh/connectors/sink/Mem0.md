<!--
  Licensed to the Apache Software Foundation (ASF) under one or more
  contributor license agreements. See the NOTICE file distributed with
  this work for additional information regarding copyright ownership.
  The ASF licenses this file to You under the Apache License, Version 2.0.
-->

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
