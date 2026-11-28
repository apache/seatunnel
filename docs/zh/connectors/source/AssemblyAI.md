<!--
Licensed to the Apache Software Foundation (ASF) under one or more
contributor license agreements.  See the NOTICE file distributed with
this work for additional information regarding copyright ownership.
The ASF licenses this file to You under the Apache License, Version 2.0
(the "License"); you may not use this file except in compliance with
the License.  You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
-->

import ChangeLog from '../changelog/connector-http-assemblyai.md';

# AssemblyAI

安装 `connector-http-assemblyai`，在批处理任务中使用 `AssemblyAI` 读取账户中已经创建的转录。使用转录所在 US 或 EU 区域的 API 密钥。[转录列表](https://www.assemblyai.com/docs/pre-recorded-audio/api-reference/transcripts/list) · [云服务端点](https://www.assemblyai.com/docs/pre-recorded-audio/select-the-region)

## 资源

| 资源 | 数据 | 行 ID |
| --- | --- | --- |
| `transcripts` | 列表接口返回的原始转录摘要，包含状态与创建时间 | 转录 ID |
| `transcript_sentences` | 已完成转录的句子 | 转录 ID 与从 1 开始的句子位置 |
| `paragraphs` | 已完成转录的段落 | 转录 ID 与从 1 开始的段落位置 |
| `redacted_audio` | 启用了音频脱敏的已完成转录所返回的 JSON 状态及签名下载地址 | 转录 ID |

连接器按照列表的历史页游标向早期记录读取，并处理每条转录的所选资源。转录摘要可以包含排队中、处理中、失败与已删除记录。子资源只读取已完成且没有删除标记的转录。脱敏音频的可用期由 AssemblyAI 限制为 24 小时，连接器输出其地址元数据。请选择适合存储短期下载地址和转录内容的目标端。[句子](https://www.assemblyai.com/docs/pre-recorded-audio/api-reference/transcripts/get-sentences) · [段落](https://www.assemblyai.com/docs/pre-recorded-audio/api-reference/transcripts/get-paragraphs) · [脱敏音频](https://www.assemblyai.com/docs/pre-recorded-audio/api-reference/transcripts/get-redacted-audio)

## 参数

| 参数 | 类型 | 默认值 | 用途 |
| --- | --- | --- | --- |
| `token` | 字符串 | 必填 | AssemblyAI API 密钥 |
| `region` | 字符串 | `US` | 选择 US 或 EU API 区域 |
| `api_origin` | 字符串 | 区域端点 | 可选的固定 HTTPS 源站。US 为 `https://api.assemblyai.com`，EU 为 `https://api.eu.assemblyai.com`。 |
| `tls_trust_certificate` | 字符串 | JVM 信任配置 | 可选的连接器专用 X.509 信任证书，支持 PEM 或 base64 DER。始终校验 HTTPS 主机名。 |
| `resources` | 字符串列表 | 全部四项 | 每条转录按配置顺序读取的资源 |
| `start_date` | 字符串 | 批次开始前 30 天 | 包含起点的转录创建时间，采用带偏移量的 ISO 8601 |
| `end_date` | 字符串 | 批次开始时间 | 包含终点的转录创建时间，采用带偏移量的 ISO 8601，上限为批次开始时间 |
| `page_size` | 整数 | 100 | 转录列表每页记录数，范围 1 至 200 |
| `max_pages` | 整数 | 1000 | 单批列表读取的最大页数 |
| `request_timeout_ms` | 整数 | 30000 | 单次 HTTPS 尝试的总超时 |
| `operation_timeout_ms` | 整数 | 120000 | 一次读取包含重试的总超时 |
| `min_request_interval_ms` | 整数 | 100 | HTTPS 尝试之间的最短时间 |
| `max_retries` | 整数 | 3 | 临时故障的额外尝试次数 |
| `max_response_bytes` | 整数 | 4194304 | 分别限制传输和解压后的 JSON 字节数 |

AssemblyAI 当前列表接口的文档规定了 30 天的可见范围。创建时间窗口对供应商可用的摘要和子资源统一生效。供应商返回的创建时间未提供偏移量时，按 UTC 解读。

## 输出与恢复

输出四列字符串 `resource`、`id`、`transcript_id` 和 `record`。`record` 保留相应摘要、句子、段落或脱敏音频元数据的 JSON 对象。连接器仅向所选源站发起读取请求，媒体地址作为字段输出。引擎需要信任源站的 TLS 证书。

一个活跃读取器保存固定的时间窗口、历史页游标、父转录 ID 和已接收记录位置。页内或子资源恢复前先核对保存内容的指纹。供应商数据可能在读取期间变化，若保存页已经改变，任务将以恢复错误终止。新建批次从供应商当前列表读取。目标端交付保证取决于 Sink。

```hocon
env {
  job.mode = "BATCH"
  parallelism = 1
}

source {
  AssemblyAI {
    token = ${ASSEMBLYAI_TOKEN}
    region = "US"
    resources = ["transcripts", "transcript_sentences", "paragraphs"]
  }
}

sink {
  Console {}
}
```

<ChangeLog />
