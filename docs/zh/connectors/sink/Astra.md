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

import ChangeLog from '../changelog/connector-astra.md';

# Astra DB

`Astra` Sink 通过 [Astra DB Serverless Data API](https://docs.datastax.com/en/astra-db-serverless/api-reference/dataapiclient.html) 写入向量文档。
使用 `bin/install-plugin.sh` 安装 `connector-astra`。在 SeaTunnel Zeta 批作业或流作业中使用，
Sink 并行度设为 1。Astra 应用令牌需要读取集合元数据及执行所选写入模式的权限。
自动创建缺失的集合还需要目标 keyspace 的集合创建权限。

## 写入模式

| 模式 | 行为 |
| --- | --- |
| `append` | 为每个输入记录追加分块，不同输入中相同的主键值仍可产生独立文档。 |
| `overwrite` | 作业首次写入前清理指定命名空间和流。即使没有输入，刷新也可能清理该范围。 |
| `append_dedup` | 移除当前命名空间、流和复合主键对应的全部旧分块，再写入新分块。同一主键的多次更新按输入顺序执行。 |

缺失的集合自动按余弦相似度创建，复用集合时校验维数和度量方式。清理只匹配带有本连接器范围标记的文档。
连接器不会删除或重建集合。若现有集合采用选择性索引，请保留 `_st_scope` 的索引，去重模式还需要
`_st_record_id` 的索引。覆盖或去重的同一个范围由单个写入作业管理。

`append_dedup` 将 `DELETE` 和 `UPDATE_BEFORE` 用于删除对应记录的全部分块，
`INSERT` 和 `UPDATE_AFTER` 写入分块。其他模式忽略删除和更新前镜像。
覆盖使用固定的命名空间和流，两种追加模式也可以通过配置的字段按行路由。

## 参数

| 参数 | 类型 | 默认值 | 说明 |
| --- | --- | --- | --- |
| `endpoint` | string | — | 必填。Astra 数据库的 HTTPS 源站。 |
| `token` | string | — | 必填。通过受保护的配置值提供 Astra 应用令牌。 |
| `keyspace` | string | — | 必填。已存在的 Astra DB keyspace。 |
| `collection` | string | — | 必填。向量集合名称，不存在时可自动创建。 |
| `mode` | string | `append` | 可选 `append`、`overwrite`、`append_dedup`。 |
| `namespace` | string | 输入数据库 | 默认使用输入数据库名，无数据库名时为空。 |
| `stream` | string | 输入表名 | 默认使用输入表名。 |
| `namespace_field` | string | `""` | 可选，指定每行命名空间的字面字符串路径。两种追加模式可用。 |
| `stream_field` | string | `""` | 可选，指定每行流名称的字面字符串路径。两种追加模式可用。 |
| `primary_keys` | list | `[]` | 有序、非空的标量字段路径。`append_dedup` 必填。 |
| `text_fields` | list | `[]` | 用来构造嵌入文本的字段。空列表选择全部字段。 |
| `metadata_fields` | list | `[]` | 写入 `metadata` 的字段。空列表选择全部字段。 |
| `field_name_mappings` | map | `{}` | 对文本和元数据使用的选择路径或字段名重命名。 |
| `omit_raw_text` | boolean | `false` | 省略生成的分块文本字段。元数据仍按自身配置选择。 |
| `splitter` | string | `separator` | 可选 `separator`、`markdown` 或 `code`。 |
| `separators` | list | `["\n\n", "\n", " ", ""]` | 按顺序配置分隔符，可用末尾的空字符串表示按字符边界拆分。 |
| `keep_separator` | boolean | `false` | 在下一个分块边界保留选中的分隔符。 |
| `markdown_level` | int | `1` | 按标题优先拆分的最大 Markdown 标题级别，1–6。 |
| `code_language` | string | `""` | 使用 `code` 时必填，见下方语言列表。 |
| `chunk_size` | int | `400` | 每个分块最多的 GPT-2 token 数，1–8191。 |
| `chunk_overlap` | int | `0` | 分块最多重复的 GPT-2 token 数，小于 `chunk_size`。 |
| `tokenizer_data_file` | string | `""` | 可选。每个写入节点可使用本地的已校验 tokenizer rank 文件。 |
| `embedding_mode` | string | — | 必填。选择下节列出的嵌入模式。 |
| `embedding_secret_key` | string | `""` | 独立的远程嵌入服务凭据。兼容服务无鉴权时可为空。 |
| `embedding_url` | string | 取决于模式 | OpenAI、Azure OpenAI 或 Cohere 的 HTTPS 源站。兼容服务需提供完整的 HTTPS API 基址。 |
| `embedding_model` | string | `text-embedding-ada-002` | `openai_compatible` 的模型名称。 |
| `embedding_deployment` | string | `""` | Azure OpenAI 部署名，该模式必填。 |
| `embedding_field` | string | `""` | 已有数字向量的字面字段路径。`from_field` 必填。 |
| `embedding_dimensions` | int | 取决于模式 | `from_field` 和 `openai_compatible` 为 1–4096。其他模式使用固定维数。 |
| `max_row_bytes` | int | `262144` | 输入序列化值及提取文本的字节上限，256–4194304。 |
| `max_buffer_bytes` | int | `4194304` | 缓冲中展开文档的估计字节上限，1024–33554432。 |
| `max_body_bytes` | int | `4194304` | JSON 请求和响应的字节上限，1024–8388608。插入请求另有连接器的 4 MiB 限制。 |
| `max_chunks_per_row` | int | `100` | 单行生成的最大分块数，1–500。 |
| `max_delete_requests` | int | `500` | 一次条件清理最多的分批请求数，1–100000。 |
| `connect_timeout_ms` | int | `10000` | TCP 连接超时，1–300000。 |
| `read_timeout_ms` | int | `15000` | TLS 握手及响应读取的空闲超时，1–300000。 |
| `request_timeout_ms` | int | `30000` | 一次 HTTPS 操作的总时限，含重试，1–300000。 |
| `flush_timeout_ms` | int | `120000` | 一次缓冲刷新全部网络操作的时限，1–600000。 |
| `max_attempts` | int | `3` | 可重试请求的总尝试次数，包含初次请求，1–10。 |

## 嵌入与文本处理

| 模式 | 维数 | 配置及请求 |
| --- | --- | --- |
| `from_field` | 配置值 | 从输入的 `embedding_field` 读取已有向量，每个分块使用该向量。 |
| `fake` | 1536 | 生成确定性的本地合成向量，供测试使用。 |
| `openai` | 1536 | 使用 `text-embedding-ada-002`、Bearer 密钥和 HTTPS 域名 `api.openai.com`。 |
| `azure_openai` | 1536 | 配置 Azure 源站、1536 维模型的部署名及 API Key，调用 Azure v1 embeddings API。 |
| `cohere` | 1024 | 使用 `embed-english-v3.0`、检索文档模式、Bearer 密钥和 `api.cohere.com`。输出浮点向量，输入过长时失败。 |
| `openai_compatible` | 配置值 | 配置 HTTPS API 基址、模型名、维数及必要的凭据。连接器向基址追加 `/embeddings`。 |

Cohere 已停用旧 light v2 模型。新旧模型即使维数相同，向量语义也不同。更换模型时请使用新集合
或重新计算已有数据的嵌入向量。[Cohere 下线公告](https://docs.cohere.com/changelog/2026-04-04-embed-v2-aya-8b-retirement)

文本选择按每行一个命名值渲染。使用点号访问嵌套字段和从零开始的数组索引。
`users.*.name` 这样的通配路径按输入顺序匹配多个值。使用 `field_name_mappings` 重命名选择路径。
每个数据行至少需要一项文本选择及维数精确匹配、有限且非全零的向量。
元数据独立存放在 `metadata` 对象中，多值结果保留为数组。展开的元数据键中的点号是字面值，
用于过滤时按 Data API 规则转义。

分块大小及重叠使用真实 GPT-2 byte-pair token。分块时优先考虑字面分隔符、Markdown 标题或代码结构，
再应用 token 上限，保留完整的 Unicode 字符。支持的代码语言为 `cpp`, `go`, `java`, `js`, `php`, `proto`, `python`, `rst`, `ruby`, `rust`, `scala`, `swift`, `markdown`, `latex`, `html`, `sol`。
计数需要时才使用 JVM 信任库获取[公开 tokenizer rank 文件](https://openaipublic.blob.core.windows.net/encodings/r50k_base.tiktoken)，下载不发送凭据或行数据。
离线环境可通过 `tokenizer_data_file` 指定每个写入节点上的文件，SHA-256 必须为
`306cd27f03c1a714eca7108e03d66b7dc042abe8c258b44c199a7ed9838dd930`。

## 输出与恢复

文档包含 `_id`、`$vector`、选中的 `metadata`、可选的 `text`、连接器范围、分块序号及负载确认字段。
若要从 Astra 元数据中去除源文本，需要同时设置 `omit_raw_text` 并在 `metadata_fields` 排除相关文本。
远程嵌入模式会向嵌入服务发送选择的文本，可能产生费用。

每个插入请求最多写入 20 个文档。带过滤条件的大批清理每次最多删除 20 个文档，受配置的请求次数
和时限约束。检查点在所有缓冲写入确认后保存下一序号和覆盖标记。写入在检查点前可见，
删除和重写之间没有事务保护。传输异常或部分写入可能已经改变远端状态，连接器会报告失败。
回放写入遇到重复 ID 时，需要确认已存文档摘要一致才能接受确认。恢复已完成的检查点不会重复清理
覆盖范围。回放时需要保持输入顺序稳定。

## 示例

```hocon
env {
  job.mode = "BATCH"
  parallelism = 1
}
source {
  FakeSource {
    plugin_output = "articles"
    row.num = 1
    schema {
      fields {
        id = bigint
        body = string
        vector = "array<double>"
      }
    }
    rows = [{kind = "INSERT", fields = [42, "A document to index.", [0.5, 0.25, 1.0]]}]
  }
}
sink {
  Astra {
    plugin_input = "articles"
    endpoint = "https://example-us-east-2.apps.astra.datastax.com"
    token = ${ASTRA_DB_TOKEN}
    keyspace = "default_keyspace"
    collection = "sample_documents"
    mode = "append_dedup"
    namespace = "sample"
    stream = "articles"
    primary_keys = ["id"]
    text_fields = ["body"]
    metadata_fields = ["id"]
    embedding_mode = "from_field"
    embedding_field = "vector"
    embedding_dimensions = 3
  }
}
```

## 更新日志

<ChangeLog />
