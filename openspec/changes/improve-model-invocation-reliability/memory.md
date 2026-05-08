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

# Model Invocation Reliability Memory

Last updated: 2026-05-08 13:59 Asia/Shanghai

## Change

- OpenSpec change：`improve-model-invocation-reliability`。
- 主题：在 `seatunnel-transforms-v2` 的 `nlpmodel` 下抽取通用远程模型调用层。
- 第一批接入方：`seatunnel-transforms-v2` 下的 `Embedding` transform。
- 战略位置：该变更承接 `dev-markdown`。`dev-markdown` 解决 chunk 稳定身份问题，本变更解决 chunk 到 vector 的可靠转换问题，然后再推进 production-quality 的 document-to-Milvus 示例。

## 需求有效性

这个需求是有效的，但前提是范围必须收窄为“通用远程模型调用可靠性层”，而不是泛化 AI 平台。

SeaTunnel 已经具备 Embedding 和 LLM 等 AI-facing transform。这些 transform 会调用外部模型服务，而外部服务天然会遇到网络超时、限流、临时 5xx、认证失败、响应格式变化、批量返回数量不一致等问题。这些失败模式不是某个 provider 或 Embedding 独有的问题。如果每个 provider 独立处理，会产生重复逻辑，也会让后续 LLM 可靠性改造变得不一致。

因此需求应该表达为：

> SeaTunnel 应在 `seatunnel-transforms-v2` 的 `nlpmodel` 下抽一个通用远程模型调用层；Embedding provider 负责构造请求和解析响应，通用层负责超时、重试、错误分类、响应校验、日志、指标和缓存边界。

## 当前上下文

- `EmbeddingTransform` 当前负责选择输入字段，并按行调用 `model.vectorization(...)`。
- `AbstractModel` 当前通过 `single_vectorized_input_number` 做请求级输入拆分，并校验返回 vector 数量与输入数量一致。
- `process_batch_size` 已经在 model transform config 中暴露，但 Embedding 当前还没有清晰的 row-level batching 执行契约。
- OpenAI、Doubao、Qianfan、Custom 等 provider 类目前各自构造 HTTP 请求、执行请求、解析响应，并抛出各自的 `IOException` 或 runtime error。
- 二进制多模态 cache 当前用于重组文件分片，不是 embedding 结果缓存。
- Embedding 文档当前主要说明如何使用和配置，还没有定义 retry 行为、partial failure 语义、幂等边界、缓存边界和可观测性要求。

## Apply 基线记录

Last checked: 2026-05-08 Asia/Shanghai

- `EmbeddingTransform` 的普通文本路径按行读取 `vectorization_fields`，构造一个当前 row 的 `Object[]`，然后调用 `model.vectorization(fieldValues)`。
- `MetadataUtil.isBinaryFormat(inputRow)` 路径先用 `binaryFileCache` 和 `partIndexMap` 重组 binary 文件分片；只有完整文件到达后才调用 `model.vectorization(...)`。该 cache 是文件重组状态，不是 embedding 结果缓存。
- `single_vectorized_input_number` 当前在 `AbstractModel.batchProcess(...)` 中控制每个 provider 请求包含的 input 数量；它不是 row-level micro-batching。
- `process_batch_size` 当前定义在 `ModelTransformConfig`，但 Embedding 实现没有用它实现 row-level batch。
- OpenAI、Doubao text、Qianfan、Zhipu 都在 provider 类里直接使用 Apache HTTP client，并各自设置 20 秒 connect/socket timeout。
- Custom provider 当前直接执行 HTTP 请求，用 JsonPath `custom_response_parse` 解析响应。
- Amazon Bedrock provider 通过 AWS BedrockRuntimeClient 调用，不是 Apache HTTP client 路径；本变更需要单独评估是否纳入第一批迁移。
- 当前 `AbstractModel.batchProcess(...)` 已经做响应数量校验，但错误是普通 `RuntimeException`，没有 provider/model/error category 上下文。
- Qianfan provider 当前遇到 `error_code == 110` 会刷新 access token，但刷新后仍抛出 `IOException`，后续 runtime 需要保留该 refresh 行为并由统一 retry 决策接管。
- Doubao multimodal 当前禁止 `singleVectorizedInputNumber > 1` 的单请求 batch，并逐个 multimodal field 调用；该限制需要保持兼容。
- 现有测试主要覆盖 request JSON、维度解析、vector 精度、多模态配置和多模态 body/parse 行为；尚缺通用 retry/error classification/sanitized logging/runtime tests。

## 具体场景

一个每日任务把 Markdown 文档写入 Milvus：

```text
Markdown files
  -> Markdown source
  -> chunk metadata: document_id / chunk_id / content_hash / chunk_index
  -> Embedding transform
  -> Milvus sink upsert
```

`dev-markdown` 让每个 chunk 稳定、可追踪、可 upsert。剩余风险在于 chunk text 到 vector 的转换。

典型失败：

- provider 对某个 batch 返回 HTTP 429 时，SeaTunnel 应把它分类为可重试错误，按 backoff 等待，重试同一个 batch，并且只在重试耗尽后失败。
- provider 返回 HTTP 401 或 403 时，SeaTunnel 应把它分类为不可重试认证错误，并带着足够上下文失败，方便用户修复凭证。
- 请求发送 16 个输入但只返回 15 个 vector 时，SeaTunnel 必须让该 batch 失败，不能静默补 null，也不能把可能错位的 vector 写入下游。
- Custom provider 的 `custom_response_parse` 不再匹配响应结构时，应分类为 response parse/configuration error，而不是盲目重试。
- Qianfan access token 过期时，可以保留 provider-specific 的 refresh 逻辑，但 retry 决策和日志仍应遵守共享契约。

目标不是让每次模型调用都成功，而是让失败清晰、重试谨慎，并且绝不写入错位向量。

## 推荐设计方向

不要把 retry/error/log 逻辑直接写死在 `EmbeddingTransform` 里，而是在 `seatunnel-transforms-v2` 的 `nlpmodel` 下抽一个通用远程模型调用层。

```text
EmbeddingTransform
  ↓
ModelInvocationRuntime       通用可靠性层
  ↓
ProviderAdapter              OpenAI / Doubao / Qianfan / Custom ...
  ↓
HTTP Client / Provider API
```

职责：

- `EmbeddingTransform`：负责字段选择、输出 schema、把输入 row 转为模型输入。
- `ModelInvocationRuntime`：通用可靠性层，负责 timeout、retry、backoff、错误分类、response count validation、安全日志、metric hook 和缓存边界。
- `ProviderAdapter`：OpenAI / Doubao / Qianfan / Custom 等 provider adapter，负责 provider-specific 的 request body、header、auth、请求发送或请求委托、response parse，以及把 provider 错误转换为通用错误。
- `HTTP Client / Provider API`：实际执行远程请求，可以继续复用现有 Apache HTTP client。
- 现有 `Model`/`AbstractModel` 实现应逐步迁移为 `ProviderAdapter -> ModelInvocationRuntime` 路径，让 provider 类不再各自重复实现 HTTP 可靠性逻辑。

该方案让第一版 PR 有实际收益，同时保留后续 LLM transform 复用的空间。

## Provider-Neutral 契约

### Batch 语义

- `single_vectorized_input_number` 继续表示一个远程请求中发送多少个 model input。
- 它不是 row-level micro-batching。
- 除非后续设计明确修改 transform 执行流，否则 `process_batch_size` 不应被视为 Embedding 已实现的 row-level batching。
- 对每个 request batch，输入数量和 vector 数量必须完全一致。

### 错误分类

建议公共错误类别：

- `RATE_LIMIT`：可重试，通常对应 HTTP 429。
- `TIMEOUT`：可重试，网络或 socket timeout。
- `TEMPORARY_REMOTE_ERROR`：可重试，通常对应 HTTP 5xx 或临时 provider 故障。
- `AUTHENTICATION_ERROR`：默认不可重试，除非 provider-specific token refresh 先成功。
- `CONFIGURATION_ERROR`：不可重试，例如 endpoint 缺失、model 不存在、custom parse expression 错误、unsupported multimodal request。
- `RESPONSE_PARSE_ERROR`：默认不可重试。
- `RESPONSE_COUNT_MISMATCH`：默认不可重试，必须让 batch 失败。
- `UNKNOWN_REMOTE_ERROR`：保守处理；只有 status 或 provider adapter 标记为可重试时才重试。

### Retry 策略

建议将通用配置放到 shared model transform config，使 Embedding 和后续 LLM 都能复用：

- `model_retry_max_attempts`
- `model_retry_backoff_ms`
- `model_retry_max_backoff_ms`
- `model_request_timeout_ms`

兼容性预期：

- 默认尽量保持当前行为。安全默认值是 `model_retry_max_attempts = 1`，即默认不自动重试。
- 文档可以推荐生产环境设置为 3 次尝试。
- backoff 必须避免紧密重试循环。

### 幂等边界

SeaTunnel 不能保证远端模型在数学上完全确定，但应该保证 SeaTunnel 自身的幂等边界：

- 相同 provider、model、config 和 input value 构造稳定请求。
- 输出字段顺序稳定。
- 输入与 vector 严格一一对应。
- 如果无法证明返回 vector 与输入对应，就不能静默部分成功。

### 缓存边界

第一版不做持久化 embedding 结果缓存。

原因：

- cache key 必须包含 provider、model、endpoint、input hash 和相关 config hash。
- cache value 可能代表敏感文本或敏感向量。
- 持久化或 checkpoint-aware cache 语义大于第一版可靠性 PR。
- 内存 cache 需要容量、淘汰和可观测性设计。

第一版可接受边界：

- 定义 `NoopCache` 或留下明确扩展点。
- 文档说明结果缓存是未来工作。
- 继续把现有 binary multimodal cache 视为文件重组 cache。

### 日志与指标

日志可以包含：

- provider
- model
- request batch size
- attempt number
- error category
- retryable flag
- elapsed time

日志不能包含：

- API key 或 secret key
- 完整原始文本 chunk
- 二进制图片或视频 payload
- 可能包含敏感回显的完整 provider response body

建议指标：

- model request count
- model request failure count
- retry attempt count
- retry exhaustion count
- response count mismatch count
- generated vector count
- request latency

## 第一版范围

推荐第一版实现范围：

- 在 `seatunnel-transforms-v2` 的 `nlpmodel` 包内新增 `ModelInvocationRuntime` 通用远程模型调用层。
- 新增 `ProviderAdapter`、公共 request/response/error 抽象。
- 新增 retry/backoff/timeout 配置。
- 首先让 Embedding provider 调用经过 `ProviderAdapter -> ModelInvocationRuntime` 路径。
- 保持默认行为和输出 schema 兼容。
- 覆盖 OpenAI、Doubao、Qianfan、Custom provider 路径，足以验证公共行为。
- 更新英文和中文 Embedding 文档，说明 batch、retry、failure、idempotence 和 logging 规则。
- 使用 mock provider response 增加聚焦单测。

推荐测试场景：

- 成功路径：每个 input 返回一个 vector。
- HTTP 429 重试后成功。
- HTTP 5xx 重试后成功。
- HTTP 401/403 不重试。
- response vector count mismatch 让 batch 失败。
- retry exhaustion 带 provider/model/error 上下文失败。
- custom response parse mismatch 分类为不可重试。

## 非目标

- 不在 SeaTunnel 中构建通用 AI 平台。
- 不增加 DataAudit 可信评分或校验逻辑。
- 不增加自治 Agent 编排或自动修复流程。
- 第一版不增加持久化 embedding cache。
- 不改变已有配置名或默认输出 schema。
- 不在本变更中重设计 Transform API 的 row-level micro-batching。
- 不实现 provider fallback 或 model routing。

## 待确认问题

- retry 默认值应严格兼容 `model_retry_max_attempts = 1`，还是采用更生产友好的 `3`。
- request timeout 是先复用现有 provider 硬编码值，还是立即暴露 `model_request_timeout_ms`。
- `RESPONSE_PARSE_ERROR` 是否存在 provider 间歇性 malformed response 时可重试的例外。
- 模型调用指标应该挂到 SeaTunnel 哪个 transform metric group。
- LLM transform 是否纳入第一版，还是等 Embedding 验证契约后另开 change。

## 后续 Artifact

- `proposal.md`：说明为什么需要通用模型调用可靠性，以及为什么 Embedding 先接入。
- `design.md`：定义 runtime、adapter 契约、错误模型、retry 策略、日志/指标、兼容性和迁移方式。
- `specs/model-invocation-reliability/spec.md`：定义用户可见的可靠性要求。
- `tasks.md`：拆分配置、runtime、provider 迁移、测试、文档和验证任务。

## Apply 实现记录

- 已在 `seatunnel-transforms-v2/src/main/java/org/apache/seatunnel/transform/nlpmodel` 下新增通用调用层：
  - `ModelInvocationRuntime`
  - `ProviderAdapter`
  - `ModelInvocationOptions`
  - `ModelInvocationException`
  - `ModelInvocationErrorType`
  - `ModelInvocationContext`
  - `ModelInvocationMetrics`
  - `ModelInvocationCache`
- `ModelTransformConfig` 新增共享配置：
  - `model_retry_max_attempts`，默认 `1`
  - `model_retry_backoff_ms`，默认 `1000`
  - `model_retry_max_backoff_ms`，默认 `10000`
  - `model_request_timeout_ms`，默认 `20000`
- `ModelInvocationRuntime` 当前职责：
  - 按 `maxAttempts` 执行 retry loop。
  - 仅对 provider adapter 标记的可重试错误重试。
  - 将 timeout、HTTP 429、HTTP 5xx、401/403、parse failure、response count mismatch 分类为 provider-neutral error type。
  - 对 batch 输入数和输出 vector 数做通用校验。
  - 记录脱敏日志上下文，不打印 provider response body 或原始 payload。
  - 暴露 no-op metrics hook 和 no-op cache 扩展点。
- 已接入的 Embedding provider：
  - OpenAI：HTTP timeout、HTTP status 分类、parse error 分类、response count 校验。
  - Doubao text：HTTP timeout、HTTP status 分类、parse error 分类、response count 校验。
  - Doubao multimodal：保持 `single_vectorized_input_number > 1` 禁止批量多模态的行为，并将每个多模态输入通过 runtime 调用。
  - Qianfan：保留 `error_code == 110` 时刷新 access token 的行为，并把刷新后的失败标记为可重试认证错误，让 runtime 决定是否再次尝试。
  - Custom：HTTP timeout、HTTP status 分类、JsonPath/转换失败分类为不可重试 parse error。
  - Zhipu：一并接入 runtime，保留 `dimension()` 使用配置值的行为。
- Amazon Bedrock 评估：
  - 该 provider 使用 AWS SDK `BedrockRuntimeClient`，不是当前 Apache HTTP client 路径。
  - AWS SDK timeout、异常类型、SDK retry 与 SeaTunnel runtime retry 的边界需要单独设计，避免重复重试或错误分类不准。
  - 本变更先记录延后原因，不在第一批强行迁移 Amazon。
- 已新增聚焦单测 `ModelInvocationRuntimeTest`，覆盖默认单次尝试、429/5xx 重试后成功、401 不重试、response count mismatch、parse failure 不重试、错误消息不暴露 provider response body。
- 已更新 `docs/en/transforms/embedding.md` 与 `docs/zh/transforms/embedding.md`，说明 retry、timeout、request-level batch、错误行为、幂等边界、缓存边界和安全日志。

## Apply 验证记录

- `openspec validate --all --json`：通过，3 个 active change 全部 valid。
- `.\mvnw.cmd -pl seatunnel-transforms-v2 spotless:apply`：通过。
- `.\mvnw.cmd spotless:apply`：通过。
- `.\mvnw.cmd -o -pl seatunnel-transforms-v2 -am "-DskipTests=true" "-Dmaven.test.skip=true" "-DfailIfNoTests=false" install`：通过，用于离线 reactor 生产代码编译和本地安装。
- `.\mvnw.cmd -o -pl seatunnel-transforms-v2 "-Dtest=ModelInvocationRuntimeTest" "-DfailIfNoTests=false" test`：通过，7 个测试通过。
- `.\mvnw.cmd -o -pl seatunnel-transforms-v2 "-Dtest=ModelInvocationRuntimeTest,EmbeddingRequestJsonTest,EmbeddingModelDimensionTest,DoubaoMultimodalModelTest" "-DfailIfNoTests=false" test`：通过，25 个测试通过。
- 非离线聚焦 Maven 测试曾因私有 Nexus SNAPSHOT metadata 访问缓慢而长时间停留；之后改用 `-o` 离线模式并配合 reactor install 完成验证。
- `.\mvnw.cmd -q -DskipTests verify`：已启动，但用户明确要求本轮跳过 verify；残留 Maven 进程已停止。本轮不声明完整 verify 已通过。
