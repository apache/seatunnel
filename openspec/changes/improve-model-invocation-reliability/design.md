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

## Context

SeaTunnel 已经包含 Embedding、LLM 等模型类 transform。随着 Markdown source 能输出稳定的 chunk 元数据，Embedding transform 会成为“文档 -> 向量 -> Milvus”链路中的核心环节。

当前 Embedding 实现通过 `Model.vectorization(...)` 调用各 provider。每个 provider 自己构造 HTTP 请求、执行请求、解析响应，并在失败时抛出自己的异常。`AbstractModel` 目前会按照 `single_vectorized_input_number` 拆分一次请求中的输入数量，并检查返回 vector 数量是否与输入数量一致，但还没有统一的重试、错误分类、超时、安全日志、指标和响应 mismatch 处理契约。

本变更在 `seatunnel-transforms-v2` 的 `nlpmodel` 下抽取一个通用远程模型调用层，并先接入 Embedding provider。它不是 AI 平台化改造，也不承担 DataAudit 或 Agent 的职责。

## Goals / Non-Goals

**Goals:**

- 在 `seatunnel-transforms-v2` 的 `nlpmodel` 下新增通用远程模型调用层 `ModelInvocationRuntime`。
- 定义 `ProviderAdapter` 职责边界：Embedding provider 负责构造请求和解析响应，通用调用层负责超时、重试、错误分类、响应校验、日志、指标和缓存边界。
- 对远程模型调用失败进行统一分类，并据此判断是否可重试。
- 增加统一的 retry、backoff、timeout 配置，默认保持兼容。
- 保证 Embedding 请求的输入与返回 vector 严格一一对应。
- 增加安全可观测性：能定位 provider、model、错误类型、attempt 和耗时，但不泄露密钥或原始 payload。
- 先接入 Embedding provider 路径。
- 保持现有 Embedding 输出 schema、配置名和默认行为兼容。

**Non-Goals:**

- 不构建通用 AI 平台或 Agent 编排层。
- 不实现 DataAudit 的可信校验或评分逻辑。
- 不新增持久化 embedding 结果缓存。
- 不重设计 Transform API 的 row-level micro-batching。
- 不实现 provider fallback 或模型路由。
- 第一版不要求 LLM transform 同步迁移。

## Decisions

1. 在 `seatunnel-transforms-v2` 的 `nlpmodel` 包下新增通用远程模型调用层 `ModelInvocationRuntime`。

   理由：timeout、retry、错误分类、响应校验、安全日志、指标和缓存边界是远程模型调用的共性问题。放在 `nlpmodel` 下可以让 Embedding 先使用，后续 LLM 也能复用，同时不会把模型调用细节扩散到 engine 或 connector。

   备选方案：只在 `EmbeddingTransform` 或每个 provider 内补 retry。这样短期更快，但会重复实现可靠性逻辑，后续 LLM/Custom provider 很难保持一致。

2. 使用 `ProviderAdapter` 明确 provider-specific 职责。

   理由：OpenAI、Doubao、Qianfan、Amazon、Zhipu、Custom 的 body、header、auth、响应结构都不同。通用调用层不应该理解每个 provider 的 payload。`ProviderAdapter` 只负责构造请求、执行或委托执行 provider API、解析响应，并把 provider-specific 错误转换为统一错误；`ModelInvocationRuntime` 负责 timeout、retry、错误分类、响应数量校验、日志、指标和缓存边界。

   备选方案：设计一个所有 provider 共用的 JSON 请求模型。这个方案对 Custom 比较自然，但不适合多模态 payload、OAuth、Amazon Bedrock 等差异化路径。

3. 引入统一的模型调用错误分类。

   建议类别：

   - `RATE_LIMIT`
   - `TIMEOUT`
   - `TEMPORARY_REMOTE_ERROR`
   - `AUTHENTICATION_ERROR`
   - `CONFIGURATION_ERROR`
   - `RESPONSE_PARSE_ERROR`
   - `RESPONSE_COUNT_MISMATCH`
   - `UNKNOWN_REMOTE_ERROR`

   理由：是否重试不能只看 Java exception 类型。HTTP 429、网络超时和 5xx 通常可以重试；401/403、配置错误和响应解析错误通常不能重试。

   备选方案：对所有 `IOException` 都重试。这个方案会反复重试错误密钥、错误模型名、错误 JsonPath 等确定性失败，不适合生产。

4. 在共享 model transform 配置中新增 retry 和 timeout 选项。

   建议配置：

   - `model_retry_max_attempts`，默认 `1`
   - `model_retry_backoff_ms`，默认 `1000`
   - `model_retry_max_backoff_ms`，默认 `10000`
   - `model_request_timeout_ms`，默认 `20000`

   理由：`model_retry_max_attempts = 1` 等价于默认不自动重试，最符合向后兼容要求。`model_request_timeout_ms = 20000` 与当前多个 provider 中硬编码的 20 秒超时保持一致。生产文档可以建议用户设置为 `3` 次重试。

   备选方案：默认重试 3 次。这个方案对生产更友好，但可能改变现有任务的失败耗时和 API 成本。

5. 明确 `single_vectorized_input_number` 是 request-level batch。

   理由：当前 Embedding 是按行进入 transform，然后对该行配置的多个 vectorization input 做请求级拆分。它不是 row-level micro-batch。行级批处理会影响 Transform 执行流和 backpressure，应该作为独立变更。

   备选方案：本次直接让 `process_batch_size` 成为 Embedding 行级批处理配置。这个方案更大，容易把可靠性改造和执行模型改造混在一起。

6. 第一版不做持久化结果缓存。

   理由：embedding 缓存涉及 key 设计、容量控制、checkpoint 语义、模型版本、敏感文本和敏感向量处理。它有价值，但不应该和第一版可靠性契约绑定。

   备选方案：立即加入 in-memory cache。它能减少调用成本，但会引入内存、淘汰策略和可观测性问题。

7. 统一安全日志和指标语义。

   理由：用户需要知道失败发生在哪个 provider/model/batch/attempt，但日志不能打印 `api_key`、`secret_key`、完整文本 chunk、二进制 payload，或可能包含敏感回显的完整响应体。

   备选方案：为方便排查直接打印完整 provider response。这个方案有泄露敏感数据的风险，不适合作为默认行为。

## 当前拟实现方案

当前方案固定为下面这套分层架构：

```text
EmbeddingTransform
  ↓
ModelInvocationRuntime       通用可靠性层
  ↓
ProviderAdapter              OpenAI / Doubao / Qianfan / Custom ...
  ↓
HTTP Client / Provider API
```

职责拆分如下：

- `EmbeddingTransform`：负责读取 `vectorization_fields`、构造待向量化输入、维护输出 schema、把返回 vector 写入输出字段。
- `ModelInvocationRuntime`：通用可靠性层，负责 timeout、retry、backoff、错误分类、响应数量校验、安全日志、指标 hook 和缓存边界。
- `ProviderAdapter`：OpenAI / Doubao / Qianfan / Custom 等 provider adapter，负责 provider-specific 的 request body、header、认证、请求发送或请求委托、响应解析，以及把 provider 错误转换为通用错误。
- `HTTP Client / Provider API`：实际执行远程请求，可以继续复用现有 Apache HTTP client 路径。

这不是已经存在的完整实现，而是本 OpenSpec change 的目标落地架构。现有代码里 provider 仍然各自执行 HTTP 调用；后续 apply 阶段会把这些 provider 调用包到 `ProviderAdapter` 后，再统一经过 `ModelInvocationRuntime`。

## Risks / Trade-offs

- [风险] runtime 抽象过度，掩盖 provider 差异。-> 缓解：provider-specific 的 body、header、auth、request sending 和 parse 仍留在 `ProviderAdapter`。
- [风险] provider 继续绕过通用层直接做 retry/log。-> 缓解：tasks 明确要求 OpenAI、Doubao、Qianfan、Custom 通过 `ProviderAdapter -> ModelInvocationRuntime` 路径接入。
- [风险] 默认重试改变 API 成本或失败耗时。-> 缓解：默认 `model_retry_max_attempts = 1`，生产推荐写入文档而不是默认打开。
- [风险] response count mismatch 会让过去“看起来成功”的任务失败。-> 缓解：这是必要的安全行为，因为继续执行可能写入错位向量。
- [风险] timeout 暴露后，一些过去长时间挂住的请求会更快失败。-> 缓解：使用与当前代码一致的默认 20 秒，并允许用户配置。
- [风险] transform metrics 接入点可能不统一。-> 缓解：先实现轻量 hook；如果上下文缺失，不阻塞模型调用主流程。

## Migration Plan

1. 增加共享配置项，默认保持兼容。
2. 新增 `ModelInvocationRuntime`、`ProviderAdapter`、错误类别和响应数量校验工具，不立即改变 provider 公共行为。
3. 将 Embedding provider 调用迁移为 `ProviderAdapter -> ModelInvocationRuntime` 路径，优先处理 OpenAI、Doubao、Qianfan、Custom。
4. 保留现有 provider identifier、配置名和输出 schema。
5. 同步更新英文和中文 Embedding 文档。
6. 如出现兼容问题，用户可保持或显式设置 `model_retry_max_attempts = 1`，维持无自动重试行为。

## Open Questions

- Amazon 和 Zhipu 是否纳入第一批迁移，还是在 OpenAI、Doubao、Qianfan、Custom 稳定后补充。
- 最终 metric 名称应挂在哪个 transform metric group 下。
- LLM transform 是否在 Embedding 验证后立即复用 runtime，还是另开后续 change。
