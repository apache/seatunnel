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

## ADDED Requirements

### Requirement: Nlpmodel invocation runtime architecture
SeaTunnel MUST 在 `seatunnel-transforms-v2` 的 `nlpmodel` 下提供通用远程模型调用层，并 MUST 通过 `EmbeddingTransform -> ModelInvocationRuntime -> ProviderAdapter -> HTTP Client / Provider API` 的架构接入 Embedding provider。

#### Scenario: Embedding provider uses common runtime
- **WHEN** Embedding provider 发起远程模型调用
- **THEN** 调用 MUST 经过 `ProviderAdapter` 和 `ModelInvocationRuntime`，由 `ModelInvocationRuntime` 处理超时、重试、错误分类、响应校验、日志、指标和缓存边界。

#### Scenario: Provider adapter owns provider-specific behavior
- **WHEN** provider 需要构造请求或解析响应
- **THEN** `ProviderAdapter` MUST 负责 provider-specific 的 request body、header、认证、响应解析和 provider 错误转换。

### Requirement: Model invocation retry policy
SeaTunnel MUST 为远程模型调用提供 provider-neutral 的重试配置，并且 MUST 在默认配置下保持无自动重试的兼容行为。

#### Scenario: Default retry behavior remains compatible
- **WHEN** 用户没有配置模型调用重试选项
- **THEN** SeaTunnel MUST 对每个远程模型请求只执行一次尝试。

#### Scenario: Retryable provider failure is retried
- **WHEN** 远程模型请求因限流、超时或临时远端服务错误等可重试错误失败
- **THEN** SeaTunnel MUST 对同一个请求进行重试，直到请求成功或达到配置的最大尝试次数。

#### Scenario: Non-retryable provider failure is not retried
- **WHEN** 远程模型请求因认证失败、模型配置错误或响应解析错误等不可重试错误失败
- **THEN** SeaTunnel MUST 不再重试该请求，并直接让请求失败。

### Requirement: Model invocation error classification
SeaTunnel MUST 将远程模型调用失败分类为 provider-neutral 的错误类别，用于决定重试行为和生成用户可理解的诊断信息。

#### Scenario: HTTP status is mapped to error category
- **WHEN** provider 返回 HTTP 429、5xx、401 或 403
- **THEN** SeaTunnel MUST 根据状态码映射为 rate limit、temporary remote error 或 authentication error 等错误类别。

#### Scenario: Provider parser failure is classified
- **WHEN** provider 响应无法按照配置或 provider 响应契约解析
- **THEN** SeaTunnel MUST 将该失败分类为 response parse error 或 configuration error。

### Requirement: Embedding response count validation
SeaTunnel MUST 校验每个 Embedding 请求都为请求中的每个模型输入返回且仅返回一个 vector。

#### Scenario: Vector count matches input count
- **WHEN** Embedding 请求发送 N 个输入，并且 provider 返回 N 个 vector
- **THEN** SeaTunnel MUST 保持 vector 顺序，并将 vector 写入对应输出字段。

#### Scenario: Vector count does not match input count
- **WHEN** Embedding 请求发送 N 个输入，但 provider 返回的 vector 数量不是 N
- **THEN** SeaTunnel MUST 让该请求失败，并且 MUST NOT 输出可能错位的 vector。

### Requirement: Request-level batch semantics
SeaTunnel MUST 文档化并保持 `single_vectorized_input_number` 的语义：它表示一个远程请求中包含的模型输入数量。

#### Scenario: User configures request batch size
- **WHEN** 用户设置 `single_vectorized_input_number`
- **THEN** SeaTunnel MUST 将模型输入拆分为不超过该配置值的 request batch。

#### Scenario: Row-level micro-batching is not implied
- **WHEN** 用户阅读 Embedding 文档中的 `single_vectorized_input_number`
- **THEN** 文档 MUST 区分 request-level 模型输入批处理和 row-level transform micro-batching。

### Requirement: Safe model invocation observability
SeaTunnel MUST 暴露足够的模型调用上下文用于排障，同时保护密钥和源数据 payload。

#### Scenario: Model invocation is logged
- **WHEN** 远程模型调用失败或发生重试
- **THEN** SeaTunnel MUST 记录经过脱敏的上下文，例如 provider、model、batch size、attempt number、error category、retryable flag 和 elapsed time。

#### Scenario: Sensitive data is protected
- **WHEN** SeaTunnel 记录模型调用诊断日志
- **THEN** SeaTunnel MUST NOT 记录 API key、secret key、完整源文本 chunk、二进制 payload，或不安全的完整 provider response body。

#### Scenario: Model invocation metrics are recorded
- **WHEN** 远程模型调用被执行
- **THEN** SeaTunnel MUST 在 transform metrics 可用时记录请求数、失败数、重试数、重试耗尽数、response count mismatch 数、生成 vector 数或调用耗时等指标。

### Requirement: Embedding compatibility
SeaTunnel MUST 将模型调用可靠性接入 Embedding，同时不删除现有配置项、不改变默认输出 schema、不改变 provider identifier。

#### Scenario: Existing Embedding job omits new options
- **WHEN** 现有 Embedding 任务没有配置新的模型调用可靠性选项
- **THEN** SeaTunnel MUST 保持相同输出 schema，并且每个 request batch 只执行一次远程请求尝试。

#### Scenario: Existing vectorization field mapping is used
- **WHEN** 现有 Embedding 任务配置了 `vectorization_fields`
- **THEN** SeaTunnel MUST 保持输出字段顺序和 vector 赋值语义。

### Requirement: Embedding cache boundary
SeaTunnel MUST NOT 在该 capability 中新增持久化 Embedding 结果缓存。

#### Scenario: Embedding result cache is requested implicitly
- **WHEN** 用户启用模型调用可靠性选项
- **THEN** SeaTunnel MUST NOT 持久化模型输入或 vector 结果用于复用，除非后续引入独立缓存 capability。

#### Scenario: Binary multimodal cache is used
- **WHEN** Embedding 处理 binary multimodal row
- **THEN** SeaTunnel MUST 将现有 binary cache 视为文件重组状态，而不是模型结果缓存。
