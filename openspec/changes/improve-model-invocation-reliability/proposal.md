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

## Why

SeaTunnel 的 Embedding transform 已经能把文本或多模态数据转换成向量，但远程模型调用在生产环境里会遇到限流、超时、临时 5xx、认证失败、响应格式变化、批量返回数量不一致等问题。`dev-markdown` 已经让 Markdown chunk 具备稳定身份，下一步需要保证 chunk 到 vector 的转换过程可重试、可定位，并且不会把错位向量写入下游。

## What Changes

- 在 `seatunnel-transforms-v2` 的 `nlpmodel` 下抽取一个通用远程模型调用层 `ModelInvocationRuntime`。
- 新增 `ProviderAdapter` 职责边界：Embedding provider 负责构造请求和解析响应，通用调用层负责超时、重试、错误分类、响应数量校验、安全日志、指标和缓存边界。
- 以 Embedding provider 作为第一批接入方，同时保持现有 Embedding 输出 schema 和默认行为兼容。
- 明确 `single_vectorized_input_number` 是 request-level batch，不是 row-level micro-batch。
- 明确缓存边界：本变更不加入持久化 embedding 结果缓存；现有二进制多模态缓存仍只用于文件分片重组。
- 更新英文和中文 Embedding 文档，说明失败语义、重试、幂等边界、可观测性和批处理行为。
- 本变更不引入 DataAudit 校验、Agent 编排、provider 自动降级、持久化缓存或 Transform API 的行级批处理改造。

## Capabilities

### New Capabilities

- `model-invocation-reliability`：定义 `nlpmodel` 下通用远程模型调用层的职责边界和可靠性行为，包括 `ModelInvocationRuntime`、`ProviderAdapter`、重试策略、错误分类、响应校验、安全可观测性，以及 Embedding provider 的首批接入。

### Modified Capabilities

- 无。

## Impact

- 影响代码：`seatunnel-transforms-v2` 中的通用 model transform 配置、`nlpmodel` 远程模型调用层，以及 OpenAI、Doubao、Qianfan、Custom 等 Embedding provider adapter 路径。
- 影响文档：`docs/en/transforms/embedding.md` 与 `docs/zh/transforms/embedding.md`。
- 公共兼容性：不删除配置项、不改变默认输出 schema、不让现有任务在默认配置下出现静默行为变化。
- 依赖：预计不新增运行时依赖；实现优先使用现有 SeaTunnel 工具和已有 shaded 依赖。
- 后续扩展：Embedding 验证该契约后，LLM transform 可以在后续变更中复用同一套调用层。
