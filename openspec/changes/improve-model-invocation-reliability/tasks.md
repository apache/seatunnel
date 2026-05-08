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

## 1. 基线与兼容性

- [x] 1.1 修改代码前，重新检查当前 `dev` 分支中的 Embedding 实现、provider 类、现有测试和文档。
- [x] 1.2 确认 `single_vectorized_input_number`、`process_batch_size`、provider timeout 和 response count 校验的当前行为。
- [x] 1.3 记录必须保持兼容的 provider-specific 行为，包括 Qianfan access token refresh 和 Doubao multimodal 行为。

## 2. 共享配置与契约

- [x] 2.1 在共享 model transform config 中新增 `model_retry_max_attempts`、`model_retry_backoff_ms`、`model_retry_max_backoff_ms` 和 `model_request_timeout_ms`。
- [x] 2.2 将 `model_retry_max_attempts` 默认值设置为 1，保持默认无自动重试行为。
- [x] 2.3 新增 provider-neutral 错误类别：rate limit、timeout、temporary remote error、authentication error、configuration error、response parse error、response count mismatch、unknown remote error。
- [x] 2.4 新增模型调用异常或结果包装，携带 error category、retryable flag、脱敏 message、provider、model 和可选 HTTP status。

## 3. 调用运行时

- [x] 3.1 在 `seatunnel-transforms-v2` 的 `nlpmodel` 包下新增 `ModelInvocationRuntime` 通用远程模型调用层。
- [x] 3.2 新增 `ProviderAdapter` 接口或等价抽象，明确 provider 只负责构造请求、解析响应、认证和 provider-specific 错误转换。
- [x] 3.3 基于共享配置在 `ModelInvocationRuntime` 中实现 retry loop、backoff、retry exhaustion 处理和 timeout 传递。
- [x] 3.4 在 `ModelInvocationRuntime` 中为需要一一对应的 request batch 增加通用 response count 校验。
- [x] 3.5 在 `ModelInvocationRuntime` 中增加安全日志，记录 provider、model、batch size、attempt number、error category、retryable flag 和 elapsed time，但不记录密钥或完整 payload。
- [x] 3.6 在 `ModelInvocationRuntime` 中定义缓存边界，第一版使用 no-op 或扩展点，不做持久化结果缓存。
- [x] 3.7 在 transform metrics 可用时增加模型调用指标；如果指标上下文缺失，metric hook 不应阻塞 runtime。

## 4. Embedding Provider 接入

- [x] 4.1 为 OpenAI Embedding 增加 `ProviderAdapter`，并通过 `ModelInvocationRuntime` 执行调用。
- [x] 4.2 为 Doubao text Embedding 增加 `ProviderAdapter`，并通过 `ModelInvocationRuntime` 执行调用。
- [x] 4.3 保持 Doubao multimodal 请求行为，同时通过 adapter 接入通用错误分类和安全日志。
- [x] 4.4 为 Qianfan Embedding 增加 `ProviderAdapter`，通过 runtime 执行调用，并保留 access token refresh 行为。
- [x] 4.5 为 Custom Embedding 增加 `ProviderAdapter`，通过 runtime 执行调用，并把 JsonPath 或响应转换失败分类为不可重试的 parse/configuration error。
- [x] 4.6 评估 Amazon 和 Zhipu provider 路径，并在本变更中迁移，或记录延后原因。

## 5. 测试

- [x] 5.1 增加单测，证明默认 retry 行为只执行一次远程请求尝试。
- [x] 5.2 增加单测，覆盖 HTTP 429 和 5xx 可重试失败在重试后成功。
- [x] 5.3 增加单测，证明 HTTP 401 或 403 不会被重试。
- [x] 5.4 增加单测，证明 response vector count mismatch 会让请求失败，并且不会输出错位 vector。
- [x] 5.5 增加单测，证明 custom response parse 失败会被分类为不可重试。
- [x] 5.6 增加单测或断言，证明脱敏日志/错误不会暴露 API key、secret key、完整源文本或二进制 payload。
- [x] 5.7 如构造器签名或 provider test helper 变化，更新现有 Embedding 测试。

## 6. 文档

- [x] 6.1 更新 `docs/en/transforms/embedding.md`，说明 retry、timeout、request-level batch、错误行为、幂等边界、缓存边界和安全日志。
- [x] 6.2 更新 `docs/zh/transforms/embedding.md`，保持相同配置名和行为说明。
- [x] 6.3 明确 `single_vectorized_input_number` 控制每个远程请求中的模型输入数量，不代表 row-level transform micro-batching。
- [x] 6.4 明确本变更不包含持久化 embedding 结果缓存。

## 7. 验证

- [x] 7.1 运行 `openspec validate --all --json` 并修复所有 OpenSpec 问题。
- [x] 7.2 运行聚焦的 Embedding transform 单元测试。
- [x] 7.3 运行 `./mvnw spotless:apply`。
- [ ] 7.4 运行 `./mvnw -q -DskipTests verify`。
- [x] 7.5 如果完整 Maven 验证无法完成，记录尝试过的完整命令、失败原因和已通过的窄范围验证。
