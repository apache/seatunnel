---
title: 贡献性能优化
---

<!--
Licensed to the Apache Software Foundation (ASF) under one or more
contributor license agreements. See the NOTICE file distributed with
this work for additional information regarding copyright ownership.
The ASF licenses this file to you under the Apache License, Version 2.0
(the "License"); you may not use this file except in compliance with
the License. You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
-->

# 贡献性能优化

SeaTunnel 鼓励贡献者解决已经发现的真实性能问题。Benchmark 不是寻找问题的工具，而是用于
验证问题是否来自某条生产路径。一个方法被频繁调用，并不能直接证明它是瓶颈。

```text
发现异常 → 提出怀疑并验证 → Benchmark PR → 合入 dev → 修复 PR → 对比两个版本
```

## 从已发现的问题到瓶颈

性能贡献通常始于贡献者在生产运行、压测或故障排查中已经发现的异常，例如 OOM、Checkpoint
超时、CPU 打满、吞吐下降、延迟增长或线程停顿。本文不要求为了寻找贡献目标而遍历指标；
它关注的是如何验证已经发现的问题。

```text
发现异常 → 怀疑某个生产路径 → 受控复现 → 证实或排除
```

| 阶段 | 判断依据 |
|---|---|
| 发现异常 | 问题发生在明确的负载和环境中，并且能够描述它对吞吐、延迟、资源或作业运行的影响。 |
| 提出怀疑 | 日志、Metrics、线程栈或 Profile 将异常指向某个生产路径，并能解释为什么该路径可能造成问题。 |
| 证实或排除 | 受控实验能够复现原问题；改变该路径的成本后，原有系统指标按预期变化且结果可以重复。否则应排除该路径，继续定位。 |

Benchmark 用于复现和验证已经怀疑的路径，不用于在代码库中广泛寻找可能的热点。仅仅发现方法
调用频繁、CPU 占比较高或存在锁样本，还不能证明它是瓶颈。

例如，发现 Checkpoint 超时后，如果已有证据将问题指向状态序列化，就用可控状态数据复现
序列化成本，并验证降低该成本是否同时缩短 Checkpoint。若微基准更快但 Checkpoint 没有改善，
该路径就不能解释原问题。

## 构建可复现的实验

选择能够触达已定位生产路径的负载，定义逻辑操作、输入形态、并发度和计时范围。除非测试目标
就是 Fixture 创建，否则数据准备和结果校验应放在计时之外。

必须校验输出，避免操作虽然更快，却没有完成预期工作。在受控条件下重复实验，确认问题能够
稳定复现。构建、运行和 Profiling 命令见[Zeta 基准测试](../engines/zeta/benchmark.md)。

## 单独提交 Benchmark

实验确认存在可复现的瓶颈后，再创建一个聚焦的 Benchmark PR，只包含 Benchmark、确定性的
Fixture、验证测试以及同步的中英文文档，不要在这个 PR 中加入性能优化。

Benchmark PR 合入 `dev` 后，记录它的合并 Commit。该 Commit 才是新 Benchmark 可以使用的
第一个有效 Baseline。

:::caution 两个版本必须使用相同的 Benchmark

如果 Benchmark 只存在于性能修复 PR，Baseline 就无法运行同一测试；如果两个版本使用的
Benchmark 或 Fixture 不同，结果也无法单独证明生产代码改动带来的影响。

:::

## 提交并测量性能修复

从已经包含该 Benchmark 的 `dev` 版本创建性能修复分支。实现优化时，保持 Benchmark 及其
参数不变。

运行 `Benchmarks` Workflow 时：

- `seatunnel_ref` 填写精确的 Baseline Commit；
- `pr_number` 填写性能修复 PR 编号；
- 两个版本使用相同的 Benchmark 方法、参数和 JDK。

使用不带 Profiler 的对比量化性能改善或回退。Profiling 可以解释原因，但它引入的额外开销
使其 Score 不适合作为对比结果。

## 分享可复现的证据

在性能修复 PR 中附上：

- Baseline 和 Candidate Commit SHA；
- 精确的 Benchmark 方法和负载参数；
- JDK、JVM 设置和相关机器信息；
- 对比报告和原始 JMH 结果；
- 测量边界以及它能够支持的结论。

新 Benchmark 应先按需运行。只有当负载具有代表性、运行时间可控，并且多轮结果足够稳定时，
才适合加入定时测试套件。
