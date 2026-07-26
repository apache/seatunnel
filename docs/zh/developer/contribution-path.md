---
title: 贡献路径
---

# 贡献路径

## 为什么需要这篇文档

很多新贡献者不是缺少扩展点，而是缺少一条清晰入口：

- 环境搭建在一页
- connector 开发在另一页
- 社区联系方式散在 README 和 FAQ
- 架构参考又分布在多个目录

这篇文档的目标，就是给出一条稳定的 onboarding 路径。

## 这篇文档适合谁

如果你准备做下面这些事情，建议从本页开始：

- 修文档
- 贡献 connector 或 transform
- 修一个 bug
- 想先知道应该去哪里提问、再决定要不要提 PR

## 从最小可行入口开始

不要一上来就试图看懂整个仓库。先从离你目标最近的那种贡献开始。

### 文档贡献

比较好的起点包括：

- 修 broken link
- 改进 quick start 表达
- 让配置文档和真实 connector option 对齐
- 同时补齐英文和中文文档

建议先看：

- [快速入门总览](../getting-started/overview.md)
- [作业配置指南](../getting-started/job-configuration-guide.md)
- [文档格式规范](./docs-format-specification.md)

### Connector 贡献

比较好的起点包括：

- 修一个 connector 的 option 或文档不一致问题
- 给现有 connector 补一个小能力
- 在研究一个相近 connector 之后，再做全新的 source 或 sink

建议先看：

- [开发自己的 Connector](./how-to-create-your-connector.md)
- [Source Connector 开发指南](./source-connector-development.md)
- [Sink Connector 开发指南](./sink-connector-development.md)

### Transform 贡献

比较好的起点包括：

- 改一个已有 Transform 的 option 或示例
- 修一个聚焦的 schema 或 CDC 相关问题
- 在研究一个相近实现之后，再新增一个 Transform

建议先看：

- [贡献 Transform-V2 插件](./contribute-transform-v2-guide.md)
- [Transform 插件体系](../architecture/transform-plugin-system.md)
- [Transforms 目录](../transforms)

### 代码或架构贡献

比较好的起点包括：

- 先稳定复现一个具体 bug
- 先补一个聚焦的测试
- 先研究最小相关模块，再去改引擎

建议先看：

- [搭建开发环境](./setup.md)
- [架构概览](../architecture/overview.md)
- [核心 API 设计](../architecture/core-api-design.md)

## 推荐贡献流程

对大多数贡献者来说，最短且稳妥的路径通常是：

1. 先读你要改的功能对应的用户文档
2. 在本地复现当前行为
3. 在仓库里找一个最相近的已有实现
4. 用最小改动解决一个明确问题
5. 如果是用户可见变更，同时更新 `docs/en` 和 `docs/zh`

通常这比一开始就做大规模重构更容易落地。

## 去哪里提问

不同类型的问题，适合的渠道也不同：

- [GitHub Issues](https://github.com/apache/seatunnel/issues) 适合具体 bug、proposal 和跟踪
- [dev 邮件列表](https://lists.apache.org/list.html?dev@seatunnel.apache.org) 适合长线程设计讨论和项目级决策

如果你不确定去哪里问，通常可以先在 issue 里说明你的具体问题，以及你已经检查过什么。

## Maintainer 最希望你补齐什么

一条贡献如果具备这些信息，通常会明显更容易评审：

- 清楚的问题定义
- 尽量小且聚焦的改动范围
- 精确的配置名和示例
- 测试，或者说明为什么测试不现实
- 英文和中文文档同步更新

对代码贡献来说，不要把无关清理和真实修复混在一起。

## 哪类首个贡献更容易落地

这些类型通常更容易合入：

- 改一页文档，并同步 `en` + `zh`
- 修一个 connector 的 option 校验问题
- 补一个缺失的示例或错误提示
- 给已有 bug 补一个聚焦的 unit test 或 E2E test

这些类型通常需要更多上下文：

- 改引擎调度行为
- 大规模 connector 重构
- 修改公开配置名或默认值

## 贡献角色在实践中怎么理解

从日常协作角度看，最重要的演进其实很简单：

- 用户提出问题和缺口
- 贡献者提交修复和改进
- 长期贡献者再逐步更多参与评审和项目方向

对新贡献者来说，最重要的不是正式头衔，而是你的改动是否清晰、聚焦、容易验证。

## 推荐阅读顺序

按目标选择一条路径即可：

- 文档路径：[文档格式规范](./docs-format-specification.md) -> [快速入门总览](../getting-started/overview.md)
- connector 路径：[开发自己的 Connector](./how-to-create-your-connector.md) -> [Source Connector 开发指南](./source-connector-development.md) 或 [Sink Connector 开发指南](./sink-connector-development.md)
- transform 路径：[贡献 Transform-V2 插件](./contribute-transform-v2-guide.md) -> [Transform 插件体系](../architecture/transform-plugin-system.md)
- 引擎路径：[搭建开发环境](./setup.md) -> [架构概览](../architecture/overview.md)
