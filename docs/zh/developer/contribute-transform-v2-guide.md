---
title: 贡献 Transform-V2 插件
---

# 贡献 Transform-V2 插件

## 为什么需要这篇文档

Transform 贡献是给 SeaTunnel 增加用户可见能力时最容易入手的一类工作，但它的入口信息分散在 transform 文档、架构页和仓库 README 之间，新贡献者很容易找不到最短路径。

这篇文档的目标，就是把这条入口收拢起来。

## 从这里开始

如果你准备贡献一个 Transform 插件，建议先读下面这些页面：

- [贡献路径](./contribution-path.md)
- [搭建开发环境](./setup.md)
- [Transform 插件体系](../architecture/transform-plugin-system.md)
- [Transform 通用参数](../transforms/common-options/common-options.md)

这些页面会先解释开发环境、Transform 在整条作业链路里的位置，以及 `plugin_input` 和 `plugin_output` 如何把多个数据集串起来。

## 常见贡献类型

大多数 Transform 相关贡献，基本都落在下面几类里：

- 新增一个 Transform 插件
- 给现有 Transform 增加或调整一个 option
- 修复 schema、metadata 或 CDC 相关行为
- 改进 Transform 文档和示例

通常先做一个最小且聚焦的改动，会比直接发起大规模 Transform 重构更容易落地。

## 写代码前建议先看什么

在修改 `seatunnel-transforms-v2` 之前，至少先找一个相近的已有 Transform 实现，对照下面几个方面：

- factory 定义和参数校验
- 运行时 transform 契约
- schema 或 metadata 行为
- unit test 或 integration test
- 对应的 `docs/en` 与 `docs/zh` 页面

Transform 类改动如果能同时带上代码、文档和示例，通常更容易评审。

## 推荐阅读顺序

1. [Transform 插件体系](../architecture/transform-plugin-system.md)
2. [Transforms 目录](../transforms)
3. [Transform 通用参数](../transforms/common-options/common-options.md)
4. [核心 API 设计](../architecture/core-api-design.md)
5. [seatunnel-transforms-v2 README](https://github.com/apache/seatunnel/blob/dev/seatunnel-transforms-v2/README.zh.md)

## 什么时候继续看仓库级指南

如果你需要的是 `seatunnel-transforms-v2` 模块内部的约定、示例或更贴近源码目录的说明，再继续看仓库里的指南：

- [Transform-V2 贡献指南](https://github.com/apache/seatunnel/blob/dev/seatunnel-transforms-v2/README.zh.md)
