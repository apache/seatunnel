---
sidebar_position: 3
title: 配置与 Option 系统
---

# 配置与 Option 系统

SeaTunnel 的配置系统并不只是简单的键值对集合。它同时服务于 Connector 开发者、运行时校验，以及 REST API 和 Web UI 这样的运维工具。

本页从架构层面解释这套系统。若你更关心配置语法和直接可运行的样例，请继续阅读 [配置文件简介](../introduction/concepts/config.md)。

## 为什么这一层很重要

这套配置系统同时解决了三类问题：

- Connector 开发者需要一种类型安全的方式来定义参数
- 运行时需要在任务启动前校验配置是否合法
- 工具侧需要结构化元数据来渲染表单和解释参数依赖关系

SeaTunnel 通过以下几个核心构件把这三件事连接起来：

- `Option`
- `OptionRule`
- `ReadonlyConfig`
- 运行时校验与 REST 元数据暴露

## 核心组成

### `Option`

`Option` 用于定义单个配置项，通常包含：

- key
- type
- 默认值
- 描述

它是 SeaTunnel 配置契约中最小、最基础的单元。

### `OptionRule`

`OptionRule` 用于表达多个配置项之间的组合规则，例如：

- 必填项
- 互斥项
- 成组项
- 条件项

这也是 SeaTunnel 能够表达复杂连接器配置约束，而不仅仅是平铺参数列表的关键。

### `ReadonlyConfig`

`ReadonlyConfig` 是运行时读取参数的统一容器。配置经过解析与校验后，Connector 和 Transform 会从这里以稳定、类型化的方式获取最终值。

## 端到端的数据流

从整体上看，配置会沿着下面的链路在系统内流动：

1. 插件定义 `Option` 与 `OptionRule`
2. 用户编写 HOCON、JSON 或 SQL 配置
3. SeaTunnel 解析配置
4. 校验器根据规则执行校验
5. 运行时通过 `ReadonlyConfig` 获取已解析参数
6. 同一套元数据还可以通过 REST 暴露给 UI 或自动化系统

## 为什么这对运维也重要

这套设计也是 `option-rules` REST 接口能够成立的原因。运维平台或 UI 可以通过运行时元数据动态获知：

- 哪些字段是必填的
- 哪些字段受条件约束
- 当前服务端实际安装 Connector 的默认值和规则

因此，配置与 Option 系统不仅是开发者能力，也是运维能力的一部分。

## 推荐继续阅读

- 面向用户的配置语法： [配置文件简介](../introduction/concepts/config.md)
- 引擎环境参数： [JobEnvConfig](../introduction/configuration/JobEnvConfig.md)
- SQL 作业配置： [SQL 配置](../introduction/configuration/sql-config.md)
- 运行时元数据暴露： [RESTful API V2](../engines/zeta/rest-api-v2.md)
