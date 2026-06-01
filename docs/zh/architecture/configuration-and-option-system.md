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

在 Connector 选项类中的典型用法：

```java
public static final Option<Integer> PORT =
        Options.key("port")
                .intType()
                .defaultValue(3306)
                .withDescription("Database server port");

public static final Option<String> HOST =
        Options.key("host")
                .stringType()
                .noDefaultValue()
                .withDescription("Database server hostname");

public static final Option<List<String>> TABLES =
        Options.key("tables")
                .listType()
                .noDefaultValue()
                .withDescription("List of tables to read");
```

`Options.key(...)` 构建器支持以下类型方法：

| 方法 | Java 类型 |
|------|-----------|
| `stringType()` | `String` |
| `intType()` | `Integer` |
| `longType()` | `Long` |
| `doubleType()` | `Double` |
| `floatType()` | `Float` |
| `booleanType()` | `Boolean` |
| `listType()` | `List<String>` |
| `listType(Class<T>)` | `List<T>` |
| `mapType()` | `Map<String, String>` |
| `enumType(Class<E>)` | `Enum` 子类 |
| `singleChoice(Class<T>, List<T>)` | 单选，限定允许值列表 |
| `type(new TypeReference<T>() {})` | 任意自定义类型 |

### `OptionRule`

`OptionRule` 用于表达多个配置项之间的组合规则，例如：

- 必填项
- 互斥项
- 成组项
- 条件项

这也是 SeaTunnel 能够表达复杂连接器配置约束，而不仅仅是平铺参数列表的关键。

Connector 的 Factory 通过 `optionRule()` 方法暴露其规则：

```java
@Override
public OptionRule optionRule() {
    return OptionRule.builder()
            .required(HOST, PORT)                              // 必填项
            .exclusive(USERNAME, BEARER_TOKEN)                 // 互斥：只能设置其一
            .bundled(USERNAME, PASSWORD)                       // 成组：全部设置或全部不设
            .conditional(MODE, WriteMode.UPSERT, UPSERT_KEY)  // 条件：当 MODE == UPSERT 时必填
            .optional(BATCH_SIZE, RETRY_COUNT)                 // 可选项
            .build();
}
```

### 值约束（`Condition`）

除了结构性规则（必填、互斥等），配置项还可以携带**值级别约束**，运行时会在作业启动前进行校验。`Condition` API 提供了一种流式方式，在 `OptionRule.builder()` 中附加这些约束。

**数值范围约束：**

```java
OptionRule.builder()
        .required(PORT,
                Condition.greaterOrEqual(PORT, 1)
                        .and(Condition.lessOrEqual(PORT, 65535)))
        .build();
```

**字符串约束：**

```java
OptionRule.builder()
        .required(HOST, Condition.notBlank(HOST))
        .optional(DB_NAME, Condition.upperCase(DB_NAME))
        .build();
```

**跨字段比较：**

```java
OptionRule.builder()
        .required(START_TS, END_TS,
                Condition.lessThanField(START_TS, END_TS))
        .build();
```

**集合约束：**

```java
OptionRule.builder()
        .required(TABLES,
                Condition.notEmpty(TABLES)
                        .and(Condition.unique(TABLES)))
        .build();
```

可用的操作符：

| 类别 | 操作符 | 说明 |
|------|--------|------|
| 数值 | `greaterThan` | 值 > 阈值 |
| 数值 | `greaterOrEqual` | 值 >= 阈值 |
| 数值 | `lessThan` | 值 < 阈值 |
| 数值 | `lessOrEqual` | 值 <= 阈值 |
| 字符串 | `notBlank` | 字符串非空且不全为空白字符 |
| 字符串 | `startsWith` | 字符串以指定前缀开头 |
| 字符串 | `endsWith` | 字符串以指定后缀结尾 |
| 字符串 | `contains` | 字符串包含指定子串 |
| 字符串 | `matches` | 字符串匹配正则表达式 |
| 字符串 | `upperCase` | 字符串全部大写 |
| 字符串 | `lowerCase` | 字符串全部小写 |
| 字符串长度 | `lengthEqual` | 字符串长度 == n |
| 字符串长度 | `lengthGreaterOrEqual` | 字符串长度 >= n |
| 字符串长度 | `lengthLessOrEqual` | 字符串长度 <= n |
| 集合 | `notEmpty` | 集合非空 |
| 集合 | `unique` | 集合元素无重复 |
| 集合 | `sizeEqual` | 集合大小 == n |
| 集合 | `sizeGreaterOrEqual` | 集合大小 >= n |
| 集合 | `sizeLessOrEqual` | 集合大小 <= n |
| 跨字段 | `lessThanField` | 值 < 另一个配置项的值 |
| 跨字段 | `lessOrEqualField` | 值 <= 另一个配置项的值 |
| 跨字段 | `greaterThanField` | 值 > 另一个配置项的值 |
| 跨字段 | `greaterOrEqualField` | 值 >= 另一个配置项的值 |
| 跨字段 | `fieldEqual` | 值 == 另一个配置项的值 |
| 跨字段 | `fieldNotEqual` | 值 != 另一个配置项的值 |
| 跨字段 | `fieldSizeEqual` | 集合大小 == 另一个集合的大小 |

:::tip
多个条件可以通过 `.and(...)` 或 `.or(...)` 链式组合成复合约束。
:::

### `ReadonlyConfig`

`ReadonlyConfig` 是运行时读取参数的统一容器。配置经过解析与校验后，Connector 和 Transform 会从这里以稳定、类型化的方式获取最终值。

```java
@Override
public void prepare(Config pluginConfig) {
    ReadonlyConfig config = ReadonlyConfig.fromConfig(pluginConfig);
    String host = config.get(HOST);     // 类型化访问，不会返回原始 Object
    int port = config.get(PORT);        // 未设置时自动应用默认值
}
```

## 校验流程

从整体上看，配置会沿着下面的链路在系统内流动：

1. 插件定义 `Option` 与 `OptionRule`（包括值约束）。
2. 用户编写 HOCON、JSON 或 SQL 配置。
3. SeaTunnel 解析配置。
4. `ConfigValidator` 先检查结构性规则（必填、互斥、成组、条件），再将每个 `Condition` 委托给 `ConditionEvaluators` 执行值约束校验。
5. 运行时通过 `ReadonlyConfig` 获取已解析参数。
6. 同一套元数据还可以通过 REST 暴露给 UI 或自动化系统。

校验失败时，`OptionValidationException` 会被抛出，异常消息中包含约束表达式和涉及的配置项 key。

## OptionRule 模式编码指南

在 `optionRule()` 中声明的校验逻辑会在作业提交时执行，产出统一格式的错误消息，且自动暴露给 REST API 和 Web UI。如果把校验写在 Config 构造器或 Writer/Reader 中，失败时机会推迟到任务调度之后，工具侧也无法感知这些约束。

以下按常见场景列出推荐的声明式写法，均在 `OptionRule.builder()` 中使用。

### 必填字段

某些字段必须配置，缺少时作业在提交阶段即被拒绝。

```java
.required(HOST, PORT, DATABASE)
```

### 互斥选项

多个选项中只能选择一个，同时配置会报错。

```java
.exclusive(TOPIC, TOPIC_PATTERN)
```

### 成组选项

一组选项要么全部配置，要么全部留空。

```java
.bundled(USERNAME, PASSWORD)
```

### 条件必填（枚举驱动）

当某个枚举字段取特定值时，另一些字段才变为必填。

```java
.conditional(START_MODE, StartMode.TIMESTAMP, START_MODE_TIMESTAMP)
.conditional(START_MODE, StartMode.SPECIFIC_OFFSETS, START_MODE_OFFSETS)
```

### 条件必填（布尔驱动）

布尔开关控制不同的必填字段集合。

```java
.conditional(IS_EXACTLY_ONCE, true, XA_DATA_SOURCE_CLASS, TRANSACTION_TIMEOUT)
.conditional(IS_EXACTLY_ONCE, false, MAX_RETRIES)
```

### 数值范围

端口号、batch size、比率等数值字段通常有合法范围。

```java
.required(PORT,
        Condition.greaterOrEqual(PORT, 1)
                .and(Condition.lessOrEqual(PORT, 65535)))
```

### 字符串格式与内容

字段不能为空白、标识符必须全大写、或需要匹配特定格式。

```java
.required(HOST, Condition.notBlank(HOST))
.required(DATABASE, Condition.upperCase(DATABASE))
.required(ENDPOINT, Condition.matches(ENDPOINT, "^[^:]+:\\d+$"))
```

### 跨字段比较

一个字段的值必须小于或大于另一个字段。

```java
.required(START_TS, END_TS,
        Condition.lessThanField(START_TS, END_TS))
```

### 集合约束

列表不能为空，或元素不能重复。

```java
.required(TABLES,
        Condition.notEmpty(TABLES)
                .and(Condition.unique(TABLES)))
```

### 复合约束

多个条件通过 `.and(...)` 或 `.or(...)` 链式组合。

```java
.required(RATIO,
        Condition.greaterThan(RATIO, 0.0)
                .and(Condition.lessOrEqual(RATIO, 1.0)))
```

## 为什么这对运维也重要

这套设计也是 `option-rules` REST 接口能够成立的原因。运维平台或 UI 可以通过运行时元数据动态获知：

- 哪些字段是必填的
- 哪些字段受条件约束
- 有哪些值约束（数值范围、字符串模式、跨字段规则）
- 当前服务端实际安装 Connector 的默认值和规则

因此，配置与 Option 系统不仅是开发者能力，也是运维能力的一部分。

## 推荐继续阅读

- 面向用户的配置语法： [配置文件简介](../introduction/concepts/config.md)
- 引擎环境参数： [JobEnvConfig](../introduction/configuration/JobEnvConfig.md)
- SQL 作业配置： [SQL 配置](../introduction/configuration/sql-config.md)
- 运行时元数据暴露： [RESTful API V2](../engines/zeta/rest-api-v2.md)
