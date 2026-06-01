---
sidebar_position: 3
title: Configuration And Option System
---

# Configuration And Option System

SeaTunnel's configuration model is more than a set of free-form key-value pairs. It is a shared contract between connector authors, runtime validation, and operational tooling such as the REST API and Web UI.

This page provides the architecture view of that system. For end-user syntax and examples, continue with [Config Concept](../introduction/concepts/config.md).

## Why This Layer Matters

The configuration system solves three related problems at once:

- connector authors need a type-safe way to define options
- the runtime needs to validate configuration before executing a job
- tools need structured metadata to render forms and explain requirements

SeaTunnel addresses this through a small set of core building blocks:

- `Option`
- `OptionRule`
- `ReadonlyConfig`
- runtime validation and REST metadata exposure

## The Core Pieces

### `Option`

An `Option` defines a single configuration field:

- key
- type
- default value, when applicable
- description

This is the smallest reusable configuration contract in SeaTunnel.

Typical usage in a connector options class:

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

`Options.key(...)` builder supports the following type methods:

| Method | Java Type |
|--------|-----------|
| `stringType()` | `String` |
| `intType()` | `Integer` |
| `longType()` | `Long` |
| `doubleType()` | `Double` |
| `floatType()` | `Float` |
| `booleanType()` | `Boolean` |
| `listType()` | `List<String>` |
| `listType(Class<T>)` | `List<T>` |
| `mapType()` | `Map<String, String>` |
| `enumType(Class<E>)` | `Enum` subclass |
| `singleChoice(Class<T>, List<T>)` | single-choice with allowed values |
| `type(new TypeReference<T>() {})` | any custom type |

### `OptionRule`

An `OptionRule` describes how multiple options behave together. It can express rules such as:

- absolutely required options
- mutually exclusive options
- bundled options
- conditional options

This is how SeaTunnel moves beyond flat configuration and supports richer connector contracts.

A connector factory exposes its rules through the `optionRule()` method:

```java
@Override
public OptionRule optionRule() {
    return OptionRule.builder()
            .required(HOST, PORT)                              // absolutely required
            .exclusive(USERNAME, BEARER_TOKEN)                 // exactly one must be set
            .bundled(USERNAME, PASSWORD)                       // all or none
            .conditional(MODE, WriteMode.UPSERT, UPSERT_KEY)  // required when MODE == UPSERT
            .optional(BATCH_SIZE, RETRY_COUNT)                 // purely optional
            .build();
}
```

### Value Constraints (`Condition`)

Beyond structural rules (required, exclusive, etc.), options can carry **value-level constraints** that the runtime validates before a job starts. The `Condition` API provides a fluent way to attach these constraints inside `OptionRule.builder()`.

**Numeric range constraints:**

```java
OptionRule.builder()
        .required(PORT,
                Condition.greaterOrEqual(PORT, 1)
                        .and(Condition.lessOrEqual(PORT, 65535)))
        .build();
```

**String constraints:**

```java
OptionRule.builder()
        .required(HOST, Condition.notBlank(HOST))
        .optional(DB_NAME, Condition.upperCase(DB_NAME))
        .build();
```

**Cross-field comparison:**

```java
OptionRule.builder()
        .required(START_TS, END_TS,
                Condition.lessThanField(START_TS, END_TS))
        .build();
```

**Collection constraints:**

```java
OptionRule.builder()
        .required(TABLES,
                Condition.notEmpty(TABLES)
                        .and(Condition.unique(TABLES)))
        .build();
```

Available operators:

| Category | Operator | Description |
|----------|----------|-------------|
| Numeric | `greaterThan` | value > threshold |
| Numeric | `greaterOrEqual` | value >= threshold |
| Numeric | `lessThan` | value < threshold |
| Numeric | `lessOrEqual` | value <= threshold |
| String | `notBlank` | string is not empty or whitespace-only |
| String | `startsWith` | string starts with a given prefix |
| String | `endsWith` | string ends with a given suffix |
| String | `contains` | string contains a given substring |
| String | `matches` | string matches a regular expression |
| String | `upperCase` | string is all uppercase |
| String | `lowerCase` | string is all lowercase |
| String length | `lengthEqual` | string length == n |
| String length | `lengthGreaterOrEqual` | string length >= n |
| String length | `lengthLessOrEqual` | string length <= n |
| Collection | `notEmpty` | collection is not empty |
| Collection | `unique` | collection has no duplicate elements |
| Collection | `sizeEqual` | collection size == n |
| Collection | `sizeGreaterOrEqual` | collection size >= n |
| Collection | `sizeLessOrEqual` | collection size <= n |
| Cross-field | `lessThanField` | value < another option's value |
| Cross-field | `lessOrEqualField` | value <= another option's value |
| Cross-field | `greaterThanField` | value > another option's value |
| Cross-field | `greaterOrEqualField` | value >= another option's value |
| Cross-field | `fieldEqual` | value == another option's value |
| Cross-field | `fieldNotEqual` | value != another option's value |
| Cross-field | `fieldSizeEqual` | collection size == another collection's size |

:::tip
Multiple conditions can be chained with `.and(...)` or `.or(...)` to form compound constraints.
:::

### `ReadonlyConfig`

`ReadonlyConfig` is the runtime container from which connectors and transforms read their resolved values. It gives plugin implementations a stable, typed access pattern after parsing and validation have already happened.

```java
@Override
public void prepare(Config pluginConfig) {
    ReadonlyConfig config = ReadonlyConfig.fromConfig(pluginConfig);
    String host = config.get(HOST);     // typed access, never returns raw Object
    int port = config.get(PORT);        // default applied automatically if not set
}
```

## Validation Flow

At a high level, configuration flows through the system like this:

1. A plugin defines `Option` and `OptionRule` metadata (including value constraints).
2. A user writes HOCON, JSON, or SQL-based job configuration.
3. SeaTunnel parses the configuration into a runtime representation.
4. `ConfigValidator` checks structural rules (required, exclusive, bundled, conditional) and then evaluates value constraints by delegating each `Condition` to `ConditionEvaluators`.
5. The resolved values are exposed to the runtime through `ReadonlyConfig`.
6. The same metadata can also be exposed through REST for UI rendering and automation.

When validation fails, `OptionValidationException` is thrown with a message that includes the constraint expression and the option key involved.

## OptionRule Pattern Guide

Validation logic declared in `optionRule()` runs at job submission time, produces uniform error messages, and is automatically exposed to the REST API and Web UI. Placing validation in Config constructors or Writer/Reader code delays failure to task startup time and hides constraints from tooling.

The following patterns cover common scenarios. Each one shows the recommended declarative form inside `OptionRule.builder()`.

### Required fields

Some fields must always be present. A job that omits them should be rejected at submission.

```java
.required(HOST, PORT, DATABASE)
```

### Mutually exclusive options

When only one of several options should be set at a time, `exclusive` enforces the constraint.

```java
.exclusive(TOPIC, TOPIC_PATTERN)
```

### Bundled options

A group of options that only make sense together. Either all of them are set or none.

```java
.bundled(USERNAME, PASSWORD)
```

### Conditional required options driven by an enum

When an enum option takes a specific value, additional fields become required.

```java
.conditional(START_MODE, StartMode.TIMESTAMP, START_MODE_TIMESTAMP)
.conditional(START_MODE, StartMode.SPECIFIC_OFFSETS, START_MODE_OFFSETS)
```

### Conditional required options driven by a boolean

A boolean toggle that activates different sets of required fields depending on its value.

```java
.conditional(IS_EXACTLY_ONCE, true, XA_DATA_SOURCE_CLASS, TRANSACTION_TIMEOUT)
.conditional(IS_EXACTLY_ONCE, false, MAX_RETRIES)
```

### Numeric range

Port numbers, batch sizes, ratios, and similar numeric fields often have valid ranges.

```java
.required(PORT,
        Condition.greaterOrEqual(PORT, 1)
                .and(Condition.lessOrEqual(PORT, 65535)))
```

### String format and content

Host names that must not be blank, identifiers that must be uppercase, or endpoints that must match a pattern.

```java
.required(HOST, Condition.notBlank(HOST))
.required(DATABASE, Condition.upperCase(DATABASE))
.required(ENDPOINT, Condition.matches(ENDPOINT, "^[^:]+:\\d+$"))
```

### Cross-field comparison

When the value of one option must be smaller or larger than another.

```java
.required(START_TS, END_TS,
        Condition.lessThanField(START_TS, END_TS))
```

### Collection constraints

Lists that must not be empty, or whose elements must be unique.

```java
.required(TABLES,
        Condition.notEmpty(TABLES)
                .and(Condition.unique(TABLES)))
```

### Compound constraints

Multiple conditions combined with `.and(...)` or `.or(...)`.

```java
.required(RATIO,
        Condition.greaterThan(RATIO, 0.0)
                .and(Condition.lessOrEqual(RATIO, 1.0)))
```

## Why It Matters For Operators

This architecture is also what makes the `option-rules` REST endpoint useful. Tools can inspect the runtime metadata of installed connectors and dynamically understand:

- which fields are required
- which fields are conditional
- what value constraints apply (numeric ranges, patterns, cross-field rules)
- which defaults are active on the running server

That is why the option system sits at the boundary of both developer experience and operations.

## Recommended Reading

- End-user syntax: [Config Concept](../introduction/concepts/config.md)
- Engine-specific environment fields: [JobEnvConfig](../introduction/configuration/JobEnvConfig.md)
- SQL-oriented jobs: [SQL configuration](../introduction/configuration/sql-config.md)
- Runtime metadata exposure: [RESTful API V2](../engines/zeta/rest-api-v2.md)
