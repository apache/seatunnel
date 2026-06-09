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

Beyond structural rules (required, exclusive, etc.), options can carry **value-level constraints** that the runtime validates before a job starts. The `Condition` API provides a fluent way to attach these constraints inside `OptionRule.builder()`. See the [OptionRule Pattern Guide](#optionrule-pattern-guide) below for usage examples.

Available operators (all accessed via the `Conditions` factory class):

| Category | Method | Description |
|----------|--------|-------------|
| Equality | `Condition.of(option, value)` | value == expected (legacy API) |
| Equality | `Condition.of(option, NOT_EQUAL, value)` | value != expected |
| Numeric | `greaterThan(option, threshold)` | value > threshold |
| Numeric | `greaterOrEqual(option, threshold)` | value >= threshold |
| Numeric | `lessThan(option, threshold)` | value < threshold |
| Numeric | `lessOrEqual(option, threshold)` | value <= threshold |
| String | `notBlank(option)` | string is not empty or whitespace-only |
| String | `startsWith(option, prefix)` | string starts with a given prefix |
| String | `contains(option, substring)` | string contains a given substring |
| String | `matches(option, regex)` | string matches a regular expression |
| String | `upperCase(option)` | string is all uppercase |
| String | `lowerCase(option)` | string is all lowercase |
| Collection | `notEmpty(option)` | collection is not empty |
| Collection | `unique(option)` | collection has no duplicate elements |
| Map | `mapNotEmpty(option)` | map is not empty |
| Map | `mapContainsKey(option, key)` | map contains the specified key |
| Map | `mapContainsKeys(option, key1, key2, ...)` | map contains all specified keys |
| Cross-field | `lessThanField(option, other)` | value < another option's value |
| Cross-field | `lessOrEqualField(option, other)` | value <= another option's value |
| Cross-field | `greaterThanField(option, other)` | value > another option's value |
| Cross-field | `greaterOrEqualField(option, other)` | value >= another option's value |

:::tip
Multiple conditions can be chained with `.and(...)` or `.or(...)` to form compound constraints. AND binds tighter than OR, so `A.or(B).and(C)` evaluates as `A || (B && C)`.
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

When validation fails, `OptionValidationException` is thrown with a structured error message. See the [Validation Error Messages](#validation-error-messages) section below for details.

## Validation Error Messages

Option validation errors are thrown as `OptionValidationException`, a subclass of `SeaTunnelRuntimeException`, carrying the error code `API-02`. The message always begins with:

```
ErrorCode:[API-02], ErrorDescription:[Option item validate failed]
```

Structural (required, bundled, exclusive, conditional) and value constraint errors are aggregated into a single numbered list. Each entry follows a consistent three-line format with a `type` label (`required` / `bundled` / `exclusive` / `conditional` / `value`) for easy identification. Structural errors come first. If a required option is absent, its value constraint is automatically suppressed to avoid redundant noise.

```
ErrorCode:[API-02], ErrorDescription:[Option item validate failed] -
Option validation failed (4 errors):
  [1] option: 'host'
      type: required
      constraint: required option is not configured
  [2] options: 'username', 'password'
      type: bundled
      constraint: bundled options must be present or absent together (present: ['username'], absent: ['password'])
  [3] option: port
      type: value
      constraint: 'port' >= 1
  [4] option: start_ts
      type: value
      constraint: 'start_ts' < 'end_ts'
```

## OptionRule Pattern Guide

Validation logic declared in `optionRule()` runs at job submission time, produces uniform error messages, and is automatically exposed to the REST API and Web UI. Placing validation in Config constructors or Writer/Reader code delays failure to task startup time and hides constraints from tooling.

The following patterns cover common scenarios. Each one shows the recommended declarative form inside `OptionRule.builder()`.

Quick reference:

| Scenario | Recommended API |
|----------|------------------|
| Always required fields | `.required(opt...)` |
| Exactly one in a set | `.exclusive(opt...)` |
| All-or-none group | `.bundled(opt...)` |
| Required only when trigger matches | `.conditional(trigger, value, requiredOpt...)` |
| Validate value only when trigger matches | `.conditional(trigger, value, condition...)` |
| Optional field with value check when present | `.optional(opt, condition...)` |
| Cross-field comparisons | `Conditions.lessThanField/greaterThanField(...)` |

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

When an enum option takes a specific value, additional fields become required. The method signature is:

```
.conditional(triggerOption, triggerValue, requiredOption...)
```

Meaning: when the user sets `triggerOption` to `triggerValue`, all listed `requiredOption` fields become mandatory.

```java
// When START_MODE = TIMESTAMP, START_MODE_TIMESTAMP becomes required
.conditional(START_MODE, StartMode.TIMESTAMP, START_MODE_TIMESTAMP)
// When START_MODE = SPECIFIC_OFFSETS, START_MODE_OFFSETS becomes required
.conditional(START_MODE, StartMode.SPECIFIC_OFFSETS, START_MODE_OFFSETS)
```

### Conditional required options driven by a boolean

Same pattern as enum-driven, but the trigger value is a boolean.

```java
// When IS_EXACTLY_ONCE = true, XA_DATA_SOURCE_CLASS and TRANSACTION_TIMEOUT become required
.conditional(IS_EXACTLY_ONCE, true, XA_DATA_SOURCE_CLASS, TRANSACTION_TIMEOUT)
// When IS_EXACTLY_ONCE = false, MAX_RETRIES becomes required
.conditional(IS_EXACTLY_ONCE, false, MAX_RETRIES)
```

### Numeric range

Port numbers, batch sizes, ratios, and similar numeric fields often have valid ranges.

```java
.required(PORT,
        Conditions.greaterOrEqual(PORT, 1)
                .and(Conditions.lessOrEqual(PORT, 65535)))
```

### String format and content

Host names that must not be blank, identifiers that must be uppercase, or endpoints that must match a pattern.

```java
.required(HOST, Conditions.notBlank(HOST))
.required(DATABASE, Conditions.upperCase(DATABASE))
.required(ENDPOINT, Conditions.matches(ENDPOINT, "^[^:]+:\\d+$"))
```

### Cross-field comparison

When the value of one option must be smaller or larger than another.

```java
.required(START_TS, END_TS,
        Conditions.lessThanField(START_TS, END_TS))
```

### Collection constraints

Lists that must not be empty, or whose elements must be unique.

```java
.required(TABLES,
        Conditions.notEmpty(TABLES)
                .and(Conditions.unique(TABLES)))
```

### Map constraints

Map must not be empty:

```java
.required(PROPERTIES, Conditions.mapNotEmpty(PROPERTIES))
```

Map must contain a specific key:

```java
.required(KAFKA_CONFIG, Conditions.mapContainsKey(KAFKA_CONFIG, "bootstrap.servers"))
```

Map must contain multiple keys simultaneously:

```java
.required(JDBC_PROPS, Conditions.mapContainsKeys(JDBC_PROPS, "url", "driver", "user"))
```

### Compound constraints with AND

Multiple conditions combined with `.and(...)`. All conditions must hold.

```java
.required(RATIO,
        Conditions.greaterThan(RATIO, 0.0)
                .and(Conditions.lessOrEqual(RATIO, 1.0)))
```

### OR chain — at least one alternative must pass

When the user can satisfy the constraint through any one of several options, use `.or(...)`. The constraint passes as long as at least one branch succeeds.

```java
// At least one of HOST or ENDPOINT must be non-blank
.optional(HOST, Conditions.notBlank(HOST).or(Conditions.notBlank(ENDPOINT)))
.optional(ENDPOINT)
```

### Mixed AND / OR chain

AND binds tighter than OR, so `A.or(B.and(C))` evaluates as `A || (B && C)`. This is useful when one simple condition can serve as a fallback for a stricter compound check.

```java
// Valid if HOST is non-blank, OR if PORT is within range [1, 65535]
.optional(HOST,
        Conditions.notBlank(HOST)
                .or(Conditions.greaterOrEqual(PORT, 1)
                        .and(Conditions.lessOrEqual(PORT, 65535))))
.optional(PORT)
```

### Conditional required vs conditional value constraint

:::tip

These two forms look similar but mean different things:

- `conditional(trigger, value, option...)` makes options conditionally required.
- `conditional(trigger, value, condition...)` only validates values when the target option is present; it does not make that option required.

:::

```java
// A) Conditionally required field
.conditional(START_MODE, StartMode.TIMESTAMP, START_TIMESTAMP)

// B) Optional field with conditional value validation
.conditional(START_MODE, StartMode.TIMESTAMP,
        Conditions.greaterThan(START_TIMESTAMP, 0L))

// C) Required + value constraint (combine A and B)
.conditional(START_MODE, StartMode.TIMESTAMP, START_TIMESTAMP)
.conditional(START_MODE, StartMode.TIMESTAMP,
        Conditions.greaterThan(START_TIMESTAMP, 0L))
```

### Optional with value constraint

An optional field that, when present, must satisfy a constraint. If the field is absent, the constraint is skipped entirely.

```java
.optional(BATCH_SIZE,
        Conditions.greaterOrEqual(BATCH_SIZE, 1)
                .and(Conditions.lessOrEqual(BATCH_SIZE, 10000)))
```

### Optional cross-field constraint

When two optional fields are provided together, their values must satisfy a cross-field rule. If either field is absent, the constraint is skipped.

```java
.optional(START_TS, END_TS,
        Conditions.lessThanField(START_TS, END_TS))
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
