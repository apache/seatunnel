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

### `OptionRule`

An `OptionRule` describes how multiple options behave together. It can express rules such as:

- absolutely required options
- mutually exclusive options
- bundled options
- conditional options

This is how SeaTunnel moves beyond flat configuration and supports richer connector contracts.

### `ReadonlyConfig`

`ReadonlyConfig` is the runtime container from which connectors and transforms read their resolved values. It gives plugin implementations a stable, typed access pattern after parsing and validation have already happened.

## End-To-End Flow

At a high level, configuration flows through the system like this:

1. A plugin defines `Option` and `OptionRule` metadata.
2. A user writes HOCON, JSON, or SQL-based job configuration.
3. SeaTunnel parses the configuration into a runtime representation.
4. Validation applies the connector rules.
5. The resolved values are exposed to the runtime through `ReadonlyConfig`.
6. The same metadata can also be exposed through REST for UI rendering and automation.

## Why It Matters For Operators

This architecture is also what makes the `option-rules` REST endpoint useful. Tools can inspect the runtime metadata of installed connectors and dynamically understand:

- which fields are required
- which fields are conditional
- which defaults are active on the running server

That is why the option system sits at the boundary of both developer experience and operations.

## Recommended Reading

- End-user syntax: [Config Concept](../introduction/concepts/config.md)
- Engine-specific environment fields: [JobEnvConfig](../introduction/configuration/JobEnvConfig.md)
- SQL-oriented jobs: [SQL configuration](../introduction/configuration/sql-config.md)
- Runtime metadata exposure: [RESTful API V2](../engines/zeta/rest-api-v2.md)
