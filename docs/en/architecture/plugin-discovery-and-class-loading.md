---
title: Plugin Discovery and Class Loading
---

# Plugin Discovery and Class Loading

## Why This Page Exists

SeaTunnel documentation already explains how to configure connectors and how isolated dependencies are laid out in the binary package. What is still easy to miss is the runtime path between a job config and a loaded plugin instance.

This page explains that path at a system level:

- how a configured plugin name becomes a factory implementation
- how plugin metadata is discovered
- how connector jars and dependency jars are located
- why class loader isolation matters, especially in Zeta

## The Problem SeaTunnel Solves

SeaTunnel needs to support a large plugin ecosystem across sources, sinks, transforms, and formats. At runtime it has to answer a few questions safely:

- Which plugin implementation matches the user config?
- Where are that plugin's jars and isolated dependencies?
- How do we load them without polluting the whole process classpath?
- How do we expose plugin metadata to CLI, REST API, and UI features?

Without a discovery and class-loading layer, adding connectors would quickly lead to dependency conflicts and fragile startup behavior.

## End-to-End Runtime Path

At a high level, a plugin goes through this path:

```text
Job Config
  -> parse plugin name
  -> discover factory
  -> validate options
  -> resolve connector jar and isolated dependencies
  -> create plugin classloader
  -> instantiate source / sink / transform
  -> execute inside engine runtime
```

Each step matters for a different reason:

- discovery finds the correct plugin implementation
- validation ensures required options are present
- jar resolution determines what code is available
- classloader isolation reduces dependency conflicts

## Discovery Model

### Plugin Identity

At runtime SeaTunnel distinguishes plugins by a logical identity, typically combining:

- engine type
- plugin type, such as source or sink
- plugin name

This identity is used in multiple places:

- config parsing
- plugin lookup
- option metadata exposure
- log messages and diagnostics

### Factory Discovery

Most user-visible plugins are created through factory interfaces. In practice, SeaTunnel relies on Java SPI and factory discovery helpers to locate implementations declared under `META-INF/services`.

Typical examples include:

- `TableSourceFactory`
- `TableSinkFactory`
- transform factories
- catalog and format factories where applicable

This is why connector development docs require factory registration and service metadata.

Related docs:

- [Source Connector Development](../developer/source-connector-development.md)
- [Sink Connector Development](../developer/sink-connector-development.md)

## Jar Resolution and Packaging

SeaTunnel separates the plugin jar from connector-specific third-party dependencies.

Typical runtime layout:

```text
SEATUNNEL_HOME/
  connectors/
    connector-jdbc-<version>.jar
  plugins/
    connector-jdbc/
      dependency-a.jar
      dependency-b.jar
```

The mapping between plugin name and dependency directory is managed through `plugin-mapping.properties`.

This gives SeaTunnel two useful properties:

- the connector implementation jar can be shipped centrally
- connector-specific dependency jars can stay isolated from each other

Related docs:

- [Connector Isolated Dependency Loading Mechanism](../connectors/connector-isolated-dependency.md)

## Class Loading in Practice

### Why Isolation Matters

Different connectors may depend on different versions of the same third-party library. If all jars were loaded into one flat classpath, a single dependency conflict could break unrelated connectors in the same job.

Isolation helps in three places:

- connector startup
- task deployment
- long-running multi-connector jobs

### Zeta vs Flink / Spark

The most important practical difference is that SeaTunnel Engine (Zeta) gives stronger connector dependency isolation for job execution. The existing isolated dependency document already calls out that Spark and Flink still have tighter classpath sharing, so mixed versions remain more risky there.

That distinction is important when:

- adding a connector with a heavy dependency tree
- troubleshooting `ClassNotFoundException` or `NoSuchMethodError`
- packaging jobs for a distributed cluster

## Metadata Discovery and Option Exposure

Plugin discovery is not only used to instantiate runtime classes. It also powers metadata-oriented capabilities, such as exposing connector options to REST API and Web UI clients.

This is why the plugin system needs access to factory-level metadata such as:

- supported plugin identifier
- `OptionRule`
- required and optional fields
- grouping and validation semantics

Related docs:

- [Configuration And Option System](./configuration-and-option-system.md)
- [REST API and Web UI](../engines/zeta/rest-api-and-web-ui.md)

## Failure Modes and What They Usually Mean

When plugin loading fails, the symptom often tells you which layer is broken.

### Discovery Failures

Typical symptoms:

- plugin not found
- connector name recognized in docs but not in runtime

Usually means:

- missing connector jar
- missing SPI registration
- wrong plugin identifier

### Validation Failures

Typical symptoms:

- job submission fails before execution
- REST or UI metadata exists, but config is rejected

Usually means:

- required options missing
- mutually exclusive options violated
- option names differ from factory definitions

### Classpath / ClassLoader Failures

Typical symptoms:

- `ClassNotFoundException`
- `NoClassDefFoundError`
- `NoSuchMethodError`
- version conflicts only on some engines

Usually means:

- dependency jar missing from plugin directory
- connector packaged with incompatible dependency versions
- cluster nodes do not have consistent plugin layouts

## Operator Checklist

When a plugin cannot be loaded correctly, check these items in order:

1. Is the connector jar present in the binary package?
2. Is the dependency directory under `${SEATUNNEL_HOME}/plugins/` correct?
3. Does `plugin-mapping.properties` map the plugin to the expected directory?
4. Are all cluster nodes using the same plugin layout?
5. Is the plugin name in the job config exactly the same as the factory identifier?
6. Does the failure happen only in Flink or Spark, but not in Zeta?

## Developer Checklist

When adding a new plugin, verify these items before debugging engine internals:

- the factory is registered correctly
- the factory identifier matches the doc examples
- `OptionRule` covers required, optional, and exclusive fields
- the connector is added to packaging and plugin mapping
- isolated dependencies are placed under the expected plugin directory

## Code References

Useful code entry points:

- `seatunnel-api/src/main/java/org/apache/seatunnel/api/table/factory/FactoryUtil.java`
- `seatunnel-engine/seatunnel-engine-common/src/main/java/org/apache/seatunnel/engine/common/utils/FactoryUtil.java`
- `seatunnel-engine/seatunnel-engine-core/src/main/java/org/apache/seatunnel/engine/core/classloader/ClassLoaderService.java`
- `seatunnel-engine/seatunnel-engine-core/src/main/java/org/apache/seatunnel/engine/core/classloader/DefaultClassLoaderService.java`
- `seatunnel-engine/seatunnel-engine-server/src/main/java/org/apache/seatunnel/engine/server/rest/service/OptionRulesService.java`

## Recommended Reading Path

1. this page for the runtime map
2. [Connector Isolated Dependency Loading Mechanism](../connectors/connector-isolated-dependency.md)
3. [Configuration And Option System](./configuration-and-option-system.md)
4. [Source Connector Development](../developer/source-connector-development.md) or [Sink Connector Development](../developer/sink-connector-development.md)
