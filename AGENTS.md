# LLM Context Guide for Apache SeaTunnel

This guide helps AI assistants (LLMs / Agents) make **safe, consistent, and verifiable** changes to the Apache SeaTunnel codebase. It mirrors practices from mature Apache projects and adapts them to SeaTunnel’s **build, testing, architecture, and documentation conventions**.

## ⚠️ CRITICAL: Validate Before Proposing Changes

**Agents MUST run verification commands locally before suggesting or finalizing changes.**

```bash
# Format code (mandatory)
./mvnw spotless:apply

# Quick verification (mandatory)
./mvnw -q -DskipTests verify

# Unit tests (strongly recommended)
./mvnw test
```

Failure to meet these requirements will likely result in PR rejection.

## Git Commit Message Convention

SeaTunnel follows a **strict commit message format** to maintain a clean and searchable history.

**Format**:

```
[Type][Module] Description
```

### Types

* `Feature`  – New features
* `Fix`      – Bug fixes
* `Improve`  – Improvements to existing behavior
* `Docs`     – Documentation-only changes
* `Test`     – Test cases or test framework changes
* `Chore`    – Build, dependency, or maintenance tasks

### Modules

* `Connector-V2`  – seatunnel-connectors-v2
* `Zeta`          – seatunnel-engine (Zeta engine)
* `Core`          – seatunnel-core
* `API`           – seatunnel-api
* `Transform-V2`  – seatunnel-transforms-v2
* `Format`        – seatunnel-formats
* `Translation`   – seatunnel-translation
* `E2E`           – seatunnel-e2e

### Examples

* `[Fix][Connector-V2] Fix MySQL source split enumeration bug`
* `[Fix][Zeta] Fix checkpoint timeout under heavy backpressure`
* `[Feature][Transform-V2] Add LLM transform plugin`
* `[Improve][Core] Optimize jar package loading speed`
* `[Docs] Update quick start guide`

## Repository Structure

```text
seatunnel/
├── seatunnel-api/              # Core API definitions
├── seatunnel-connectors-v2/    # Source & Sink connectors (main contribution area)
├── seatunnel-transforms-v2/    # Transform plugins (including LLM)
├── seatunnel-engine/           # Zeta engine & Web UI
├── seatunnel-core/             # Job submission & CLI entry points
├── seatunnel-translation/      # Flink & Spark adapters
├── seatunnel-formats/          # Data formats (JSON, Avro, etc.)
├── seatunnel-e2e/              # End-to-End integration tests
├── docs/                       # Documentation (en & zh)
└── config/                     # Default configurations
```

## Code Standards

### Java Backend

* **Formatting**: Google Java Format (AOSP style), enforced by Spotless
* **Imports**:
    * No wildcard imports
    * Use shaded dependencies: `org.apache.seatunnel.shade.*`
* **Nullability**: Avoid implicit null assumptions
* **Visibility**: Keep APIs minimal; prefer package-private when possible
* **Comments**: Add comments for important methods (public APIs, complex logic). Important methods include public APIs, lifecycle hooks (initialization, start/stop, checkpoint), and complex or performance-critical logic. Example:

```java
/**
 * Enumerates source splits for parallel reading.
 * Called once during job initialization.
 *
 * @param context Split enumeration context
 * @return Collection of discovered splits
 */
@Override
public List<SourceSplit> enumerateSplits(SplitEnumerationContext context) {
    // Implementation
}
```

### Apache License Header (MANDATORY)

All **new files** MUST include the ASF license header:

```java
/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
```

## 🚨 Backward Compatibility (VERY IMPORTANT)

Agents MUST treat backward compatibility as a **hard constraint**.

* DO NOT remove or rename existing config options
* DO NOT change default values casually
* DO NOT break public APIs or SPI contracts

Any incompatible change MUST:

* Be explicitly documented
* Be documented in `docs/en/introduction/concepts/incompatible-changes.md`
* Include migration guidance
* Be clearly explained in the PR description

## Dependency Rules

* DO NOT introduce new dependencies unless absolutely necessary
* Prefer existing shaded dependencies under `org.apache.seatunnel.shade.*`
* Any new dependency MUST:
    * Be justified in the PR description
    * Consider shading, size, and conflict risks

## Architecture Guidelines

### Connector (V2)

* Implement `SeaTunnelSource` or `SeaTunnelSink`
* Define configs using `Option`
* Support parallelism via `SourceSplitEnumerator`
* Avoid connector-specific logic leaking into engine or core

### Zeta Engine

* **Client**: Submits job config
* **Master**: Schedules & coordinates
* **Worker**: Executes tasks (Source → Transform → Sink)

Respect task boundaries and lifecycle semantics.

## Configuration (Option) Rules

* All user-facing configs MUST be defined using `Option`
* Each option MUST include:
    * name
    * type
    * default value (if applicable)
    * clear description
* Option names are **stable contracts** and must not be renamed lightly

## Error Handling & Logging

* Exceptions MUST include sufficient context (table, task, config key)
* Avoid swallowing exceptions
* Use proper log levels:
    * INFO  – lifecycle events
    * WARN  – recoverable issues
    * ERROR – task-failing errors
* NEVER log sensitive information (passwords, tokens, credentials)

## Documentation Rules

* Any user-visible change MUST update:

    * `docs/en`
    * `docs/zh`
* Config names, defaults, and examples MUST match the code exactly
* Documentation is part of the feature, not an afterthought

## Testing Guidelines

### Unit Tests

* Located under `src/test/java`
* Validate behavior, not implementation details
* Prefer deterministic and minimal tests

Command:

```bash
./mvnw test
```

### E2E Tests

* Located in `seatunnel-e2e`
* Uses Testcontainers
* Extend `TestSuiteBase`

Command:

```bash
./mvnw -DskipUT -DskipIT=false verify
```

## Performance Awareness

Agents MUST consider performance implications:

* Avoid unnecessary object creation in hot paths
* Be cautious with large in-memory buffers
* Consider parallelism and resource usage

## PR Scope Rule

* Keep changes minimal and focused
* Avoid unrelated refactors or formatting-only changes
* One PR should solve **one problem**

## Running & Debugging

### Build from Source

```bash
./mvnw clean install -DskipTests -Dskip.spotless=true
```

### Install Connectors

```bash
sh bin/install-plugin.sh $current_version
```

### Run Job (Zeta)

```bash
sh bin/seatunnel.sh --config config/v2.batch.config.template -e local
```
