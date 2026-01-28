# LLM Context Guide for Apache SeaTunnel

This guide helps AI assistants (LLMs/Agents) make safe, consistent, and verifiable changes to the SeaTunnel codebase. It mirrors practices from mature Apache projects and adapts them to SeaTunnel’s build, testing, and documentation conventions.

⚠️ **CRITICAL: Validate Before Pushing**
ALWAYS run verification commands before proposing changes.
- **Format Code**: `./mvnw spotless:apply`
- **Quick Verify**: `./mvnw -q -DskipTests verify`
- **Unit Tests**: `./mvnw test`

## Git Commit Message Convention
SeaTunnel follows a strict commit message format to maintain a clean history.
**Format**: `[Type][Module] Description`

**Types**:
- `Feature`: New features
- `Fix`: Bug fixes
- `Improve`: Improvements to existing features
- `Docs`: Documentation changes
- `Test`: Test cases or test framework changes
- `Chore`: Build process, dependency updates, or maintenance

**Modules**:
- `Connector-V2`: Changes in `seatunnel-connectors-v2`
- `Zeta`: Changes in `seatunnel-engine` (Zeta engine)
- `Core`: Changes in `seatunnel-core`
- `API`: Changes in `seatunnel-api`
- `E2E`: Changes in `seatunnel-e2e`
- `Transform-V2`: Changes in `seatunnel-transforms-v2`
- `Format`: Changes in `seatunnel-formats`
- `Translation`: Changes in `seatunnel-translation`

**Examples**:
- `[Fix][Connector-V2] Fix MySQL connector source split bug`
- `[Fix][Zeta] Fix checkpoint timeout issue`
- `[Feature][Transform-V2] Add LLM transform plugin`
- `[Improve][Core] Optimize jar package loading speed`
- `[Docs] Update quick start guide`

## Key Directories
```text
seatunnel/
├── seatunnel-api/              # Core API definitions
├── seatunnel-connectors-v2/    # Source & Sink connectors (Main contribution area)
├── seatunnel-transforms-v2/    # Transform plugins (including LLM)
├── seatunnel-engine/           # SeaTunnel Zeta Engine & Web UI
├── seatunnel-core/             # Job submission & CLI entry points
├── seatunnel-translation/      # Adapters for Flink & Spark
├── seatunnel-formats/          # Data format handling (JSON, Avro, etc.)
├── seatunnel-e2e/              # End-to-End integration tests
├── docs/                       # Documentation (en & zh)
└── config/                     # Default configurations
```

## Code Standards
**Java Backend**
- **Style**: Google Java Format (AOSP style). Enforced by Spotless.
- **Imports**: No wildcard imports. `org.apache.seatunnel.shade.*` must be used for shaded dependencies (Guava, Jetty, Hikari, Janino, Commons-Lang3).
- **License Header**: All new files must include the standard Apache Software Foundation license header.

**Apache License Headers**
- **Requirement**: New files require ASF license headers.
- **Header Content**:
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

**Documentation**
- **Bilingual**: User-visible changes MUST update both `docs/en` and `docs/zh`.
- **Consistency**: Config options in docs must match the code implementation.

## Architecture Patterns
**Connectors (V2)**
- Implement `SeaTunnelSource` or `SeaTunnelSink`.
- Use `Option` rule for configuration definition.
- Support `SourceSplitEnumerator` for parallel reading.

**Engine (Zeta)**
- **Client**: Submits job config to Master.
- **Master**: Schedules tasks to Workers.
- **Worker**: Executes tasks (Source -> Transform -> Sink).

## Test Utilities
**Unit Tests**
- Run with `./mvnw test`.
- Located in `src/test/java` of each module.

**E2E Tests (`seatunnel-e2e`)**
- Uses Testcontainers to spin up docker environments.
- Define test cases extending `TestSuiteBase`.
- **Command**: `./mvnw -DskipUT -DskipIT=false verify` (Runs ITs, can be slow).

## Running & Debugging
**Build from Source**
```bash
./mvnw clean install -DskipTests -Dskip.spotless=true
```

**Install Connectors**
```bash
sh bin/install-plugin.sh 2.3.13  # Or specific version
```

**Run Job (Zeta)**
```bash
sh bin/seatunnel.sh --config config/v2.batch.config.template -e local
```
