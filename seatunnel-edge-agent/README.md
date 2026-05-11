<!--
Licensed to the Apache Software Foundation (ASF) under one or more
contributor license agreements.  See the NOTICE file distributed with
this work for additional information regarding copyright ownership.
The ASF licenses this file to You under the Apache License, Version 2.0
(the "License"); you may not use this file except in compliance with
the License.  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
-->

# SeaTunnel Edge Agent

SeaTunnel Edge Agent is a lightweight edge-side collector for SeaTunnel Engine scenarios where source data is only reachable from local hosts (for example local files, private logs, and host-local event feeds).  
It reads records near the source, persists outbound data in SQLite WAL, and forwards batches to running SeaTunnel jobs through EdgeSocket.

The overall design follows these principles:

- Lightweight runtime, independent process deployment on edge hosts;
- Durable outbound buffering with SQLite WAL state transitions;
- Protocol compatibility with SeaTunnel Engine EdgeSocket source;
- Simple operations through YAML config and start/stop/status scripts;
- Clear module boundaries for later extension (parallel senders and stronger guarantees).

### Core Functions

- Support built-in `file`, `log`, and `event` inputs;
- Support endpoint discovery by job id (`SeaTunnelClient#getJobTaskGroupAddresses`);
- Support auth/batch/commit polling with retry and reconnect;
- Support crash recovery by restoring stale `SENDING` rows to `PENDING`;
- Support packaging into standard SeaTunnel distribution assemblies.

### Module Layout

- `seatunnel-edge-agent-core`: bootstrap lifecycle, YAML load/validate, batch/WAL loop;
- `seatunnel-edge-agent-transport`: EdgeSocket protocol and discovery client;
- `seatunnel-edge-agent-connector`: local input implementations and NDJSON normalization;
- `bin/`: launcher scripts (`seatunnel-edge-agent.sh`, `seatunnel-edge-agent.cmd`);
- `conf/`: default runtime config (`agent.yaml`).

### Runtime Layout

The launcher expects an install root that contains:

- `bin/`
- `conf/`
- `lib/`

Default command examples:

- Unix: `sh bin/seatunnel-edge-agent.sh start`
- Windows: `bin\seatunnel-edge-agent.cmd start`

### Packaging

Edge Agent artifacts are packaged through SeaTunnel standard assemblies in `seatunnel-dist`:

- `assembly-bin.xml`
- `assembly-bin-ci.xml`
- `assembly-src.xml`

### Documentation

- [Edge Agent Architecture (EN)](../docs/en/architecture/edge-agent-architecture.md)
- [Edge Agent 架构 (ZH)](../docs/zh/architecture/edge-agent-architecture.md)
