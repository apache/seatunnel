# SeaTunnel Edge Agent

Lightweight edge-side collector: reads local sources, buffers outbound data in SQLite WAL, and forwards batches to SeaTunnel Engine via the EdgeSocket line protocol. **User guides and `agent.yaml` reference live in the docs site** — avoid duplicating them here.

## Modules

| Path | Role |
|------|------|
| `seatunnel-edge-agent-connector/` | `EdgeInputReader` SPI (`file` collector) |
| `seatunnel-edge-agent-transport/` | `EdgeCollectorTransport` SPI (EdgeSocket / console), payload encoding |
| `seatunnel-edge-agent-starter/` | Config, runtime, scheduler, SQLite WAL and source positions |
| `bin/` | `seatunnel-edge-agent.sh` / `.cmd` — `start` \| `stop` \| `status` \| `db` |
| `config/` | Sample [`agent.yaml`](config/agent.yaml), `log4j2.properties` |

E2E: `seatunnel-e2e/seatunnel-edge-agent-e2e/`. Distribution: `seatunnel-dist` (`assembly-edge-agent-*.xml`).

## Documentation (canonical)

| Topic | English | 中文 |
|-------|---------|------|
| Overview & reading path | [About Edge Agent](../docs/en/edge-agent/about.md) | [简介](../docs/zh/edge-agent/about.md) |
| Quick start | [Quick Start](../docs/en/edge-agent/quick-start.md) | [快速开始](../docs/zh/edge-agent/quick-start.md) |
| Full `agent.yaml` parameters | [Configuration](../docs/en/edge-agent/configuration.md) | [配置说明](../docs/zh/edge-agent/configuration.md) |
| Input / output scenarios | [Input](../docs/en/edge-agent/input-configuration.md) · [Output](../docs/en/edge-agent/output-configuration.md) | [输入](../docs/zh/edge-agent/input-configuration.md) · [输出](../docs/zh/edge-agent/output-configuration.md) |
| Deploy & operate | [Deployment Guide](../docs/en/edge-agent/deployment-guide.md) · [Operations](../docs/en/edge-agent/operations.md) · [FAQ](../docs/en/edge-agent/faq.md) | [部署指南](../docs/zh/edge-agent/deployment-guide.md) · [运维](../docs/zh/edge-agent/operations.md) · [FAQ](../docs/zh/edge-agent/faq.md) |
| System design | [Edge Agent Architecture](../docs/en/edge-agent/architecture-overview.md) | [架构](../docs/zh/edge-agent/architecture-overview.md) |
| Wire protocol (engine) | [EdgeSocket Source](../docs/en/connectors/source/EdgeSocket.md) | [EdgeSocket](../docs/zh/connectors/source/EdgeSocket.md) |

## Build (developers)

```bash
./mvnw -pl seatunnel-edge-agent/seatunnel-edge-agent-starter -DskipTests package
./mvnw -pl seatunnel-dist -DskipTests package   # edge-agent tarball
```
