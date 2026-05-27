---
sidebar_position: 9
title: 常见问题
---

# 常见问题

身份、迁移、WAL DEAD 行、同一主机多实例等高频问答。命令与故障树见[运维](operations.md)。全量参数表见[配置参数说明](configuration.md)。

## 身份与恢复

### edge-agent.id 是什么？

安装根目录下的身份文件（与 edge-agent.pid 同级）。YAML 中省略 agent.id、input.id、output.id 时，Agent 在此读取或写入；YAML 显式 id 优先。详见 [身份文件](configuration.md#身份文件edge-agentid)。

### agent.id、input.id、output.id 各做什么？

| 键 / YAML | 作用 |
|-----------|------|
| agent.id | Agent 实例标识（日志、同一主机多实例区分） |
| input.id | 采集源标识；作为 WAL / 位点的 sourceId，重启后续读 |
| output.id | 出站逻辑标识（迁移与日志；当前不写入 EdgeSocket 线协议） |

### 故障重启后，靠哪个 ID 续读文件位点？

input.id。位点存在 SQLite 的 edge_agent_source_position 表中，按 source_id（= input.id）和文件路径查找。agent.id、output.id 不参与位点查询。

### 迁移或升级时要保留什么？

1. edge-agent.id（保住逻辑 ID，尤其是 input.id）
2. queue.sqlite-path 指向的 SQLite 库文件，以及同目录下的 -wal、-shm 伴生文件（不要只拷主库文件）

迁移或升级时必须保留：config/agent.yaml（若 ID 已在身份文件中，可不在 YAML 里重复写）。

### 删除 edge-agent.id 会怎样？

若 YAML 也未配置 input.id，会生成新的 input.id，已有位点不再匹配，Agent 按新源处理。

### 同一主机跑多个 Agent 要注意什么？

每个实例使用独立安装根（或独立的 edge-agent.id 与 queue.sqlite-path），不要共用同一库文件。

### EDGE_AGENT_ID_FILE 是什么？

启动脚本环境变量，默认 $EDGE_AGENT_HOME/edge-agent.id。见 [运维 — 环境变量](operations.md#环境变量)。

## SQLite 持久化

默认路径、磁盘文件（data、-wal、-shm）及表内数据说明见[配置说明 — SQLite 持久化文件](configuration.md#sqlite-持久化文件)。console 模式仍会使用该库。

### WAL 状态 DEAD 是什么意思？

该行已超过 retry.max-attempts（认领时递增的 attempt_count），调度器不再发送。排查示例：

```bash
sh bin/seatunnel-edge-agent.sh db wal-list --status DEAD
sh bin/seatunnel-edge-agent.sh db wal-show --id <行-id>
```

根因修复后先 stop，再清理或重试：

```bash
sh bin/seatunnel-edge-agent.sh stop
sh bin/seatunnel-edge-agent.sh db wal-purge-dead --yes
# 或：db wal-retry-dead --yes（可能重复投递；BEST_EFFORT 下 WAL 会重试）
```

本版本不会自动清理 DEAD 行。详见 [运维](operations.md) 中的 SQLite db 命令章节。

### Agent 如何选取 EdgeSocket 的 batchId？

每条出站 WAL 行带有 batch_id，由 edge_agent_meta 表中的 next_batch_id 分配。调度器发送 `__BATCH__:<batch_id>:...`。同一 WAL 行重试时复用同一 batch_id。

WAL 行 id（自增）仅用于本地 ack/清理，不是线上的 batchId。

请在 Agent 重启时保留 queue.sqlite-path 对应的数据库文件（及 -wal、-shm），以便 next_batch_id 持续递增。若删除或重建该库，计数会从 1 重来，可能与 [EdgeSocket Source](../connectors/source/EdgeSocket.md) 在引擎 checkpoint 后对 全局单调 batchId 的约定冲突。

本版本 不发送 `__COMMIT__`，不会根据引擎 checkpoint 水位选择下一个 batchId。

### Token 配错或收到 AUTH_FAILED？

鉴权失败为 致命错误（记录日志后进程退出）。请核对 output.token 与 Engine 侧 EdgeSocket token 一致后重启；不要指望 WAL 重试能自动修复错误 token。（secret_key 用于 PACKET 加密，不用于 token 鉴权对齐。）

