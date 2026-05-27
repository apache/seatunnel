---
sidebar_position: 8
title: 运维操作
---

# Edge Agent 运维

Edge Agent 日常运维：启停脚本、日志、健康检查与常见问题。

## 生命周期命令

在 EDGE_AGENT_HOME 下执行：

```bash
# 后台启动
sh bin/seatunnel-edge-agent.sh start

# 停止
sh bin/seatunnel-edge-agent.sh stop

# 查看进程状态
sh bin/seatunnel-edge-agent.sh status

# SQLite WAL / 位点运维
sh bin/seatunnel-edge-agent.sh db [子命令]

# 帮助
sh bin/seatunnel-edge-agent.sh help
```

:::note

Windows 使用 bin\seatunnel-edge-agent.cmd，子命令相同。

:::

## SQLite 运维

无需手写 SQL，通过 agent JAR 与 JDBC 访问 queue.sqlite-path（不依赖系统 sqlite3）。

```bash
# 只读（agent 运行中也可）
sh bin/seatunnel-edge-agent.sh db info
sh bin/seatunnel-edge-agent.sh db wal-summary
sh bin/seatunnel-edge-agent.sh db wal-list --status DEAD --limit 20
sh bin/seatunnel-edge-agent.sh db wal-show --id 42

# 文件 tail 位点
sh bin/seatunnel-edge-agent.sh db positions

# 写入：先 stop，再用 --yes（或 --dry-run 预览）
sh bin/seatunnel-edge-agent.sh stop
sh bin/seatunnel-edge-agent.sh db wal-purge-dead --dry-run
sh bin/seatunnel-edge-agent.sh db wal-purge-dead --yes
sh bin/seatunnel-edge-agent.sh db wal-retry-dead --yes    # BEST_EFFORT 下可能重复投递
sh bin/seatunnel-edge-agent.sh db wal-unstick-sending --yes
sh bin/seatunnel-edge-agent.sh db wal-purge-acked --older-than-ms 86400000 --yes
```

| 子命令 | 读/写 | 说明 |
|--------|------|------|
| info | 读 | 库路径、-wal/-shm 大小、进程是否在跑 |
| wal-summary | 读 | 各状态行数与最老 updated_at |
| wal-list | 读 | 列出 WAL 行；第一列 id(pk) 为行主键，供 wal-show 使用 |
| wal-show | 读 | 单行详情；需先用 wal-list 取得 --id（不是 source_id 或 batch_id） |
| positions | 读 | 文件 tail 位点（可通过 --source-id 指定来源） |
| wal-purge-dead | 写 | 删除 DEAD 行 |
| wal-retry-dead | 写 | DEAD → PENDING（可能重复投递） |
| wal-unstick-sending | 写 | SENDING → PENDING |
| wal-purge-acked | 写 | 删除过期 ACKED（--older-than-ms） |

完整参数：sh bin/seatunnel-edge-agent.sh db help。

:::caution db 命令的 SQLite 路径

db 使用的文件路径按优先级：--sqlite-path → EDGE_AGENT_SQLITE_PATH → 安装根/data/wal.db（默认；-wal/-shm 同在 data/ 下）。

db 不读取 agent.yaml 中的 queue.sqlite-path。若运行时使用了自定义路径，请在 db 命令上显式传入相同的 --sqlite-path。

:::

## 环境变量

| 变量 | 默认值 | 说明 |
|------|--------|------|
| EDGE_AGENT_CONFIG | $EDGE_AGENT_HOME/config/agent.yaml | 配置文件 |
| EDGE_AGENT_SQLITE_PATH | （未设置时用 安装根/data/wal.db） | db 命令 SQLite 路径（低于 --sqlite-path） |
| EDGE_AGENT_PID_FILE | $EDGE_AGENT_HOME/edge-agent.pid | PID 文件 |
| EDGE_AGENT_ID_FILE | $EDGE_AGENT_HOME/edge-agent.id | 安装根身份文件路径 |
| EDGE_AGENT_LOG_FILE | $EDGE_AGENT_HOME/edge-agent.out | 脚本日志 |
| EDGE_AGENT_LOG_CONFIG | $EDGE_AGENT_HOME/config/log4j2.properties | Log4j2 配置 |
| EDGE_AGENT_LOG_DIR | $EDGE_AGENT_HOME/log | 应用日志目录 |
| EDGE_AGENT_APP_LOG_NAME | edge-agent.log | 应用日志文件名 |
| EDGE_AGENT_STARTUP_READY_TIMEOUT_S | 10 | 等待 BOOTSTRAP_READY 秒数 |

## 日志

| 文件 | 内容 |
|------|------|
| log/edge-agent.log | 主应用日志（Log4j2 滚动） |
| edge-agent.out | 脚本启停输出 |

:::note 关键日志关键字

- BOOTSTRAP_READY — 启动成功（启动脚本也依赖此标记）。
- BOOTSTRAP_FAILED — 致命启动错误。
- Shutdown signal received — 优雅关闭 hook。

配置路径仅在 DEBUG 级别打印。

:::

## 监控建议

- 进程：status 子命令或 `kill -0 <pid>`。
- 功能：Engine EdgeSocket Source 对批次返回 RECEIVED，并可观测到持续摄入。
- WAL：监控 queue.sqlite-path 库文件及同目录 -wal/-shm 占用磁盘空间。
- 背压：采集端可能收到 QUEUE_FULL 或 RETRY 响应，详见 [EdgeSocket Source](../connectors/source/EdgeSocket.md)。

## 常见问题

### AUTH_FAILED

Token 认证失败。常见原因：

- output.token 与 Engine EdgeSocket token 不一致。

对齐 token 后重启 Agent；不要指望 WAL 重试能修复错误 token 配置。

### REJECTED

采集端被 Source 策略拒绝。常见原因：

- 同一监听策略下存在重复 Agent 实例（身份冲突）。

确保同一监听策略下只有一个有效采集端；REJECTED 后勿依赖自动重连。

### 超时未见 BOOTSTRAP_READY

- 查看 log/edge-agent.log 中的堆栈跟踪（配置校验、SQLite 路径、paths 为空等）。
- 磁盘较慢时可调大 EDGE_AGENT_STARTUP_READY_TIMEOUT_S。
- 确认 starter/seatunnel-edge-agent-starter.jar 存在。

### 位点丢失

删除 edge-agent.id 且 YAML 未配置 input.id 时，位点不再匹配（按新源处理）。迁移或升级时请保留 edge-agent.id 与 SQLite 库文件（queue.sqlite-path，默认 data/wal.db 及 data/ 下 wal.db-wal、wal.db-shm），或在 YAML 中写死各 id。见 [常见问题 — 身份与恢复](faq.md#身份与恢复)。

### WAL 行处于 DEAD 或 SENDING

使用上文 db wal-list、db wal-show 与写子命令。何时 purge / retry 及重复投递风险见 [常见问题 — DEAD 说明](faq.md#wal-状态-dead-是什么意思)。

## systemd 示例

```ini
[Unit]
Description=SeaTunnel Edge Agent
After=network.target

[Service]
Type=forking
Environment=EDGE_AGENT_HOME=/opt/apache-seatunnel-edge-agent
Environment=JAVA_HOME=/usr/lib/jvm/java-11
ExecStart=/bin/sh ${EDGE_AGENT_HOME}/bin/seatunnel-edge-agent.sh start
ExecStop=/bin/sh ${EDGE_AGENT_HOME}/bin/seatunnel-edge-agent.sh stop
PIDFile=${EDGE_AGENT_HOME}/edge-agent.pid
Restart=on-failure

[Install]
WantedBy=multi-user.target
```

请按发行版调整路径与用户。部分发行版可能需要将 Type=forking 改为 Type=simple 并使用前台模式运行 Java。

