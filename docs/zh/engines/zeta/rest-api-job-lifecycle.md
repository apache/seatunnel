---
sidebar_position: 5
---

# REST API 作业生命周期手册

## 概述

本手册是 [REST API v2 参考文档](rest-api-v2.md) 的实践补充，提供管理完整作业生命周期的操作食谱：
提交、监控、停止、取消、保存点以及恢复。同时涵盖认证、性能注意事项和常见错误。

---

## 1. 前置条件

在 `seatunnel.yaml` 中启用 REST API：

```yaml
seatunnel:
  engine:
    http:
      enable-http: true
      port: 8080
      enable-dynamic-port: true
      port-range: 100
```

以下所有示例均使用 `http://<master>:8080`，请替换为实际的 Master 节点地址和端口。

---

## 2. 作业提交

### 2.1 通过 JSON 提交作业

```bash
curl -X POST http://<master>:8080/hazelcast/rest/maps/submit-job \
  -H "Content-Type: application/json" \
  -d @job.json
```

最小 `job.json` 结构示例：

```json
{
  "env": {
    "job.name": "my-cdc-job",
    "job.mode": "STREAMING",
    "checkpoint.interval": 30000
  },
  "source": [
    {
      "plugin_name": "MySQL-CDC",
      "result_table_name": "mysql_cdc_result",
      "base-url": "jdbc:mysql://localhost:3306/mydb",
      "username": "cdc_user",
      "password": "password",
      "database-names": ["mydb"],
      "table-names": ["mydb.orders"],
      "startup.mode": "initial",
      "server-id": "5400-5404"
    }
  ],
  "transform": [],
  "sink": [
    {
      "plugin_name": "Console",
      "source_table_name": ["mysql_cdc_result"]
    }
  ]
}
```

### 2.2 提交含多个 Transform 的作业（JSON 格式）

```json
{
  "env": {
    "job.name": "etl-with-transforms",
    "job.mode": "BATCH"
  },
  "source": [
    {
      "plugin_name": "FakeSource",
      "result_table_name": "fake",
      "row.num": 100,
      "schema": {
        "fields": {
          "id": "int",
          "name": "string",
          "amount": "double"
        }
      }
    }
  ],
  "transform": [
    {
      "plugin_name": "FieldMapper",
      "source_table_name": ["fake"],
      "result_table_name": "after_field_map",
      "field_mapper": {
        "id": "user_id",
        "name": "user_name"
      }
    },
    {
      "plugin_name": "Filter",
      "source_table_name": ["after_field_map"],
      "result_table_name": "filtered",
      "fields": ["user_id", "user_name", "amount"]
    }
  ],
  "sink": [
    {
      "plugin_name": "Console",
      "source_table_name": ["filtered"]
    }
  ]
}
```

### 2.3 提交响应

提交成功返回：

```json
{
  "jobId": "733584788375093248",
  "jobName": "my-cdc-job"
}
```

请保存 `jobId`，后续所有生命周期操作都需要它。

---

## 3. 作业状态查询

### 3.1 查询单个运行中作业

```bash
curl http://<master>:8080/hazelcast/rest/maps/running-job/<jobId>
```

响应字段说明：

| 字段 | 描述 |
|---|---|
| `jobId` | 作业唯一标识 |
| `jobName` | 作业名称 |
| `jobStatus` | `RUNNING`、`FINISHED`、`FAILED`、`CANCELLED` |
| `envOptions` | 生效的 env 配置 |
| `createTime` | 作业创建时间戳 |
| `jobDag` | DAG 拓扑结构 |
| `metrics` | Source / Sink 吞吐量计数器 |

### 3.2 查询所有运行中作业

```bash
curl http://<master>:8080/hazelcast/rest/maps/running-jobs
```

### 3.3 查询已完成的作业

```bash
curl http://<master>:8080/hazelcast/rest/maps/finished-job/<jobId>
```

### 3.4 仅查询作业指标

```bash
curl http://<master>:8080/hazelcast/rest/maps/running-job-metrics/<jobId>
```

关键指标字段：

| 指标 | 含义 |
|---|---|
| `SourceReceivedCount` | 从 Source 读取的总行数 |
| `SinkWriteCount` | 写入 Sink 的总行数 |
| `SourceReceivedQPS` | 当前读取吞吐量（行/秒）|
| `SinkWriteQPS` | 当前写入吞吐量（行/秒）|

---

## 4. 查询作业日志

```bash
# 获取运行中作业的最后 N 行日志
curl "http://<master>:8080/hazelcast/rest/maps/running-job-logs/<jobId>?size=100"
```

对于日志文件分散在各 Worker 节点的大规模部署，建议直接访问 Worker 节点的 REST 端口，
或配置集中式日志系统（参见 [日志配置](logging.md)）。

---

## 5. 停止、取消与保存点语义

### 操作语义对比

| 操作 | 行为 | 状态保留 | 是否可恢复 |
|---|---|---|---|
| `stop`（优雅停止）| 等待在途数据刷写完毕 | 停止时刻的 Checkpoint | 是，通过 `--restore` |
| `stop-with-savepoint` | 优雅停止 + 写入显式 Savepoint | 完整 Savepoint | 是，通过 `--restore` |
| `cancel`（强制终止）| 立即终止 | 不写入新状态 | 仅从上次 Checkpoint 恢复 |

### 5.1 优雅停止（不创建 Savepoint）

```bash
curl -X POST "http://<master>:8080/hazelcast/rest/maps/stop-job" \
  -H "Content-Type: application/json" \
  -d '{"jobId": "733584788375093248", "isStopWithSavePoint": false}'
```

### 5.2 停止并创建 Savepoint

```bash
curl -X POST "http://<master>:8080/hazelcast/rest/maps/stop-job" \
  -H "Content-Type: application/json" \
  -d '{"jobId": "733584788375093248", "isStopWithSavePoint": true}'
```

Savepoint 路径会打印在作业日志中，也可通过查询已完成作业获取：

```bash
curl http://<master>:8080/hazelcast/rest/maps/finished-job/733584788375093248 | \
  python3 -c "import sys,json; d=json.load(sys.stdin); print(d.get('savepointPath', 'N/A'))"
```

### 5.3 强制取消

```bash
curl -X POST "http://<master>:8080/hazelcast/rest/maps/cancel-job" \
  -H "Content-Type: application/json" \
  -d '{"jobId": "733584788375093248"}'
```

---

## 6. 作业恢复与重启

### 6.1 从最新 Checkpoint 恢复

重新提交作业时携带相同的 `job.id`，引擎会自动从该作业的 Checkpoint 目录恢复状态：

```bash
curl -X POST http://<master>:8080/hazelcast/rest/maps/submit-job \
  -H "Content-Type: application/json" \
  -d '{
    "env": {
      "job.id": "733584788375093248",
      "job.name": "my-cdc-job",
      "job.mode": "STREAMING",
      "checkpoint.interval": 30000
    },
    "source": [ ... ],
    "sink": [ ... ]
  }'
```

### 6.2 从指定 Savepoint 恢复

```bash
curl -X POST http://<master>:8080/hazelcast/rest/maps/submit-job \
  -H "Content-Type: application/json" \
  -d '{
    "env": {
      "job.name": "my-cdc-job-restored",
      "job.mode": "STREAMING",
      "checkpoint.interval": 30000,
      "restore.mode": "savepoint",
      "savepoint.path": "/seatunnel/checkpoint/savepoint/733584788375093248/1748595600000"
    },
    "source": [ ... ],
    "sink": [ ... ]
  }'
```

---

## 7. 认证与授权

开启安全配置后（参见 [安全配置](security.md)），所有 REST API 请求必须携带 Bearer Token
或 Basic Auth 凭据。

### Basic Auth 示例

```bash
curl -u admin:password http://<master>:8080/hazelcast/rest/maps/running-jobs
```

### Bearer Token 示例

```bash
TOKEN=$(curl -s -X POST http://<master>:8080/hazelcast/rest/maps/login \
  -H "Content-Type: application/json" \
  -d '{"username":"admin","password":"password"}' | \
  python3 -c "import sys,json; print(json.load(sys.stdin)['token'])")

curl -H "Authorization: Bearer $TOKEN" \
  http://<master>:8080/hazelcast/rest/maps/running-jobs
```

---

## 8. REST API 性能注意事项

### 已完成作业过多导致 job-info 查询变慢

当 `finished-job-state` IMap 条目增多（数千条）时，`/running-jobs` 和 `/finished-job` 端点
可能变慢，因为它们需要全量扫描所有条目。

**缓解措施：**

1. 缩短 `history-job-expire-minutes`，减少保留窗口
2. 避免高频轮询 finished-job 端点；在监控层缓存结果
3. 在看板中直接按 `jobId` 查询，而非列出全部作业

### 并发提交速率

REST API 在 Hazelcast 执行器池中同步处理提交请求。对于批量提交场景（一次导入数百个作业），
建议将提交频率限制在每秒 10–20 个，避免 Master 节点过载。

### 动态端口分配

若 `enable-dynamic-port: true`，不同 Master 节点可能使用不同端口。可通过 Hazelcast 集群
发现接口或 Management Center 找到活跃 Master 端口：

```bash
# 从任意集群成员发现 Master 节点信息
curl http://<any-node>:8080/hazelcast/rest/cluster | \
  python3 -c "import sys,json; print(json.load(sys.stdin))"
```

---

## 9. 常见错误与故障排查

| 错误 | 原因 | 修复方法 |
|---|---|---|
| 任意端点返回 `HTTP 404` | REST API 未启用或端口不对 | 设置 `enable-http: true` 并检查端口 |
| `Connection refused` | Master 未启动或防火墙拦截 | 确认 Master 进程在运行；检查防火墙 |
| running-job 中找不到 `jobId` | 作业已完成或未启动 | 改用 `finished-job/<jobId>` 查询 |
| 提交返回 `400 Bad Request` | JSON 格式错误或缺少必填字段 | 验证 JSON；检查 `plugin_name` 拼写 |
| `Job already exists with same job.id` | 相同 `job.id` 重复提交但未先停止 | 先取消或停止现有作业，再重新提交 |
| `Unauthorized 401` | 已启用安全配置但未提供凭据 | 添加 `-u user:pass` 或 `Authorization` 请求头 |
| `Savepoint path not found` | Savepoint 已删除或路径有误 | 检查 Checkpoint 存储，提供正确路径 |

---

## 参考

- [REST API v2 参考文档](rest-api-v2.md)
- [REST API v1 参考文档](rest-api-v1.md)
- [安全配置](security.md)
- [状态存储与恢复](state-storage-and-recovery.md)
- [CDC 生产实战手册](../../connectors/cdc-production-cookbook.md)
