# StainTrace 染色数据追踪 - 快速开始指南

## 概述

StainTrace（染色数据追踪）功能可以追踪数据在SeaTunnel引擎中的流转过程，记录6个关键阶段的时间戳：

1. **S0** (SOURCE_EMIT): Source发出数据
2. **Q+** (QUEUE_IN): 进入队列
3. **Q-** (QUEUE_OUT): 离开队列
4. **T+** (TRANSFORM_IN): Transform接收
5. **T-** (TRANSFORM_OUT): Transform输出
6. **W!** (SINK_WRITE_DONE): Sink写入完成

6个追踪阶段：
| 阶段 | 代码 | 说明 | 记录位置 |
|------|------|------|---------|
| SOURCE_EMIT | S0 | Source发出数据 | SeaTunnelSourceCollector.collect() |
| QUEUE_IN | Q+ | 进入队列（入队前） | IntermediateQueue.received() |
| QUEUE_OUT | Q- | 离开队列（出队后） | IntermediateQueue.collect() |
| TRANSFORM_IN | T+ | Transform接收数据 | TransformFlowLifeCycle.received() |
| TRANSFORM_OUT | T- | Transform输出数据 | TransformFlowLifeCycle输出前 |
| SINK_WRITE_DONE | W! | Sink写入完成 | SinkFlowLifeCycle.writer.write()后 |

追踪数据会通过HTTP上报到trace-collector，自动解析并存储到MySQL数据库。

追踪数据会存储到MySQL数据库，方便后续分析端到端延迟和性能瓶颈。

---

## 一键配置与启动

### 前提条件

1. **启动MySQL服务**

```bash
# 方式1: Homebrew
brew services start mysql

# 方式2: MySQL Server
mysql.server start

# 方式3: 系统服务
sudo /usr/local/mysql/support-files/mysql.server start

# 验证MySQL运行
mysql -uroot -p'root@123' -e "SELECT 1"
```

### 步骤1: 一键配置

```bash
cd /Users/stone/work/myworkspace/seatunnel
./setup-stain-trace.sh
```

这个脚本会自动完成：
- ✅ 创建MySQL数据库 `seatunnel_trace`
- ✅ 创建3个表（st_trace_event_raw, st_trace, st_trace_entry）
- ✅ 配置trace-collector连接MySQL
- ✅ 配置seatunnel.yaml启用染色追踪
- ✅ 编译trace-collector
- ✅ 创建启动和查询脚本

### 步骤2: 启动trace-collector（新终端窗口）

```bash
./start-trace-collector.sh
```

看到以下日志表示启动成功：
```
StainTrace collector started on port 9808
Database initialized successfully
```

### 步骤3: 运行示例作业

#### 方式A: 在IDE中运行（推荐）

1. 打开 `SeaTunnelEngineLocalExample.java`
2. 点击运行（无需传递任何参数）
3. 观察控制台输出

#### 方式B: 使用Maven命令运行

```bash
./mvnw -pl seatunnel-examples/seatunnel-engine-examples \
  exec:java \
  -Dexec.mainClass=org.apache.seatunnel.example.engine.SeaTunnelEngineLocalExample \
  -nsu
```

### 步骤4: 查询追踪数据

```bash
./query-trace-data.sh
```

---

## 数据库结构

### 表说明

#### 1. st_trace_event_raw（原始事件表）
存储接收到的所有StainTraceEvent原始JSON数据。

| 字段 | 类型 | 说明 |
|------|------|------|
| id | BIGINT | 自增主键 |
| received_at | DATETIME(3) | 接收时间（毫秒精度）|
| job_id | VARCHAR(255) | 作业ID |
| event_type | VARCHAR(128) | 事件类型（STAIN_TRACE）|
| body_json | JSON | 完整事件JSON |

#### 2. st_trace（追踪摘要表）
存储每条追踪记录的摘要信息。

| 字段 | 类型 | 说明 |
|------|------|------|
| trace_id | BIGINT | 追踪ID（主键）|
| sink_task_id | BIGINT | Sink任务ID（主键）|
| job_id | VARCHAR(255) | 作业ID |
| table_id | VARCHAR(255) | 表标识 |
| created_time_ms | BIGINT | 创建时间戳（毫秒）|
| received_at | DATETIME(3) | 接收时间 |
| payload | LONGBLOB | 二进制payload（含所有阶段）|
| start_ts_ms | BIGINT | 起始时间戳 |
| entry_count | INT | 阶段条目数 |

#### 3. st_trace_entry（追踪条目表）
存储6个阶段的详细信息。

| 字段 | 类型 | 说明 |
|------|------|------|
| trace_id | BIGINT | 追踪ID |
| sink_task_id | BIGINT | Sink任务ID |
| entry_index | INT | 条目索引 |
| stage | SMALLINT | 阶段代码（1-6）|
| task_id | BIGINT | 任务ID |
| ts_ms | BIGINT | 时间戳（毫秒）|
| worker_address | VARCHAR(255) | Worker地址 |
| task_group_name | VARCHAR(255) | 任务组名称 |
| task_class | VARCHAR(255) | 任务类名 |

---

## 常用SQL查询

### 查询最近10条追踪记录

```sql
SELECT
    trace_id,
    job_id,
    table_id,
    received_at,
    entry_count
FROM st_trace
ORDER BY received_at DESC
LIMIT 10;
```

### 查看某条追踪的6个阶段详情

```sql
SELECT
    entry_index,
    CASE stage
        WHEN 1 THEN 'SOURCE_EMIT (S0)'
        WHEN 2 THEN 'QUEUE_IN (Q+)'
        WHEN 3 THEN 'QUEUE_OUT (Q-)'
        WHEN 4 THEN 'TRANSFORM_IN (T+)'
        WHEN 5 THEN 'TRANSFORM_OUT (T-)'
        WHEN 6 THEN 'SINK_WRITE_DONE (W!)'
    END AS stage_name,
    task_id,
    FROM_UNIXTIME(ts_ms / 1000.0) AS timestamp,
    ts_ms
FROM st_trace_entry
WHERE trace_id = <your_trace_id>
  AND sink_task_id = <your_sink_task_id>
ORDER BY entry_index;
```

### 端到端延迟分析

```sql
SELECT
    trace_id,
    job_id,
    -- 端到端延迟
    MAX(CASE WHEN stage = 6 THEN ts_ms END) -
    MAX(CASE WHEN stage = 1 THEN ts_ms END) AS e2e_latency_ms,
    -- Queue等待时间
    MAX(CASE WHEN stage = 3 THEN ts_ms END) -
    MAX(CASE WHEN stage = 2 THEN ts_ms END) AS queue_wait_ms,
    -- Transform处理时间
    MAX(CASE WHEN stage = 5 THEN ts_ms END) -
    MAX(CASE WHEN stage = 4 THEN ts_ms END) AS transform_ms
FROM st_trace t
JOIN st_trace_entry e ON t.trace_id = e.trace_id AND t.sink_task_id = e.sink_task_id
GROUP BY t.trace_id, t.job_id
ORDER BY e2e_latency_ms DESC
LIMIT 10;
```

---

## 配置说明

### seatunnel.yaml配置

```yaml
seatunnel:
  engine:
    # 启用染色追踪
    stain-trace-enabled: true

    # 采样率：每N条记录采样1条
    stain-trace-sample-rate: 5

    # 每Worker每秒最多产生多少条追踪Event
    stain-trace-max-traces-per-second-per-worker: 100

    # 每条追踪最多记录多少个阶段条目
    stain-trace-max-entries-per-trace: 32

    # Event上报配置
    event-report-http:
      url: "http://localhost:9808/ingest"
```

### trace-collector配置

```properties
# 服务端口
server.port=9808

# MySQL配置
db.type=mysql
db.jdbcUrl=jdbc:mysql://localhost:3306/seatunnel_trace
db.username=root
db.password=root@123

# 启用payload解析
trace.parsePayload=true
```

---

## 验证效果

运行作业后，检查以下内容：

### 1. trace-collector日志

```
Received 2 events
Ingested 2 traces with 12 entries
```

### 2. MySQL数据

```bash
./query-trace-data.sh
```

应该看到：
- ✅ st_trace表有数据
- ✅ st_trace_entry表有6个阶段的记录
- ✅ 时间戳递增（S0 < Q+ < Q- < T+ < T- < W!）

### 3. 示例作业特点

默认示例作业 `stain_trace_fake_sql_union_to_console.conf`：
- **FakeSource**: 生成10条数据
- **Sql Transform**: 使用LATERAL VIEW EXPLODE，1条输入→2条输出
- **Console Sink**: 输出20条数据
- **采样率**: sample-rate=1（全采样）
- **预期追踪数**: 10条（只有第一条split继承payload）

---

## 故障排查

### 问题1: trace-collector启动失败

**错误**: `Can't connect to MySQL`

**解决**:
```bash
# 检查MySQL是否运行
ps aux | grep mysqld

# 启动MySQL
brew services start mysql
```

### 问题2: 没有追踪数据

**检查步骤**:

1. 确认trace-collector正在运行
```bash
curl http://localhost:9808/health
```

2. 检查seatunnel.yaml配置是否正确
```bash
grep -A5 "event-report-http" seatunnel-examples/seatunnel-engine-examples/src/main/resources/examples/stain_trace_seatunnel.yaml
```

3. 查看trace-collector日志
```bash
# 应该看到接收Event的日志
```

### 问题3: 只有部分阶段数据

**原因**: Transform的1-to-N场景，只有第一条输出继承payload

**验证**: 检查`stain-trace-propagate-to-all-splits`配置
```yaml
stain-trace-propagate-to-all-splits: false  # 只第一条继承（默认）
stain-trace-propagate-to-all-splits: true   # 所有split都继承
```

---

## 进阶用法

### 自定义作业配置

创建自己的作业配置文件，参考：
```bash
seatunnel-examples/seatunnel-engine-examples/src/main/resources/examples/stain_trace_fake_sql_union_to_console.conf
```

运行时指定：
```java
public static void main(String[] args) {
    String configurePath = "/path/to/your/job.conf";
    // ... 其余代码
}
```

### 性能调优

生产环境建议配置：
```yaml
stain-trace-sample-rate: 100000  # 每10万条采样1条
stain-trace-max-traces-per-second-per-worker: 50  # 限流
```

---

## 相关文档

- 设计文档: [me/design/stain.md](me/design/stain.md)
- Review报告: [me/reviews/stain-review-final.md](me/reviews/stain-review-final.md)
- 实现代码: 搜索 `StainTrace` 相关类

---

**创建时间**: 2026-01-18
**作者**: AI Assistant
