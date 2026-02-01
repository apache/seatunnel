--
-- Licensed to the Apache Software Foundation (ASF) under one or more
-- contributor license agreements.  See the NOTICE file distributed with
-- this work for additional information regarding copyright ownership.
-- The ASF licenses this file to You under the Apache License, Version 2.0
-- (the "License"); you may not use this file except in compliance with
-- the License.  You may obtain a copy of the License at
--
--    http://www.apache.org/licenses/LICENSE-2.0
--
-- Unless required by applicable law or agreed to in writing, software
-- distributed under the License is distributed on an "AS IS" BASIS,
-- WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
-- See the License for the specific language governing permissions and
-- limitations under the License.
--

-- StainTrace MySQL初始化脚本
-- 使用方法: mysql -uroot -p'root@123' < init-mysql.sql

-- 创建数据库
CREATE DATABASE IF NOT EXISTS seatunnel_trace DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;

USE seatunnel_trace;

-- 原始事件表
CREATE TABLE IF NOT EXISTS st_trace_event_raw (
    id BIGINT NOT NULL AUTO_INCREMENT,
    received_at DATETIME(3) NOT NULL,
    job_id VARCHAR(255) NULL,
    event_type VARCHAR(128) NULL,
    body_json JSON NOT NULL,
    PRIMARY KEY(id),
    KEY idx_st_trace_event_job_received(job_id, received_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='原始StainTrace事件';

-- 追踪摘要表
CREATE TABLE IF NOT EXISTS st_trace (
    trace_id BIGINT NOT NULL,
    sink_task_id BIGINT NOT NULL,
    job_id VARCHAR(255) NULL,
    table_id VARCHAR(255) NULL,
    created_time_ms BIGINT NULL,
    received_at DATETIME(3) NOT NULL,
    payload LONGBLOB NULL,
    start_ts_ms BIGINT NULL,
    entry_count INT NULL,
    PRIMARY KEY(trace_id, sink_task_id),
    KEY idx_st_trace_job_received(job_id, received_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='追踪摘要';

-- 追踪条目表（6个阶段详细信息）
CREATE TABLE IF NOT EXISTS st_trace_entry (
    trace_id BIGINT NOT NULL,
    sink_task_id BIGINT NOT NULL,
    entry_index INT NOT NULL,
    stage SMALLINT NOT NULL COMMENT '1=SOURCE_EMIT, 2=QUEUE_IN, 3=QUEUE_OUT, 4=TRANSFORM_IN, 5=TRANSFORM_OUT, 6=SINK_WRITE_DONE',
    task_id BIGINT NOT NULL,
    ts_ms BIGINT NOT NULL,
    worker_address VARCHAR(255) NULL,
    task_group_name VARCHAR(255) NULL,
    task_class VARCHAR(255) NULL,
    PRIMARY KEY(trace_id, sink_task_id, entry_index),
    KEY idx_stage_ts(stage, ts_ms)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='追踪条目详情';

-- 创建视图：方便查询完整追踪链路
CREATE OR REPLACE VIEW v_trace_detail AS
SELECT
    t.trace_id,
    t.sink_task_id,
    t.job_id,
    t.table_id,
    t.received_at,
    t.entry_count,
    e.entry_index,
    CASE e.stage
        WHEN 1 THEN 'SOURCE_EMIT'
        WHEN 2 THEN 'QUEUE_IN'
        WHEN 3 THEN 'QUEUE_OUT'
        WHEN 4 THEN 'TRANSFORM_IN'
        WHEN 5 THEN 'TRANSFORM_OUT'
        WHEN 6 THEN 'SINK_WRITE_DONE'
        ELSE 'UNKNOWN'
    END AS stage_name,
    e.stage,
    e.task_id,
    FROM_UNIXTIME(e.ts_ms / 1000.0) AS ts,
    e.ts_ms,
    e.worker_address,
    e.task_group_name,
    e.task_class
FROM st_trace t
LEFT JOIN st_trace_entry e ON t.trace_id = e.trace_id AND t.sink_task_id = e.sink_task_id
ORDER BY t.trace_id, e.entry_index;

-- 创建视图：端到端延迟分析
CREATE OR REPLACE VIEW v_trace_latency AS
SELECT
    t.trace_id,
    t.job_id,
    t.table_id,
    t.received_at,
    MIN(CASE WHEN e.stage = 1 THEN e.ts_ms END) AS source_emit_ms,
    MIN(CASE WHEN e.stage = 6 THEN e.ts_ms END) AS sink_done_ms,
    MIN(CASE WHEN e.stage = 6 THEN e.ts_ms END) - MIN(CASE WHEN e.stage = 1 THEN e.ts_ms END) AS e2e_latency_ms,
    MIN(CASE WHEN e.stage = 3 THEN e.ts_ms END) - MIN(CASE WHEN e.stage = 2 THEN e.ts_ms END) AS queue_wait_ms,
    MIN(CASE WHEN e.stage = 5 THEN e.ts_ms END) - MIN(CASE WHEN e.stage = 4 THEN e.ts_ms END) AS transform_ms
FROM st_trace t
LEFT JOIN st_trace_entry e ON t.trace_id = e.trace_id AND t.sink_task_id = e.sink_task_id
GROUP BY t.trace_id, t.job_id, t.table_id, t.received_at;

SHOW TABLES;

SELECT 'MySQL表初始化完成！' AS status;
