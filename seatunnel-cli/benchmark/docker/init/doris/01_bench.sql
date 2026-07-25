-- Licensed to the Apache Software Foundation (ASF) under one or more
-- contributor license agreements. See the NOTICE file distributed with
-- this work for additional information regarding copyright ownership.
--
-- Pre-created sink tables for the benchmark (Doris / StarRocks — both speak
-- the MySQL protocol on the query port; apply with:
--   mysql -h127.0.0.1 -P9030 -uroot < 01_bench.sql        # doris
--   mysql -h127.0.0.1 -P9031 -uroot < 01_bench.sql        # starrocks
-- The runner applies this automatically when the olap profile is up).

CREATE DATABASE IF NOT EXISTS bench;
USE bench;

CREATE TABLE IF NOT EXISTS customers (
    id BIGINT,
    name VARCHAR(64),
    tier VARCHAR(16),
    signed_up DATE
) UNIQUE KEY(id) DISTRIBUTED BY HASH(id) BUCKETS 1
PROPERTIES ("replication_num" = "1");

CREATE TABLE IF NOT EXISTS orders (
    order_id BIGINT,
    user_id BIGINT,
    amount DECIMAL(10, 2),
    status VARCHAR(16),
    created_at DATETIME
) UNIQUE KEY(order_id) DISTRIBUTED BY HASH(order_id) BUCKETS 1
PROPERTIES ("replication_num" = "1");
