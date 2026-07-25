-- Licensed to the Apache Software Foundation (ASF) under one or more
-- contributor license agreements. See the NOTICE file distributed with
-- this work for additional information regarding copyright ownership.
--
-- Pre-created sink tables for the SeaTunnel AI CLI benchmark (ClickHouse).

CREATE DATABASE IF NOT EXISTS bench;

CREATE TABLE IF NOT EXISTS bench.products (
    id Int64,
    sku String,
    name String,
    price Decimal(10, 2)
) ENGINE = MergeTree() ORDER BY id;

CREATE TABLE IF NOT EXISTS bench.fact_sales (
    id Int64,
    region String,
    qty Int32,
    revenue Decimal(12, 2)
) ENGINE = MergeTree() ORDER BY id;

CREATE TABLE IF NOT EXISTS bench.orders_daily (
    order_id Int64,
    amount Decimal(10, 2),
    order_date Date
) ENGINE = MergeTree() ORDER BY order_id;

CREATE TABLE IF NOT EXISTS bench.dim_products (
    id Int64,
    sku String,
    name String,
    price Decimal(10, 2)
) ENGINE = MergeTree() ORDER BY id;

CREATE TABLE IF NOT EXISTS bench.price_updates (
    sku String,
    new_price Decimal(10, 2)
) ENGINE = MergeTree() ORDER BY sku;

CREATE TABLE IF NOT EXISTS bench.page_stats (
    page String,
    views Int64
) ENGINE = MergeTree() ORDER BY page;

CREATE TABLE IF NOT EXISTS bench.customers (
    id Int64,
    name String,
    tier String,
    signed_up Date
) ENGINE = MergeTree() ORDER BY id;
