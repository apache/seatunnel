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
