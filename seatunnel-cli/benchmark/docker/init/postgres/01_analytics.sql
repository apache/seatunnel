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

-- Source-of-truth data for the SeaTunnel AI CLI benchmark (PostgreSQL).

CREATE TABLE app_logs (
    id BIGSERIAL PRIMARY KEY,
    level VARCHAR(8),
    message TEXT,
    logged_at TIMESTAMP DEFAULT now()
);

CREATE TABLE customers (
    id BIGSERIAL PRIMARY KEY,
    name VARCHAR(64),
    tier VARCHAR(16),
    signed_up DATE
);

CREATE TABLE web_events (
    id BIGSERIAL PRIMARY KEY,
    uid BIGINT,
    evt VARCHAR(32),
    url TEXT,
    ts TIMESTAMP DEFAULT now()
);

CREATE TABLE inventory (
    sku VARCHAR(32) PRIMARY KEY,
    qty INT,
    updated_at TIMESTAMP DEFAULT now()
);

-- PostgreSQL-CDC requires REPLICA IDENTITY FULL for tables without suitable keys
ALTER TABLE inventory REPLICA IDENTITY FULL;

INSERT INTO app_logs (level, message) VALUES
    ('INFO', 'service started'),
    ('WARN', 'cache miss rate high'),
    ('ERROR', 'db connection dropped'),
    ('INFO', 'service healthy');

INSERT INTO customers (name, tier, signed_up) VALUES
    ('acme corp', 'gold', '2025-03-01'),
    ('globex', 'silver', '2025-06-15'),
    ('initech', 'bronze', '2025-11-20');

INSERT INTO web_events (uid, evt, url) VALUES
    (1, 'view', '/home'),
    (1, 'click', '/product/1'),
    (2, 'view', '/pricing'),
    (3, 'signup', '/register');

INSERT INTO inventory (sku, qty) VALUES
    ('SKU-001', 100),
    ('SKU-002', 42),
    ('SKU-003', 7);
