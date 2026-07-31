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

-- Source-of-truth data for the SeaTunnel AI CLI benchmark (MySQL).

CREATE DATABASE IF NOT EXISTS shop;
USE shop;

CREATE TABLE users (
    id BIGINT PRIMARY KEY AUTO_INCREMENT,
    name VARCHAR(64) NOT NULL,
    email VARCHAR(128),
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE orders (
    order_id BIGINT PRIMARY KEY AUTO_INCREMENT,
    user_id BIGINT NOT NULL,
    amount DECIMAL(10, 2) NOT NULL,
    status VARCHAR(16) DEFAULT 'NEW',
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE payments (
    payment_id BIGINT PRIMARY KEY AUTO_INCREMENT,
    order_id BIGINT NOT NULL,
    method VARCHAR(16),
    paid_amount DECIMAL(10, 2),
    paid_at DATETIME
);

CREATE TABLE products (
    id BIGINT PRIMARY KEY AUTO_INCREMENT,
    sku VARCHAR(32) NOT NULL,
    name VARCHAR(128),
    price DECIMAL(10, 2)
);

CREATE TABLE audit_log (
    id BIGINT PRIMARY KEY AUTO_INCREMENT,
    actor VARCHAR(64),
    action VARCHAR(64),
    ts DATETIME DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE transactions (
    tx_id BIGINT PRIMARY KEY AUTO_INCREMENT,
    account VARCHAR(32),
    amount DECIMAL(10, 2),
    ts DATETIME DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE fact_sales (
    id BIGINT PRIMARY KEY AUTO_INCREMENT,
    region VARCHAR(32),
    qty INT,
    revenue DECIMAL(12, 2)
);

-- Streaming sink targets (pre-created so JDBC sink tasks can insert)
CREATE TABLE orders_rt (
    order_id VARCHAR(64),
    amount DECIMAL(10, 2),
    created_at VARCHAR(64)
);

CREATE TABLE users_replica (
    id BIGINT PRIMARY KEY,
    name VARCHAR(64),
    email VARCHAR(128)
);

CREATE TABLE products_replica (
    id BIGINT PRIMARY KEY,
    sku VARCHAR(32),
    name VARCHAR(128),
    price DECIMAL(10, 2)
);

CREATE TABLE bench_landing (
    id BIGINT,
    label VARCHAR(64)
);

INSERT INTO users (name, email) VALUES
    ('alice', 'alice@example.com'),
    ('bob', 'bob@example.com'),
    ('carol', 'carol@example.com'),
    ('dave', 'dave@example.com'),
    ('erin', 'erin@example.com');

INSERT INTO orders (user_id, amount, status, created_at) VALUES
    (1, 25.50, 'PAID', '2026-01-05 10:00:00'),
    (2, 1200.00, 'PAID', '2026-01-06 11:30:00'),
    (3, 89.99, 'NEW', '2026-01-07 09:15:00'),
    (1, 450.00, 'SHIPPED', '2026-02-01 16:45:00'),
    (4, 15.00, 'PAID', '2026-02-14 08:00:00');

INSERT INTO payments (order_id, method, paid_amount, paid_at) VALUES
    (1, 'card', 25.50, '2026-01-05 10:01:00'),
    (2, 'wire', 1200.00, '2026-01-06 11:35:00'),
    (5, 'card', 15.00, '2026-02-14 08:01:00');

INSERT INTO products (sku, name, price) VALUES
    ('SKU-001', 'Widget', 19.99),
    ('SKU-002', 'Gadget', 5.50),
    ('SKU-003', 'Gizmo', 120.00);

INSERT INTO audit_log (actor, action) VALUES
    ('alice', 'LOGIN'),
    ('bob', 'UPDATE_PROFILE'),
    ('carol', 'LOGIN'),
    ('admin', 'DELETE_USER');

INSERT INTO transactions (account, amount) VALUES
    ('ACC-1', 50.00),
    ('ACC-2', 1500.00),
    ('ACC-3', 99.99),
    ('ACC-1', 2500.00),
    ('ACC-4', 101.00);

INSERT INTO fact_sales (region, qty, revenue) VALUES
    ('north', 10, 199.90),
    ('south', 5, 27.50),
    ('east', 20, 2400.00),
    ('west', 8, 960.00);
