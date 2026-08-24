--
-- Licensed to the Apache Software Foundation (ASF) under one or more
-- contributor license agreements.  See the NOTICE file distributed with
-- this work for additional information regarding copyright ownership.
-- The ASF licenses this file to You under the Apache License, Version 2.0
-- (the "License"); you may not use this file except in compliance with
-- the License.  You may obtain a copy of the License at
--
--     http://www.apache.org/licenses/LICENSE-2.0
--
-- Unless required by applicable law or agreed to in writing, software
-- distributed under the License is distributed on an "AS IS" BASIS,
-- WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
-- See the License for the specific language governing permissions and
-- limitations under the License.
--

CREATE DATABASE IF NOT EXISTS trade_db;
USE trade_db;

DROP TABLE IF EXISTS orders;
CREATE TABLE orders (
  id BIGINT NOT NULL PRIMARY KEY,
  order_no VARCHAR(64) NOT NULL,
  user_id BIGINT NOT NULL,
  amount DECIMAL(10, 2) NOT NULL,
  status VARCHAR(32) NOT NULL,
  create_time DATETIME NOT NULL
);

INSERT INTO orders (id, order_no, user_id, amount, status, create_time) VALUES
  (1, 'ORD-20260823-001', 10001, 99.50, 'completed', '2026-08-23 10:15:30'),
  (2, 'ORD-20260823-002', 10002, 199.00, 'pending', '2026-08-23 14:20:00'),
  (3, 'ORD-20260824-001', 10003, 49.90, 'COMPLETED', '2026-08-24 09:00:15'),
  (4, 'ORD-20260824-002', 10001, 350.00, 'paid', '2026-08-24 18:45:10'),
  (5, 'ORD-20260824-003', 10004, -10.00, 'cancelled', '2026-08-24 20:00:00');
