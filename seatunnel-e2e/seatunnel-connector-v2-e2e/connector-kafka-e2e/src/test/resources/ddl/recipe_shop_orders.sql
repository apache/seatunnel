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

USE $DBNAME$;

DROP TABLE IF EXISTS orders;
CREATE TABLE orders (
  id BIGINT NOT NULL PRIMARY KEY,
  order_no VARCHAR(64) NOT NULL,
  user_id BIGINT NOT NULL,
  status INT NOT NULL,
  amount DECIMAL(10, 2) NOT NULL
);

INSERT INTO orders (id, order_no, user_id, status, amount) VALUES
  (1001, 'ORD-1001', 501, 0, 19.99),
  (1002, 'ORD-1002', 502, 1, 29.99);
