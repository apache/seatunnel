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

CREATE USER IF NOT EXISTS 'st_user_source'@'%' IDENTIFIED BY 'mysqlpw';
GRANT SELECT, RELOAD, SHOW DATABASES, REPLICATION SLAVE, REPLICATION CLIENT, LOCK TABLES
    ON *.* TO 'st_user_source'@'%';

CREATE DATABASE IF NOT EXISTS mysql_cdc;

USE mysql_cdc;

CREATE TABLE orders (
    id BIGINT NOT NULL,
    order_code VARCHAR(64) NOT NULL,
    amount DECIMAL(10, 2) NOT NULL,
    PRIMARY KEY (id)
) ENGINE = InnoDB;

CREATE TABLE customers (
    id BIGINT NOT NULL,
    customer_code VARCHAR(64) NOT NULL,
    region VARCHAR(64) NOT NULL,
    PRIMARY KEY (id)
) ENGINE = InnoDB;

INSERT INTO orders (id, order_code, amount)
VALUES
    (1, 'order-1', 10.50),
    (2, 'order-2', 20.25),
    (3, 'order-3', 30.75);

INSERT INTO customers (id, customer_code, region)
VALUES
    (1, 'customer-1', 'east'),
    (2, 'customer-2', 'west'),
    (3, 'customer-3', 'central');
