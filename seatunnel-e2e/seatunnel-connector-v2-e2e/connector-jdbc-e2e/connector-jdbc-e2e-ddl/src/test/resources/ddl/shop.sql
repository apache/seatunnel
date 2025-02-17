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

-- ----------------------------------------------------------------------------------------------------------------
-- DATABASE:  shop
-- ----------------------------------------------------------------------------------------------------------------
CREATE DATABASE IF NOT EXISTS `shop`;
use shop;

drop table if exists products;
-- Create and populate our products using a single insert with many rows
CREATE TABLE products (
  id INTEGER NOT NULL AUTO_INCREMENT PRIMARY KEY,
  name VARCHAR(150) NOT NULL DEFAULT 'SeaTunnel',
  description VARCHAR(512),
  weight DECIMAL(8,2)
);

drop table if exists mysql_cdc_e2e_sink_table_with_schema_change;
CREATE TABLE if not exists mysql_cdc_e2e_sink_table_with_schema_change (
 id INTEGER NOT NULL AUTO_INCREMENT PRIMARY KEY,
 name VARCHAR(150) NOT NULL DEFAULT 'SeaTunnel',
 description VARCHAR(512),
 weight DECIMAL(8,2)
);

drop table if exists mysql_cdc_e2e_sink_table_with_schema_change_exactly_once;
CREATE TABLE if not exists mysql_cdc_e2e_sink_table_with_schema_change_exactly_once (
 id INTEGER NOT NULL AUTO_INCREMENT PRIMARY KEY,
 name VARCHAR(150) NOT NULL DEFAULT 'SeaTunnel',
 description VARCHAR(512),
 weight DECIMAL(8,2)
);

ALTER TABLE products AUTO_INCREMENT = 101;

INSERT INTO products
VALUES (101,"scooter","Small 2-wheel scooter",3.14),
       (102,"car battery","12V car battery",8.1),
       (103,"12-pack drill bits","12-pack of drill bits with sizes ranging from #40 to #3",0.8),
       (104,"hammer","12oz carpenter's hammer",0.75),
       (105,"hammer","14oz carpenter's hammer",0.87),
       (106,"hammer","16oz carpenter's hammer",1.0),
       (107,"rocks","box of assorted rocks",5.3),
       (108,"jacket","water resistent black wind breaker",0.1),
       (109,"spare tire","24 inch spare tire",22.2);


drop table if exists products_on_hand;
CREATE TABLE products_on_hand (
  product_id INTEGER NOT NULL PRIMARY KEY,
  quantity INTEGER NOT NULL
);


INSERT INTO products_on_hand VALUES (101,3);
INSERT INTO products_on_hand VALUES (102,8);
INSERT INTO products_on_hand VALUES (103,18);
INSERT INTO products_on_hand VALUES (104,4);
INSERT INTO products_on_hand VALUES (105,5);
INSERT INTO products_on_hand VALUES (106,0);
INSERT INTO products_on_hand VALUES (107,44);
INSERT INTO products_on_hand VALUES (108,2);
INSERT INTO products_on_hand VALUES (109,5);
