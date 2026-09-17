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

drop table if exists products_with_set_enum;
CREATE TABLE products_with_set_enum (
  id INTEGER NOT NULL AUTO_INCREMENT PRIMARY KEY,
  name VARCHAR(255) NOT NULL DEFAULT 'SeaTunnel',
  weight FLOAT,
  c_set set('REAL_AS_FLOAT','PIPES_AS_CONCAT','NO_UNSIGNED_SUBTRACTION') DEFAULT NULL,
  c_enum enum('unsigned','signed') DEFAULT NULL
);

insert into products_with_set_enum (id, name, weight, c_set, c_enum)
values (101, 'scooter', 3.14, 'REAL_AS_FLOAT', 'unsigned'),
       (102, 'car battery', 8.1, 'REAL_AS_FLOAT,NO_UNSIGNED_SUBTRACTION', 'signed');

-- Kept in step with the source table: the other cases in this suite pre-create their sink tables
-- so that the DESCRIBE comparison can cover the whole table, and an auto-created sink table is
-- rendered without AUTO_INCREMENT.
drop table if exists mysql_cdc_e2e_sink_table_with_set_enum;
CREATE TABLE mysql_cdc_e2e_sink_table_with_set_enum (
  id INTEGER NOT NULL AUTO_INCREMENT PRIMARY KEY,
  name VARCHAR(255) NOT NULL DEFAULT 'SeaTunnel',
  weight FLOAT,
  c_set set('REAL_AS_FLOAT','PIPES_AS_CONCAT','NO_UNSIGNED_SUBTRACTION') DEFAULT NULL,
  c_enum enum('unsigned','signed') DEFAULT NULL
);
