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
-- Source and sink tables of the case where DDL names existing columns in another case.
CREATE DATABASE IF NOT EXISTS `shop`;
use shop;
drop table if exists products_column_case;
CREATE TABLE products_column_case (
  id INTEGER NOT NULL AUTO_INCREMENT PRIMARY KEY,
  name VARCHAR(255) NOT NULL DEFAULT 'SeaTunnel',
  description VARCHAR(512),
  weight FLOAT
);
insert into products_column_case (id, name, description, weight)
values (101, 'scooter', 'Small 2-wheel scooter', 3.14),
       (102, 'car battery', '12V car battery', 8.1);
drop table if exists mysql_cdc_e2e_sink_table_column_case;
CREATE TABLE mysql_cdc_e2e_sink_table_column_case (
  id INTEGER NOT NULL AUTO_INCREMENT PRIMARY KEY,
  name VARCHAR(255) NOT NULL DEFAULT 'SeaTunnel',
  description VARCHAR(512),
  weight FLOAT
);
