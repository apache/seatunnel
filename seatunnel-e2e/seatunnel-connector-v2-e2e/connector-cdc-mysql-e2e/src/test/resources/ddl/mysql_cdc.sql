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
-- DATABASE:  inventory
-- ----------------------------------------------------------------------------------------------------------------
CREATE DATABASE IF NOT EXISTS `mysql_cdc`;

use mysql_cdc;
-- Create a mysql data source table
CREATE TABLE mysql_cdc.products_cdc (
  id INTEGER NOT NULL AUTO_INCREMENT PRIMARY KEY,
  name VARCHAR(255) NOT NULL DEFAULT 'SeaTunnel',
  description VARCHAR(512),
  weight FLOAT
);
truncate table mysql_cdc.products_cdc;
ALTER TABLE mysql_cdc.products_cdc AUTO_INCREMENT = 101;

INSERT INTO mysql_cdc.products_cdc
VALUES (101,"scooter","Small 2-wheel scooter",3.14),
       (102,"car battery","12V car battery",8.1),
       (103,"12-pack drill bits","12-pack of drill bits with sizes ranging from #40 to #3",0.8),
       (104,"hammer","12oz carpenter's hammer",0.75),
       (105,"hammer","14oz carpenter's hammer",0.875),
       (106,"hammer","16oz carpenter's hammer",1.0),
       (107,"rocks","box of assorted rocks",5.3),
       (108,"jacket","water resistent black wind breaker",0.1),
       (109,"spare tire","24 inch spare tire",22.2);

-- Create target table
CREATE TABLE mysql_cdc.products_cdc_sink (
                          id INTEGER NOT NULL AUTO_INCREMENT PRIMARY KEY,
                          name VARCHAR(255) NOT NULL DEFAULT 'SeaTunnel',
                          description VARCHAR(512),
                          weight FLOAT
);
truncate table mysql_cdc.products_cdc_sink;
