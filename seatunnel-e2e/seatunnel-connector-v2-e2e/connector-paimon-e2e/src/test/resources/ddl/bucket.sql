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
CREATE DATABASE IF NOT EXISTS `bucket`;
use bucket;

drop table if exists test_dynamic_bucket;
-- Create and populate our products using a single insert with many rows
CREATE TABLE test_dynamic_bucket (
  id INTEGER NOT NULL AUTO_INCREMENT PRIMARY KEY,
  name VARCHAR(255) NOT NULL DEFAULT 'SeaTunnel',
  description VARCHAR(512),
  version VARCHAR(2)
);


INSERT INTO test_dynamic_bucket
VALUES (101,"scooter","Small 2-wheel scooter",'1'),
       (102,"car battery","12V car battery",'1'),
       (103,"12-pack drill bits","12-pack of drill bits with sizes ranging from #40 to #3",'1'),
       (104,"hammer","12oz carpenter's hammer",'1'),
       (105,"hammer","14oz carpenter's hammer",'1'),
       (106,"zhang","16oz carpenter's hammer",'1'),
       (107,"rocks","box of assorted rocks",'1'),
       (108,"jacket","water resistent black wind breaker",'1'),
       (109,"hawk","water resistent black wind breaker",'1'),
       (110,"spare tire","24 inch spare tire",'1');



