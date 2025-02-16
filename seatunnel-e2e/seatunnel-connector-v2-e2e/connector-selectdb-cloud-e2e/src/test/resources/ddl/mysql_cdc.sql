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
CREATE TABLE IF NOT EXISTS `mysql_cdc`.`mysql_cdc_e2e_source_table` (
  `uuid` BIGINT,
  `name` VARCHAR(128),
  `score` INT,
  PRIMARY KEY (`uuid`)
) ENGINE=InnoDB;



truncate table `mysql_cdc`.`mysql_cdc_e2e_source_table`;

INSERT INTO `mysql_cdc`.`mysql_cdc_e2e_source_table` (uuid, name, score) VALUES
(1, 'Alice', 95),
(2, 'Bob', 88);