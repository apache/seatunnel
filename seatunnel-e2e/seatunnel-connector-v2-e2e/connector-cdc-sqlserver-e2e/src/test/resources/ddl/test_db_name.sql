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

-- ----------------------------------------------------------------------------------------------------------------
-- DATABASE:  test-db-name (database name with hyphen to test special character handling)
-- ----------------------------------------------------------------------------------------------------------------
CREATE DATABASE [test-db-name];

USE [test-db-name];
EXEC sys.sp_cdc_enable_db;

CREATE TABLE simple_table (
    id int NOT NULL,
    name varchar(100),
    value int,
    PRIMARY KEY (id)
);

INSERT INTO simple_table VALUES (1, 'test1', 100);
INSERT INTO simple_table VALUES (2, 'test2', 200);
INSERT INTO simple_table VALUES (3, 'test3', 300);

EXEC sys.sp_cdc_enable_table @source_schema = 'dbo', @source_name = 'simple_table', @role_name = NULL, @supports_net_changes = 0;

CREATE TABLE simple_table_sink (
    id int NOT NULL,
    name varchar(100),
    value int,
    PRIMARY KEY (id)
);
