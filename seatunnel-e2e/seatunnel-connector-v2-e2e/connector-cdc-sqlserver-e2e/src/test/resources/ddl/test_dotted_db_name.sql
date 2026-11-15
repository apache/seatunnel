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
-- DATABASE:  LIS6.0 (database name containing a dot)
-- ----------------------------------------------------------------------------------------------------------------
CREATE DATABASE [LIS6.0];

USE [LIS6.0];
EXEC sys.sp_cdc_enable_db;

CREATE TABLE LIS_ITEM (
    id int NOT NULL,
    name varchar(100),
    value int,
    PRIMARY KEY (id)
);

INSERT INTO LIS_ITEM VALUES (1, 'test1', 100);
INSERT INTO LIS_ITEM VALUES (2, 'test2', 200);
INSERT INTO LIS_ITEM VALUES (3, 'test3', 300);

EXEC sys.sp_cdc_enable_table @source_schema = 'dbo', @source_name = 'LIS_ITEM', @role_name = NULL, @supports_net_changes = 0;

CREATE TABLE LIS_ITEM_SINK (
    id int NOT NULL,
    name varchar(100),
    value int,
    PRIMARY KEY (id)
);
