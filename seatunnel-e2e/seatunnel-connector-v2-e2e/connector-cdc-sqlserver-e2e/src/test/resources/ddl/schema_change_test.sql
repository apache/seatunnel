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

CREATE DATABASE schema_change_test;
USE schema_change_test;
EXEC sys.sp_cdc_enable_db;

CREATE TABLE dbo.products (
    id INT NOT NULL,
    name VARCHAR(255) NOT NULL,
    description VARCHAR(512),
    weight FLOAT,
    PRIMARY KEY (id)
);

CREATE TABLE dbo.products_sink (
    id INT NOT NULL,
    name VARCHAR(255) NOT NULL,
    description VARCHAR(512),
    weight FLOAT,
    PRIMARY KEY (id)
);

INSERT INTO dbo.products VALUES (101, 'scooter', 'Small 2-wheel scooter', 3.14);
INSERT INTO dbo.products VALUES (102, 'car battery', '12V car battery', 8.1);
INSERT INTO dbo.products VALUES (103, '12-pack drill bits', '12-pack of drill bits with sizes ranging from #40 to #3', 0.8);
INSERT INTO dbo.products VALUES (104, 'hammer', '12oz carpenter''s hammer', 0.75);
INSERT INTO dbo.products VALUES (105, 'hammer', '14oz carpenter''s hammer', 0.875);
INSERT INTO dbo.products VALUES (106, 'hammer', '16oz carpenter''s hammer', 1.0);
INSERT INTO dbo.products VALUES (107, 'rocks', 'box of assorted rocks', 5.3);
INSERT INTO dbo.products VALUES (108, 'jacket', 'water resistant black wind breaker', 0.1);
INSERT INTO dbo.products VALUES (109, 'spare tire', '24 inch spare tire', 22.2);

EXEC sys.sp_cdc_enable_table @source_schema = 'dbo', @source_name = 'products', @role_name = NULL, @supports_net_changes = 0;
