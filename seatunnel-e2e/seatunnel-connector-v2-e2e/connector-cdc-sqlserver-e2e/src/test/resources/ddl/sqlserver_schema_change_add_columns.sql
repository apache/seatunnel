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

USE schema_change_test;

INSERT INTO dbo.products VALUES (110, 'scooter', 'Small 2-wheel scooter', 3.14);
UPDATE dbo.products SET name = 'updated-before-add' WHERE id = 101;
DELETE FROM dbo.products WHERE id = 102;

ALTER TABLE dbo.products
ADD add_column1 VARCHAR(64) NOT NULL CONSTRAINT df_products_add_column1 DEFAULT 'yy',
    add_column2 INT NOT NULL CONSTRAINT df_products_add_column2 DEFAULT 1;

INSERT INTO dbo.products VALUES (120, 'car battery', '12V car battery', 8.1, 'xx', 2);
INSERT INTO dbo.products VALUES (121, 'drill bits', 'sizes from #40 to #3', 0.8, 'xx', 3);

ALTER TABLE dbo.products
ADD add_column3 FLOAT NOT NULL CONSTRAINT df_products_add_column3 DEFAULT 1.1;

ALTER TABLE dbo.products
ADD add_column4 DATETIME2 NOT NULL CONSTRAINT df_products_add_column4 DEFAULT ('2023-02-02T09:09:09');

EXEC sys.sp_cdc_enable_table
    @source_schema = 'dbo',
    @source_name = 'products',
    @role_name = NULL,
    @capture_instance = 'dbo_products_v2',
    @supports_net_changes = 0;

INSERT INTO dbo.products VALUES (128, 'scooter', 'Small 2-wheel scooter', 3.14, 'xx', 1, 1.1, '2023-02-02T09:09:09');
INSERT INTO dbo.products VALUES (129, 'car battery', '12V car battery', 8.1, 'xx', 2, 1.2, '2023-02-02T09:09:09');
