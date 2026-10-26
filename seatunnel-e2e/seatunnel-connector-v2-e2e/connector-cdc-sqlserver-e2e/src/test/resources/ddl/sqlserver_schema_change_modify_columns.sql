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

EXEC sys.sp_cdc_disable_table
    @source_schema = 'dbo',
    @source_name = 'products',
    @capture_instance = 'dbo_products_v4';

-- Use bounded length for stable source/sink metadata comparison in IT assertions.
ALTER TABLE dbo.products ALTER COLUMN name NVARCHAR(255) NULL;

EXEC sys.sp_cdc_enable_table
    @source_schema = 'dbo',
    @source_name = 'products',
    @role_name = NULL,
    @capture_instance = 'dbo_products_v5',
    @supports_net_changes = 0;

UPDATE dbo.products SET name = N'updated-after-modify' WHERE id = 150;
INSERT INTO dbo.products VALUES (160, N'scooter-modified', 'Small 2-wheel scooter', 3.14, 1);
INSERT INTO dbo.products VALUES (161, N'car battery-modified', '12V car battery', 8.1, 2);
