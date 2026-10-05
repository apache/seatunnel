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
    @capture_instance = 'dbo_products_v3';

EXEC sp_rename 'dbo.products.add_column2', 'add_column', 'COLUMN';

EXEC sys.sp_cdc_enable_table
    @source_schema = 'dbo',
    @source_name = 'products',
    @role_name = NULL,
    @capture_instance = 'dbo_products_v4',
    @supports_net_changes = 0;

-- SQL Server exposes sp_rename as a capture-instance switch. SeaTunnel now
-- treats it conservatively as ADD + DROP unless the downstream sink can
-- preserve existing values itself, so force real post-switch row updates under
-- the renamed column. A no-op assignment is not enough here because SQL Server
-- may skip writing CDC rows when the value does not change.
UPDATE dbo.products
SET add_column = add_column + 1000
WHERE id IN (101, 103, 104, 105, 106, 107, 108, 109, 110, 120, 121, 128, 129, 130, 131, 140, 141);

UPDATE dbo.products
SET add_column = add_column - 1000
WHERE id IN (101, 103, 104, 105, 106, 107, 108, 109, 110, 120, 121, 128, 129, 130, 131, 140, 141);

DELETE FROM dbo.products WHERE id = 130;
INSERT INTO dbo.products VALUES (150, 'scooter', 'Small 2-wheel scooter', 3.14, 1);
INSERT INTO dbo.products VALUES (151, 'car battery', '12V car battery', 8.1, 2);
