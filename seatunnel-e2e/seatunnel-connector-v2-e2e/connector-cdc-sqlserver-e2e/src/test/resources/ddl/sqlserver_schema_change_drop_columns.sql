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
    @capture_instance = 'dbo_products_v2';

ALTER TABLE dbo.products DROP COLUMN add_column4;

INSERT INTO dbo.products VALUES (130, 'scooter', 'Small 2-wheel scooter', 3.14, 'xx', 1, 1.1);
INSERT INTO dbo.products VALUES (131, 'car battery', '12V car battery', 8.1, 'xx', 2, 1.2);

ALTER TABLE dbo.products DROP COLUMN add_column1, add_column3;

EXEC sys.sp_cdc_enable_table
    @source_schema = 'dbo',
    @source_name = 'products',
    @role_name = NULL,
    @capture_instance = 'dbo_products_v3',
    @supports_net_changes = 0;

-- Rows inserted between capture-instance switches may be emitted by an older
-- instance without add_column2. Touch them again after v3 is enabled so CDC
-- emits the current schema and sink rows converge.
UPDATE dbo.products
SET add_column2 = CASE id WHEN 130 THEN 11 WHEN 131 THEN 12 END
WHERE id IN (130, 131);

INSERT INTO dbo.products VALUES (140, 'scooter', 'Small 2-wheel scooter', 3.14, 1);
INSERT INTO dbo.products VALUES (141, 'car battery', '12V car battery', 8.1, 2);
UPDATE dbo.products SET name = 'updated-after-drop' WHERE id = 140;
