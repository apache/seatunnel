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
-- DATABASE:  $DBNAME$ (schema_evolution_test) - RENAME COLUMNS
-- ----------------------------------------------------------------------------------------------------------------

-- Rename columns in products table
ALTER TABLE products CHANGE COLUMN description product_description TEXT;
ALTER TABLE products CHANGE COLUMN weight product_weight DECIMAL(10,2);

-- Insert additional test data to verify rename functionality
INSERT INTO products VALUES 
(110, 'tablet', 'Android tablet with 10-inch screen', 1.2, 299.99),
(111, 'keyboard', 'Wireless bluetooth keyboard', 0.8, 59.99);
