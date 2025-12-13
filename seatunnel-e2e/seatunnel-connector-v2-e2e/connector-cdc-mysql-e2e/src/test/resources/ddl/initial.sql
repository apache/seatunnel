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
-- DATABASE:  $DBNAME$ (schema_evolution_test)
-- ----------------------------------------------------------------------------------------------------------------

-- Create the initial products table for schema evolution testing
CREATE TABLE products (
  id INT PRIMARY KEY,
  name VARCHAR(100) NOT NULL,
  description TEXT,
  weight DECIMAL(10,2),
  price DECIMAL(10,2) NOT NULL
);

-- Create products_on_hand table for testing
CREATE TABLE products_on_hand (
  product_id INTEGER NOT NULL PRIMARY KEY,
  quantity INTEGER NOT NULL,
  FOREIGN KEY (product_id) REFERENCES products(id)
);

-- Insert initial test data
INSERT INTO products VALUES 
(101, 'scooter', 'Small 2-wheel scooter', 3.14, 15.99),
(102, 'car battery', '12V car battery', 8.1, 89.99),
(103, '12-pack drill bits', '12-pack of drill bits with sizes ranging from #40 to #3', 0.8, 19.99),
(104, 'hammer', '12oz carpenter''s hammer', 0.75, 12.99),
(105, 'hammer', '14oz carpenter''s hammer', 0.875, 14.99),
(106, 'hammer', '16oz carpenter''s hammer', 1.0, 16.99),
(107, 'rocks', 'box of assorted rocks', 5.3, 9.99),
(108, 'jacket', 'water resistent black wind breaker', 0.1, 39.99),
(109, 'spare tire', '24 inch spare tire', 22.2, 99.99);

-- Insert initial inventory data
INSERT INTO products_on_hand VALUES 
(101, 3),
(102, 8),
(103, 18),
(104, 4),
(105, 5),
(106, 0),
(107, 44),
(108, 2),
(109, 5);
