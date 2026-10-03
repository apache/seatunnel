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
--

-- ----------------------------------------------------------------------------------------------------------------
-- DATABASE:  shop
-- Changes the type of a column that a SQL transform both projects directly and uses in an expression.
-- FLOAT to DECIMAL(12,3) also changes the derived type of `weight * 2` from DOUBLE to DECIMAL(12,3); a change
-- to DOUBLE would not, because the expression is already derived as DOUBLE for a FLOAT operand.
-- Runs after modify_columns, when products has the columns id, name, description, weight, add_column.
-- ----------------------------------------------------------------------------------------------------------------
CREATE DATABASE IF NOT EXISTS `shop`;

use shop;

alter table products modify weight decimal(12,3);

insert into products
values (182,"scooter","Small 2-wheel scooter",3.14,1),
       (183,"car battery","12V car battery",8.1,2),
       (184,"12-pack drill bits","12-pack of drill bits with sizes ranging from #40 to #3",0.8,3);

update products set weight = 2.5 where id = 164;

delete from products where id = 165;
