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
-- Drops and re-creates, in one statement, a column that a SQL transform projects directly.
-- Runs after modify_weight_type, when products has the columns id, name, description, weight, add_column.
-- ----------------------------------------------------------------------------------------------------------------
CREATE DATABASE IF NOT EXISTS `shop`;

use shop;

alter table products drop column name, add column name varchar(64) not null default 'n';

insert into products
values (185,"Small 2-wheel scooter",3.14,1,'n1'),
       (186,"12V car battery",8.1,2,'n2');

update products set name = 'renamed' where id = 182;

delete from products where id = 183;
