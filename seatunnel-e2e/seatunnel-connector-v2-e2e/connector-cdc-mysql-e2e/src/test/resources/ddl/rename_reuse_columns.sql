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
-- Renames a column and reuses its name in one statement, then drops and re-creates the reused name in one
-- statement. Runs after modify_columns and comment_changes, when products has the columns
-- id, name, description, weight, add_column.
-- ----------------------------------------------------------------------------------------------------------------
CREATE DATABASE IF NOT EXISTS `shop`;

use shop;

alter table products add column reuse_col int not null default 1;

insert into products
values (200,"scooter","Small 2-wheel scooter",3.14,1,11),
       (201,"car battery","12V car battery",8.1,2,12);

alter table products change column reuse_col reuse_col_old int not null default 1, add column reuse_col varchar(16) not null default 'new';

insert into products
values (202,"12-pack drill bits","12-pack of drill bits with sizes ranging from #40 to #3",0.8,3,13,'reused'),
       (203,"hammer","12oz carpenter's hammer",0.75,4,14,'reused');

update products set name = 'dailai' where id = 200;

alter table products drop column reuse_col, add column reuse_col bigint not null default 9;

insert into products
values (204,"rocks","box of assorted rocks",5.3,5,15,99),
       (205,"jacket","water resistent black wind breaker",0.1,6,16,100);

delete from products where id = 201;
