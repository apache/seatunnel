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
-- DATABASE:  shop
-- ----------------------------------------------------------------------------------------------------------------
-- MySQL column names are case-insensitive: every statement below names an existing column in
-- another case than the table defines it, or renames one only by case, and rows follow each statement.
CREATE DATABASE IF NOT EXISTS `shop`;
use shop;
alter table products_column_case add column note varchar(64) null after NAME;
insert into products_column_case (id, name, description, weight, note)
values (201, 'added after NAME', 'add', 1.5, 'note-201');
alter table products_column_case modify column NOTE varchar(128) null after WEIGHT;
insert into products_column_case (id, name, description, weight, note)
values (202, 'moved after WEIGHT', 'modify', 2.5, 'note-202');
alter table products_column_case change column Description details varchar(512) null;
insert into products_column_case (id, name, details, weight, note)
values (203, 'renamed Description', 'change', 3.5, 'note-203');
alter table products_column_case drop column WEIGHT;
insert into products_column_case (id, name, details, note)
values (204, 'dropped WEIGHT', 'drop', 'note-204');
alter table products_column_case change column details DETAILS varchar(512) null;
insert into products_column_case (id, name, details, note)
values (205, 'changed details to DETAILS', 'case only', 'note-205');
update products_column_case set details = 'updated details' where id = 201;
update products_column_case set note = 'updated' where id = 101;
delete from products_column_case where id = 102;
