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
CREATE DATABASE IF NOT EXISTS `shop`;
use shop;

-- ADD / MODIFY of SET and ENUM columns reach the sink as the DDL the job builds from the column's
-- source type expression, so these statements are only valid when that expression keeps the option
-- list (SET('a','b')) instead of the Debezium bookkeeping length (SET(5)).
--
-- The option lists avoid the word "unsigned" for the same reason as the fixture: a literal such as
-- 'NO_UNSIGNED_SUBTRACTION' is mis-read as the UNSIGNED attribute until #12333 lands, which would
-- make this case depend on a second, unrelated PR.
alter table products_with_set_enum
  add column c_set_added set('a','b') null,
  add column c_enum_added enum('x','y') null,
  modify column c_set set('REAL_AS_FLOAT','PIPES_AS_CONCAT','ANSI_QUOTES') null;

insert into products_with_set_enum (id, name, weight, c_set, c_enum, c_set_added, c_enum_added)
values (201, 'added after the schema change', 0.5, 'ANSI_QUOTES', 'signed', 'a,b', 'x');

update products_with_set_enum set name = 'scooter updated' where id = 101;
delete from products_with_set_enum where id = 102;
