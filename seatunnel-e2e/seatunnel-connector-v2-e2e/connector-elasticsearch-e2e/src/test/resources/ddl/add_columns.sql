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

alter table products ADD COLUMN add_column1 varchar(64) not null default 'yy',ADD COLUMN add_column2 int not null default 1;

insert into products
values (119,"scooter","Small 2-wheel scooter",3.14,'xx',1),
       (120,"car battery","12V car battery",8.1,'xx',2),
       (121,"12-pack drill bits","12-pack of drill bits with sizes ranging from #40 to #3",0.8,'xx',3),
       (122,"hammer","12oz carpenter's hammer",0.75,'xx',4),
       (123,"hammer","14oz carpenter's hammer",0.875,'xx',5),
       (124,"hammer","16oz carpenter's hammer",1.0,'xx',6),
       (125,"rocks","box of assorted rocks",5.3,'xx',7),
       (126,"jacket","water resistent black wind breaker",0.1,'xx',8),
       (127,"spare tire","24 inch spare tire",22.2,'xx',9);


alter table products ADD COLUMN add_column3 float not null default 1.1;
alter table products ADD COLUMN add_column4 datetime not null default now();

