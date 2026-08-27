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
-- DATABASE:  source
-- ----------------------------------------------------------------------------------------------------------------
CREATE DATABASE IF NOT EXISTS `source`;
use `source`;

drop table if exists `source`.`products`;
-- Create and populate our products using a single insert with many rows
CREATE TABLE products (
  id INTEGER NOT NULL AUTO_INCREMENT PRIMARY KEY,
  name VARCHAR(255) NOT NULL DEFAULT 'SeaTunnel',
  description VARCHAR(512),
  weight FLOAT
);

ALTER TABLE `source`.`products` AUTO_INCREMENT = 101;

INSERT INTO `source`.`products`
VALUES (101,"scooter","Small 2-wheel scooter",3.14),
       (102,"car battery","12V car battery",8.1),
       (103,"12-pack drill bits","12-pack of drill bits with sizes ranging from #40 to #3",0.8),
       (104,"hammer","12oz carpenter's hammer",0.75),
       (105,"hammer","14oz carpenter's hammer",0.875),
       (106,"hammer","16oz carpenter's hammer",1.0),
       (107,"rocks","box of assorted rocks",5.3),
       (108,"jacket","water resistent black wind breaker",0.1),
       (109,"spare tire","24 inch spare tire",22.2);


DROP TABLE IF EXISTS `source`.`customers`;
CREATE TABLE `source`.`customers` (
   id INTEGER NOT NULL AUTO_INCREMENT PRIMARY KEY,
   first_name VARCHAR(255) NOT NULL,
   last_name VARCHAR(255) NOT NULL,
   email VARCHAR(255) NOT NULL UNIQUE KEY
) AUTO_INCREMENT=1001;


INSERT INTO `source`.`customers`
VALUES (1001,"Sally","Thomas","sally.thomas@acme.com"),
       (1002,"George","Bailey","gbailey@foobar.com"),
       (1003,"Edward","Walker","ed@walker.com"),
       (1004,"Anne","Kretchmar","annek@noanswer.org");


-- ----------------------------------------------------------------------------------------------------------------
-- DATABASE:  source1
-- ----------------------------------------------------------------------------------------------------------------
CREATE DATABASE IF NOT EXISTS `source1`;
use `source1`;

DROP TABLE IF EXISTS `source1`.`orders`;
CREATE TABLE `source1`.`orders` (
    order_number INTEGER NOT NULL AUTO_INCREMENT PRIMARY KEY,
    order_date DATE NOT NULL,
    purchaser INTEGER NOT NULL,
    quantity INTEGER NOT NULL,
    product_id INTEGER NOT NULL
) AUTO_INCREMENT = 10001;


INSERT INTO `source1`.`orders`
VALUES (10001, '2016-01-16', 1001, 1, 102),
       (10002, '2016-01-17', 1002, 2, 105),
       (10003, '2016-02-18', 1004, 3, 109),
       (10004, '2016-02-19', 1002, 2, 106),
       (10005, '16-02-21', 1003, 1, 107);

CREATE DATABASE IF NOT EXISTS `sink`;

use `sink`;

DROP TABLE IF EXISTS `source_products`;
DROP TABLE IF EXISTS `source_customers`;
DROP TABLE IF EXISTS `source1_orders`;

