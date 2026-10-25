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

DROP DATABASE IF EXISTS `multi_schema_shop_a`;
DROP DATABASE IF EXISTS `multi_schema_shop_b`;
DROP DATABASE IF EXISTS `multi_schema_shop_a_sink`;
DROP DATABASE IF EXISTS `multi_schema_shop_b_sink`;

CREATE DATABASE IF NOT EXISTS `multi_schema_shop_a`;
CREATE DATABASE IF NOT EXISTS `multi_schema_shop_b`;
CREATE DATABASE IF NOT EXISTS `multi_schema_shop_a_sink`;
CREATE DATABASE IF NOT EXISTS `multi_schema_shop_b_sink`;

USE `multi_schema_shop_a`;

CREATE TABLE IF NOT EXISTS products (
  id INTEGER NOT NULL AUTO_INCREMENT PRIMARY KEY,
  name VARCHAR(255) NOT NULL DEFAULT 'SeaTunnel',
  description VARCHAR(512),
  weight FLOAT
);

ALTER TABLE products AUTO_INCREMENT = 101;

INSERT INTO products
VALUES (101,"a-scooter","Source A scooter",3.14),
       (102,"a-battery","Source A battery",8.1),
       (103,"a-hammer","Source A hammer",0.75);

USE `multi_schema_shop_b`;

CREATE TABLE IF NOT EXISTS products (
  id INTEGER NOT NULL AUTO_INCREMENT PRIMARY KEY,
  name VARCHAR(255) NOT NULL DEFAULT 'SeaTunnel',
  description VARCHAR(512),
  weight FLOAT
);

ALTER TABLE products AUTO_INCREMENT = 101;

INSERT INTO products
VALUES (101,"b-scooter","Source B scooter",4.14),
       (102,"b-battery","Source B battery",9.1),
       (103,"b-hammer","Source B hammer",1.75);

USE `multi_schema_shop_a_sink`;
DROP TABLE IF EXISTS products;

CREATE TABLE IF NOT EXISTS products (
  id INTEGER NOT NULL AUTO_INCREMENT PRIMARY KEY,
  name VARCHAR(255) NOT NULL DEFAULT 'SeaTunnel',
  description VARCHAR(512),
  weight FLOAT
);

USE `multi_schema_shop_b_sink`;
DROP TABLE IF EXISTS products;

CREATE TABLE IF NOT EXISTS products (
  id INTEGER NOT NULL AUTO_INCREMENT PRIMARY KEY,
  name VARCHAR(255) NOT NULL DEFAULT 'SeaTunnel',
  description VARCHAR(512),
  weight FLOAT
);
