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
-- DATABASE:  inventory
-- ----------------------------------------------------------------------------------------------------------------
CREATE DATABASE IF NOT EXISTS `mysql_cdc`;

use mysql_cdc;
-- Create a mysql data source table
CREATE TABLE mysql_cdc_e2e_source_table
(
    `id`                   int       NOT NULL AUTO_INCREMENT,
    `f_binary`             binary(64)                     DEFAULT NULL,
    `f_blob`               blob,
    `f_long_varbinary`     mediumblob,
    `f_longblob`           longblob,
    `f_tinyblob`           tinyblob,
    `f_varbinary`          varbinary(100)                 DEFAULT NULL,
    `f_smallint`           smallint                       DEFAULT NULL,
    `f_smallint_unsigned`  smallint unsigned              DEFAULT NULL,
    `f_mediumint`          mediumint                      DEFAULT NULL,
    `f_mediumint_unsigned` mediumint unsigned             DEFAULT NULL,
    `f_int`                int                            DEFAULT NULL,
    `f_int_unsigned`       int unsigned                   DEFAULT NULL,
    `f_integer`            int                            DEFAULT NULL,
    `f_integer_unsigned`   int unsigned                   DEFAULT NULL,
    `f_bigint`             bigint                         DEFAULT NULL,
    `f_bigint_unsigned`    bigint unsigned                DEFAULT NULL,
    `f_numeric`            decimal(10, 0)                 DEFAULT NULL,
    `f_decimal`            decimal(10, 0)                 DEFAULT NULL,
    `f_float`              float                          DEFAULT NULL,
    `f_double`             double                         DEFAULT NULL,
    `f_double_precision`   double                         DEFAULT NULL,
    `f_longtext`           longtext,
    `f_mediumtext`         mediumtext,
    `f_text`               text,
    `f_tinytext`           tinytext,
    `f_varchar`            varchar(100)                   DEFAULT NULL,
    `f_date`               date                           DEFAULT NULL,
    `f_datetime`           datetime                       DEFAULT NULL,
    `f_timestamp`          timestamp NULL                 DEFAULT NULL,
    `f_bit1`               bit(1)                         DEFAULT NULL,
    `f_bit64`              bit(64)                        DEFAULT NULL,
    `f_char`               char(1)                        DEFAULT NULL,
    `f_enum`               enum ('enum1','enum2','enum3') DEFAULT NULL,
    `f_mediumblob`         mediumblob,
    `f_long_varchar`       mediumtext,
    `f_real`               double                         DEFAULT NULL,
    `f_time`               time                           DEFAULT NULL,
    `f_tinyint`            tinyint                        DEFAULT NULL,
    `f_tinyint_unsigned`   tinyint unsigned               DEFAULT NULL,
    `f_json`               json                           DEFAULT NULL,
    `f_year`               year                           DEFAULT NULL,
    PRIMARY KEY (`id`)
) ENGINE = InnoDB
  AUTO_INCREMENT = 2
  DEFAULT CHARSET = utf8mb4
  COLLATE = utf8mb4_0900_ai_ci;

truncate table mysql_cdc_e2e_source_table;