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

-- Tables for the snapshot split E2E test of a table without a primary key whose only unique key
-- is nullable. Rows with NULL in the unique key must still be read by the snapshot.

CREATE DATABASE IF NOT EXISTS `mysql_cdc`;

use mysql_cdc;

DROP TABLE IF EXISTS nullable_uk_src;
CREATE TABLE nullable_uk_src
(
    `id`   int         NOT NULL,
    `code` int         DEFAULT NULL,
    `name` varchar(32) DEFAULT NULL,
    UNIQUE KEY `uk_code` (`code`)
) ENGINE = InnoDB;

DROP TABLE IF EXISTS nullable_uk_sink;
CREATE TABLE nullable_uk_sink
(
    `id`   int         NOT NULL,
    `code` int         DEFAULT NULL,
    `name` varchar(32) DEFAULT NULL,
    PRIMARY KEY (`id`)
) ENGINE = InnoDB;

-- 20 rows with a unique code and 10 rows with a NULL code
INSERT INTO nullable_uk_src (id, code, name)
VALUES
    (1, 1, 'row-1'),
    (2, 2, 'row-2'),
    (3, 3, 'row-3'),
    (4, 4, 'row-4'),
    (5, 5, 'row-5'),
    (6, 6, 'row-6'),
    (7, 7, 'row-7'),
    (8, 8, 'row-8'),
    (9, 9, 'row-9'),
    (10, 10, 'row-10'),
    (11, 11, 'row-11'),
    (12, 12, 'row-12'),
    (13, 13, 'row-13'),
    (14, 14, 'row-14'),
    (15, 15, 'row-15'),
    (16, 16, 'row-16'),
    (17, 17, 'row-17'),
    (18, 18, 'row-18'),
    (19, 19, 'row-19'),
    (20, 20, 'row-20'),
    (21, NULL, 'row-21'),
    (22, NULL, 'row-22'),
    (23, NULL, 'row-23'),
    (24, NULL, 'row-24'),
    (25, NULL, 'row-25'),
    (26, NULL, 'row-26'),
    (27, NULL, 'row-27'),
    (28, NULL, 'row-28'),
    (29, NULL, 'row-29'),
    (30, NULL, 'row-30');
