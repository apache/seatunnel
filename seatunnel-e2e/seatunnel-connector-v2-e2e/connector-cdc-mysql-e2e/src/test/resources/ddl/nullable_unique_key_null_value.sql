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

-- Tables for the E2E test of NULL values in the nullable unique key of a table without a primary
-- key (single and composite key), with two controls: a table with a real primary key and a table
-- whose unique key is NOT NULL. The sink tables have no unique key, so NULLs can be compared.

CREATE DATABASE IF NOT EXISTS `mysql_cdc`;
CREATE DATABASE IF NOT EXISTS `mysql_cdc_null_sink`;

use mysql_cdc;

DROP TABLE IF EXISTS uk_null_single;
CREATE TABLE uk_null_single
(
    `id`   int         NOT NULL,
    `code` int         DEFAULT NULL,
    `name` varchar(32) DEFAULT NULL,
    UNIQUE KEY `uk_code` (`code`)
) ENGINE = InnoDB;

DROP TABLE IF EXISTS uk_null_composite;
CREATE TABLE uk_null_composite
(
    `id`   int         NOT NULL,
    `a`    int         NOT NULL,
    `b`    int         DEFAULT NULL,
    `name` varchar(32) DEFAULT NULL,
    UNIQUE KEY `uk_ab` (`a`, `b`)
) ENGINE = InnoDB;

INSERT INTO uk_null_single (id, code, name)
VALUES (1, 1, 'snapshot-coded'), (2, NULL, 'snapshot-null'), (3, NULL, 'snapshot-null');

INSERT INTO uk_null_composite (id, a, b, name)
VALUES (1, 1, 1, 'snapshot-coded'), (2, 1, NULL, 'snapshot-null'), (3, 2, NULL, 'snapshot-null');

DROP TABLE IF EXISTS pk_null_control;
CREATE TABLE pk_null_control
(
    `id`   int         NOT NULL,
    `code` int         DEFAULT NULL,
    `name` varchar(32) DEFAULT NULL,
    PRIMARY KEY (`id`),
    UNIQUE KEY `uk_code` (`code`)
) ENGINE = InnoDB;

DROP TABLE IF EXISTS uk_notnull_control;
CREATE TABLE uk_notnull_control
(
    `id`   int         NOT NULL,
    `code` int         NOT NULL,
    `name` varchar(32) DEFAULT NULL,
    UNIQUE KEY `uk_code` (`code`)
) ENGINE = InnoDB;

INSERT INTO pk_null_control (id, code, name)
VALUES (1, 1, 'snapshot-coded'), (2, NULL, 'snapshot-null'), (3, NULL, 'snapshot-null');

INSERT INTO uk_notnull_control (id, code, name)
VALUES (1, 1, 'snapshot-coded'), (2, 0, 'snapshot-zero'), (3, 3, 'snapshot-coded');

use mysql_cdc_null_sink;

DROP TABLE IF EXISTS uk_null_single;
CREATE TABLE uk_null_single
(
    `id`   int         NOT NULL,
    `code` int         DEFAULT NULL,
    `name` varchar(32) DEFAULT NULL,
    PRIMARY KEY (`id`)
) ENGINE = InnoDB;

DROP TABLE IF EXISTS uk_null_composite;
CREATE TABLE uk_null_composite
(
    `id`   int         NOT NULL,
    `a`    int         NOT NULL,
    `b`    int         DEFAULT NULL,
    `name` varchar(32) DEFAULT NULL,
    PRIMARY KEY (`id`)
) ENGINE = InnoDB;

DROP TABLE IF EXISTS pk_null_control;
CREATE TABLE pk_null_control
(
    `id`   int         NOT NULL,
    `code` int         DEFAULT NULL,
    `name` varchar(32) DEFAULT NULL,
    PRIMARY KEY (`id`)
) ENGINE = InnoDB;

DROP TABLE IF EXISTS uk_notnull_control;
CREATE TABLE uk_notnull_control
(
    `id`   int         NOT NULL,
    `code` int         NOT NULL,
    `name` varchar(32) DEFAULT NULL,
    PRIMARY KEY (`id`)
) ENGINE = InnoDB;
