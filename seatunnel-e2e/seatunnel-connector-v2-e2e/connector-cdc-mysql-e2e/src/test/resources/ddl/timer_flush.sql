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

-- Source and sink tables used by engine-level timer flush E2E tests.
-- Uses $DBNAME$ so UniqueDatabase substitutes the actual database name.

USE `$DBNAME$`;

-- ── source ────────────────────────────────────────────────────────────────────
DROP TABLE IF EXISTS `timer_flush_src`;
CREATE TABLE `timer_flush_src` (
    `id`        INT          NOT NULL,
    `f_bigint`  BIGINT,
    `f_varchar` VARCHAR(255),
    PRIMARY KEY (`id`)
) ENGINE = InnoDB DEFAULT CHARSET = utf8mb4;

TRUNCATE TABLE `timer_flush_src`;

INSERT INTO `timer_flush_src` (`id`, `f_bigint`, `f_varchar`) VALUES
    (1, 100, 'row-1'),
    (2, 200, 'row-2'),
    (3, 300, 'row-3');

-- ── sink ──────────────────────────────────────────────────────────────────────
DROP TABLE IF EXISTS `timer_flush_sink`;
CREATE TABLE `timer_flush_sink` (
    `id`        INT          NOT NULL,
    `f_bigint`  BIGINT,
    `f_varchar` VARCHAR(255),
    PRIMARY KEY (`id`)
) ENGINE = InnoDB DEFAULT CHARSET = utf8mb4;

TRUNCATE TABLE `timer_flush_sink`;
