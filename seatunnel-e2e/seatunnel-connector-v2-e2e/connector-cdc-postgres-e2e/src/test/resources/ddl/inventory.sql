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

-- Create and populate our products using a single insert with many rows

DROP SCHEMA IF EXISTS inventory CASCADE;
CREATE SCHEMA inventory;
SET search_path TO inventory;
CREATE EXTENSION postgis;

CREATE TABLE postgres_cdc_table_1
(
    id                  INTEGER NOT NULL,
    f_bytea             BYTEA,
    f_small             SMALLINT,
    f_int               INTEGER,
    f_big               BIGINT,
    f_real              REAL,
    f_double_precision  DOUBLE PRECISION,
    f_numeric           NUMERIC(10, 5),
    f_decimal           DECIMAL(10, 1),
    f_boolean           BOOLEAN,
    f_text              TEXT,
    f_char              CHAR,
    f_character         CHARACTER(3),
    f_character_varying CHARACTER VARYING(20),
    f_timestamp3        TIMESTAMP(3),
    f_timestamp6        TIMESTAMP(6),
    f_date              DATE,
    f_time              TIME(0),
    f_default_numeric   NUMERIC,
    f_inet              INET,
    PRIMARY KEY (id)
);

CREATE TABLE postgres_cdc_table_2
(
    id                  INTEGER NOT NULL,
    f_bytea             BYTEA,
    f_small             SMALLINT,
    f_int               INTEGER,
    f_big               BIGINT,
    f_real              REAL,
    f_double_precision  DOUBLE PRECISION,
    f_numeric           NUMERIC(10, 5),
    f_decimal           DECIMAL(10, 1),
    f_boolean           BOOLEAN,
    f_text              TEXT,
    f_char              CHAR,
    f_character         CHARACTER(3),
    f_character_varying CHARACTER VARYING(20),
    f_timestamp3        TIMESTAMP(3),
    f_timestamp6        TIMESTAMP(6),
    f_date              DATE,
    f_time              TIME(0),
    f_default_numeric   NUMERIC,
    f_inet              INET,
    PRIMARY KEY (id)
);

CREATE TABLE sink_postgres_cdc_table_1
(
    id                  INTEGER NOT NULL,
    f_bytea             BYTEA,
    f_small             SMALLINT,
    f_int               INTEGER,
    f_big               BIGINT,
    f_real              REAL,
    f_double_precision  DOUBLE PRECISION,
    f_numeric           NUMERIC(10, 5),
    f_decimal           DECIMAL(10, 1),
    f_boolean           BOOLEAN,
    f_text              TEXT,
    f_char              CHAR,
    f_character         CHARACTER(3),
    f_character_varying CHARACTER VARYING(20),
    f_timestamp3        TIMESTAMP(3),
    f_timestamp6        TIMESTAMP(6),
    f_date              DATE,
    f_time              TIME(0),
    f_default_numeric   NUMERIC,
    f_inet              INET,
    PRIMARY KEY (id)
);

CREATE TABLE sink_postgres_cdc_table_2
(
    id                  INTEGER NOT NULL,
    f_bytea             BYTEA,
    f_small             SMALLINT,
    f_int               INTEGER,
    f_big               BIGINT,
    f_real              REAL,
    f_double_precision  DOUBLE PRECISION,
    f_numeric           NUMERIC(10, 5),
    f_decimal           DECIMAL(10, 1),
    f_boolean           BOOLEAN,
    f_text              TEXT,
    f_char              CHAR,
    f_character         CHARACTER(3),
    f_character_varying CHARACTER VARYING(20),
    f_timestamp3        TIMESTAMP(3),
    f_timestamp6        TIMESTAMP(6),
    f_date              DATE,
    f_time              TIME(0),
    f_default_numeric   NUMERIC,
    f_inet              INET,
    PRIMARY KEY (id)
);

CREATE TABLE full_types_no_primary_key
(
    id                  INTEGER NOT NULL,
    f_bytea             BYTEA,
    f_small             SMALLINT,
    f_int               INTEGER,
    f_big               BIGINT,
    f_real              REAL,
    f_double_precision  DOUBLE PRECISION,
    f_numeric           NUMERIC(10, 5),
    f_decimal           DECIMAL(10, 1),
    f_boolean           BOOLEAN,
    f_text              TEXT,
    f_char              CHAR,
    f_character         CHARACTER(3),
    f_character_varying CHARACTER VARYING(20),
    f_timestamp3        TIMESTAMP(3),
    f_timestamp6        TIMESTAMP(6),
    f_date              DATE,
    f_time              TIME(0),
    f_default_numeric   NUMERIC,
    f_inet              INET
);

CREATE TABLE postgres_cdc_table_3
(
    id                  INTEGER NOT NULL,
    f_bytea             BYTEA,
    f_small             SMALLINT,
    f_int               INTEGER,
    PRIMARY KEY (id)
);

CREATE TABLE sink_postgres_cdc_table_3
(
    id                  INTEGER NOT NULL,
    f_bytea             BYTEA,
    f_small             SMALLINT,
    f_int               INTEGER,
    PRIMARY KEY (id)
);

ALTER TABLE postgres_cdc_table_1
    REPLICA IDENTITY FULL;

ALTER TABLE postgres_cdc_table_2
    REPLICA IDENTITY FULL;

ALTER TABLE postgres_cdc_table_3
    REPLICA IDENTITY FULL;

ALTER TABLE sink_postgres_cdc_table_1
    REPLICA IDENTITY FULL;

ALTER TABLE sink_postgres_cdc_table_2
    REPLICA IDENTITY FULL;

ALTER TABLE full_types_no_primary_key
    REPLICA IDENTITY FULL;

INSERT INTO postgres_cdc_table_1
VALUES (1, '2', 32767, 65535, 2147483647, 5.5, 6.6, 123.12345, 404.4443, true,
        'Hello World', 'a', 'abc', 'abcd..xyz', '2020-07-17 18:00:22.123', '2020-07-17 18:00:22.123456',
        '2020-07-17', '18:00:22', 500,'192.168.1.1');

INSERT INTO postgres_cdc_table_2
VALUES (1, '2', 32767, 65535, 2147483647, 5.5, 6.6, 123.12345, 404.4443, true,
        'Hello World', 'a', 'abc', 'abcd..xyz', '2020-07-17 18:00:22.123', '2020-07-17 18:00:22.123456',
        '2020-07-17', '18:00:22', 500,'192.168.1.1');

INSERT INTO postgres_cdc_table_3
VALUES (1, '2', 32767, 65535);

INSERT INTO full_types_no_primary_key
VALUES (1, '2', 32767, 65535, 2147483647, 5.5, 6.6, 123.12345, 404.4443, true,
        'Hello World', 'a', 'abc', 'abcd..xyz', '2020-07-17 18:00:22.123', '2020-07-17 18:00:22.123456',
        '2020-07-17', '18:00:22', 500,'192.168.1.1');
