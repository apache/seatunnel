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

CREATE TABLE postgres_cdc_e2e_source_table
(
    id                  INTEGER NOT NULL,
    bytea_c             BYTEA,
    small_c             SMALLINT,
    int_c               INTEGER,
    big_c               BIGINT,
    real_c              REAL,
    double_precision    DOUBLE PRECISION,
    numeric_c           NUMERIC(10, 5),
    decimal_c           DECIMAL(10, 1),
    boolean_c           BOOLEAN,
    text_c              TEXT,
    char_c              CHAR,
    character_c         CHARACTER(3),
    character_varying_c CHARACTER VARYING(20),
    timestamp3_c        TIMESTAMP(3),
    timestamp6_c        TIMESTAMP(6),
    date_c              DATE,
    time_c              TIME(0),
    default_numeric_c   NUMERIC,
    geometry_c          GEOMETRY(POINT, 3187),
    geography_c         GEOGRAPHY(MULTILINESTRING),
    PRIMARY KEY (id)
);

CREATE TABLE postgres_cdc_e2e_sink_table
(
    id                  INTEGER NOT NULL,
    bytea_c             BYTEA,
    small_c             SMALLINT,
    int_c               INTEGER,
    big_c               BIGINT,
    real_c              REAL,
    double_precision    DOUBLE PRECISION,
    numeric_c           NUMERIC(10, 5),
    decimal_c           DECIMAL(10, 1),
    boolean_c           BOOLEAN,
    text_c              TEXT,
    char_c              CHAR,
    character_c         CHARACTER(3),
    character_varying_c CHARACTER VARYING(20),
    timestamp3_c        TIMESTAMP(3),
    timestamp6_c        TIMESTAMP(6),
    date_c              DATE,
    time_c              TIME(0),
    default_numeric_c   NUMERIC,
    geometry_c          GEOMETRY(POINT, 3187),
    geography_c         GEOGRAPHY(MULTILINESTRING),
    PRIMARY KEY (id)
);

ALTER TABLE postgres_cdc_e2e_source_table
    REPLICA IDENTITY FULL;

ALTER TABLE postgres_cdc_e2e_sink_table
    REPLICA IDENTITY FULL;

INSERT INTO postgres_cdc_e2e_source_table
VALUES (1, '2', 32767, 65535, 2147483647, 5.5, 6.6, 123.12345, 404.4443, true,
        'Hello World', 'a', 'abc', 'abcd..xyz', '2020-07-17 18:00:22.123', '2020-07-17 18:00:22.123456',
        '2020-07-17', '18:00:22', 500, 'SRID=3187;POINT(174.9479 -36.7208)'::geometry,
        'MULTILINESTRING((169.1321 -44.7032, 167.8974 -44.6414))'::geography);