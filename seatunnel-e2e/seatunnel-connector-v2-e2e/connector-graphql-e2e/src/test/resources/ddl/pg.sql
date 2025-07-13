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

-- DROP SCHEMA IF EXISTS test CASCADE;
-- CREATE SCHEMA test;
-- SET search_path TO test;

CREATE TABLE source
(
    id                  SERIAL PRIMARY KEY,
    val_bool            BOOLEAN,
    val_int8            SMALLINT,
    val_int16           SMALLINT,
    val_int32           INTEGER,
    val_int64           BIGINT,
    val_float           REAL,
    val_double          DOUBLE PRECISION,
    val_decimal         NUMERIC,
    val_string          VARCHAR(255),
    val_unixtime_micros TIMESTAMP
);

CREATE TABLE sink
(
    id                  INTEGER,
    val_bool            BOOLEAN,
    val_int8            SMALLINT,
    val_int16           SMALLINT,
    val_int32           INTEGER,
    val_int64           BIGINT,
    val_float           REAL,
    val_double          DOUBLE PRECISION,
    val_decimal         NUMERIC,
    val_string          VARCHAR(255),
    val_unixtime_micros TIMESTAMP
);

INSERT INTO source (val_bool, val_int8, val_int16, val_int32, val_int64, val_float, val_double, val_decimal, val_string,
                    val_unixtime_micros)
VALUES (TRUE, 1, 2, 3, 4, 4.3, 5.3, 6.3, 'NEW', '2020-02-02 02:02:02'),
       (FALSE, 0, 4, 5, 6, 7.3, 8.3, 9.3, 'OLD', '2020-02-03 03:03:03');
