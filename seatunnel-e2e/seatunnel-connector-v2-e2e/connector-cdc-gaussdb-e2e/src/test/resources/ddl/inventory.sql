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

DROP SCHEMA IF EXISTS inventory CASCADE;
CREATE SCHEMA inventory;
SET search_path TO inventory;

CREATE TABLE gaussdb_cdc_table
(
    id    INTEGER NOT NULL,
    name  CHARACTER VARYING(255),
    f_big BIGINT,
    PRIMARY KEY (id)
);

CREATE TABLE sink_gaussdb_cdc_table
(
    id    INTEGER NOT NULL,
    name  CHARACTER VARYING(255),
    f_big BIGINT,
    PRIMARY KEY (id)
);

ALTER TABLE gaussdb_cdc_table
    REPLICA IDENTITY FULL;

INSERT INTO gaussdb_cdc_table
VALUES (1, 'Hello GaussDB', 100);
