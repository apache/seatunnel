--
-- Licensed to the Apache Software Foundation (ASF) under one or more
-- contributor license agreements.  See the NOTICE file distributed with
-- this work for additional information regarding copyright ownership.
-- The ASF licenses this file to You under the Apache License, Version 2.0
-- (the "License"); you may not use this file except in compliance with
-- the License.  You may obtain a copy of the License at
--
--    http://www.apache.org/licenses/LICENSE-2.0
--
-- Unless required by applicable law or agreed to in writing, software
-- distributed under the License is distributed on an "AS IS" BASIS,
-- WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
-- See the License for the specific language governing permissions and
-- limitations under the License.


CREATE TABLE IF NOT EXISTS public.postgres_cdc_e2e_source_table (
   gid SERIAL PRIMARY KEY,
   text_col TEXT,
   varchar_col VARCHAR(255),
   char_col CHAR(10),
   boolean_col bool,
   smallint_col int2,
   integer_col int4,
   bigint_col BIGINT,
   decimal_col DECIMAL(10, 2),
   numeric_col NUMERIC(8, 4),
   real_col float4,
   double_precision_col float8,
   smallserial_col SMALLSERIAL,
   serial_col SERIAL,
   bigserial_col BIGSERIAL,
   date_col DATE,
   timestamp_col TIMESTAMP,
   bpchar_col BPCHAR(10),
   age INT NOT null,
   name VARCHAR(255) NOT null
 );

alter table public.postgres_cdc_e2e_source_table REPLICA IDENTITY FULL;

CREATE TABLE IF NOT EXISTS public.postgres_cdc_e2e_sink_table (
   gid SERIAL PRIMARY KEY,
   text_col TEXT,
   varchar_col VARCHAR(255),
   char_col CHAR(10),
   boolean_col bool,
   smallint_col int2,
   integer_col int4,
   bigint_col BIGINT,
   decimal_col DECIMAL(10, 2),
   numeric_col NUMERIC(8, 4),
   real_col float4,
   double_precision_col float8,
   smallserial_col SMALLSERIAL,
   serial_col SERIAL,
   bigserial_col BIGSERIAL,
   date_col DATE,
   timestamp_col TIMESTAMP,
   bpchar_col BPCHAR(10),
   age INT NOT null,
   name VARCHAR(255) NOT null
 );

truncate  public.postgres_cdc_e2e_source_table;
truncate  public.postgres_cdc_e2e_sink_table;

INSERT INTO public.postgres_cdc_e2e_source_table (gid,
                             text_col,
                             varchar_col,
                             char_col,
                             boolean_col,
                             smallint_col,
                             integer_col,
                             bigint_col,
                             decimal_col,
                             numeric_col,
                             real_col,
                             double_precision_col,
                             smallserial_col,
                             serial_col,
                             bigserial_col,
                             date_col,
                             timestamp_col,
                             bpchar_col,
                             age,
                             name
                           )
                         VALUES
                           (
                             '1',
                             'Hello World',
                             'Test',
                             'Testing',
                             true,
                             10,
                             100,
                             1000,
                             10.55,
                             8.8888,
                             3.14,
                             3.14159265,
                             1,
                             100,
                             10000,
                             '2023-05-07',
                             '2023-05-07 14:30:00',
                             'Testing',
                             21,
                             'Leblanc');

INSERT INTO public.postgres_cdc_e2e_source_table (gid,
                             text_col,
                             varchar_col,
                             char_col,
                             boolean_col,
                             smallint_col,
                             integer_col,
                             bigint_col,
                             decimal_col,
                             numeric_col,
                             real_col,
                             double_precision_col,
                             smallserial_col,
                             serial_col,
                             bigserial_col,
                             date_col,
                             timestamp_col,
                             bpchar_col,
                             age,
                             name
                           )
                         VALUES
                           (
                             '2',
                             'Hello World',
                             'Test',
                             'Testing',
                             true,
                             10,
                             100,
                             1000,
                             10.55,
                             8.8888,
                             3.14,
                             3.14159265,
                             1,
                             100,
                             10000,
                             '2023-05-07',
                             '2023-05-07 14:30:00',
                             'Testing',
                             21,
                             'Leblanc');

INSERT INTO public.postgres_cdc_e2e_source_table (gid,
                             text_col,
                             varchar_col,
                             char_col,
                             boolean_col,
                             smallint_col,
                             integer_col,
                             bigint_col,
                             decimal_col,
                             numeric_col,
                             real_col,
                             double_precision_col,
                             smallserial_col,
                             serial_col,
                             bigserial_col,
                             date_col,
                             timestamp_col,
                             bpchar_col,
                             age,
                             name
                           )
                         VALUES
                           (
                             '3',
                             'Hello World',
                             'Test',
                             'Testing',
                             true,
                             10,
                             100,
                             1000,
                             10.55,
                             8.8888,
                             3.14,
                             3.14159265,
                             1,
                             100,
                             10000,
                             '2023-05-07',
                             '2023-05-07 14:30:00',
                             'Testing',
                             21,
                             'Leblanc');
