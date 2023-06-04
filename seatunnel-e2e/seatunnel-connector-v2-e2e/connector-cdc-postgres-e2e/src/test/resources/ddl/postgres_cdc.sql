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

CREATE TABLE public.postgres_cdc_e2e_source_table (
	id int4 NOT NULL,
	name varchar NULL,
	CONSTRAINT postgres_cdc_e2e_source_table_pk PRIMARY KEY (id)
);

alter table public.postgres_cdc_e2e_source_table REPLICA IDENTITY FULL;

CREATE TABLE public.postgres_cdc_e2e_sink_table (
id int4 NOT NULL,
	name varchar NULL,
	CONSTRAINT postgres_cdc_e2e_sink_table_pk PRIMARY KEY (id)
);

truncate  public.postgres_cdc_e2e_source_table;
truncate  public.postgres_cdc_e2e_sink_table;

INSERT INTO public.postgres_cdc_e2e_source_table
(id, name)
VALUES(1, '332323');
