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

-- ----------------------------------------------------------------------------------------------------------------
-- DATABASE:  column_type_test
-- ----------------------------------------------------------------------------------------------------------------
-- Create the column_type_test database
CREATE DATABASE column_type_test;

USE column_type_test;
EXEC sys.sp_cdc_enable_db;

CREATE TYPE UDTDECIMAL FROM decimal(12, 2);

CREATE TABLE full_types (
    id int NOT NULL,
    val_char char(3),
    val_varchar varchar(1000),
    val_text text,
    val_nchar nchar(3),
    val_nvarchar nvarchar(1000),
    val_ntext ntext,
    val_decimal decimal(6,3),
    val_numeric numeric,
    val_float float,
    val_real real,
    val_smallmoney smallmoney,
    val_money money,
    val_bit bit,
    val_tinyint tinyint,
    val_smallint smallint,
    val_int int,
    val_bigint bigint,
    val_date date,
    val_time time,
    val_datetime2 datetime2,
    val_datetime datetime,
    val_smalldatetime smalldatetime,
    val_xml xml,
    val_datetimeoffset DATETIMEOFFSET(4),
    val_varbinary  varbinary(100),
    val_udtdecimal UDTDECIMAL,
    PRIMARY KEY (id)
);
INSERT INTO full_types VALUES (0,
                               'cč0', 'vcč', 'tč', N'cč', N'vcč', N'tč',
                               1.123, 2, 3.323, 4.323, 5.323, 6.323,
                               1, 22, 333, 4444, 55555,
                               '2018-07-13', '10:23:45', '2018-07-13 11:23:45.34', '2018-07-13 13:23:45.78', '2018-07-13 14:23:45',
                               '<a>b</a>',SYSDATETIMEOFFSET(),CAST('test_varbinary' AS varbinary(100)), 5.32);
INSERT INTO full_types VALUES (1,
                               'cč1', 'vcč', 'tč', N'cč', N'vcč', N'tč',
                               1.123, 2, 3.323, 4.323, 5.323, 6.323,
                               1, 22, 333, 4444, 55555,
                               '2018-07-13', '10:23:45', '2018-07-13 11:23:45.34', '2018-07-13 13:23:45.78', '2018-07-13 14:23:45',
                               '<a>b</a>',SYSDATETIMEOFFSET(),CAST('test_varbinary' AS varbinary(100)), 5.32);
INSERT INTO full_types VALUES (2,
                               'cč2', 'vcč', 'tč', N'cč', N'vcč', N'tč',
                               1.123, 2, 3.323, 4.323, 5.323, 6.323,
                               1, 22, 333, 4444, 55555,
                               '2018-07-13', '10:23:45', '2018-07-13 11:23:45.34', '2018-07-13 13:23:45.78', '2018-07-13 14:23:45',
                               '<a>b</a>',SYSDATETIMEOFFSET(),CAST('test_varbinary' AS varbinary(100)), 5.32);
EXEC sys.sp_cdc_enable_table @source_schema = 'dbo', @source_name = 'full_types', @role_name = NULL, @supports_net_changes = 0;

CREATE TABLE full_types_no_primary_key (
                            id int NOT NULL,
                            val_char char(3),
                            val_varchar varchar(1000),
                            val_text text,
                            val_nchar nchar(3),
                            val_nvarchar nvarchar(1000),
                            val_ntext ntext,
                            val_decimal decimal(6,3),
                            val_numeric numeric,
                            val_float float,
                            val_real real,
                            val_smallmoney smallmoney,
                            val_money money,
                            val_bit bit,
                            val_tinyint tinyint,
                            val_smallint smallint,
                            val_int int,
                            val_bigint bigint,
                            val_date date,
                            val_time time,
                            val_datetime2 datetime2,
                            val_datetime datetime,
                            val_smalldatetime smalldatetime,
                            val_xml xml,
                            val_datetimeoffset DATETIMEOFFSET(4),
                            val_varbinary  varbinary(100),
                            val_udtdecimal UDTDECIMAL
);
INSERT INTO full_types_no_primary_key VALUES (0,
                               'cč0', 'vcč', 'tč', N'cč', N'vcč', N'tč',
                               1.123, 2, 3.323, 4.323, 5.323, 6.323,
                               1, 22, 333, 4444, 55555,
                               '2018-07-13', '10:23:45', '2018-07-13 11:23:45.34', '2018-07-13 13:23:45.78', '2018-07-13 14:23:45',
                               '<a>b</a>',SYSDATETIMEOFFSET(),CAST('test_varbinary' AS varbinary(100)), 5.32);
INSERT INTO full_types_no_primary_key VALUES (1,
                               'cč1', 'vcč', 'tč', N'cč', N'vcč', N'tč',
                               1.123, 2, 3.323, 4.323, 5.323, 6.323,
                               1, 22, 333, 4444, 55555,
                               '2018-07-13', '10:23:45', '2018-07-13 11:23:45.34', '2018-07-13 13:23:45.78', '2018-07-13 14:23:45',
                               '<a>b</a>',SYSDATETIMEOFFSET(),CAST('test_varbinary' AS varbinary(100)), 5.32);
INSERT INTO full_types_no_primary_key VALUES (2,
                               'cč2', 'vcč', 'tč', N'cč', N'vcč', N'tč',
                               1.123, 2, 3.323, 4.323, 5.323, 6.323,
                               1, 22, 333, 4444, 55555,
                               '2018-07-13', '10:23:45', '2018-07-13 11:23:45.34', '2018-07-13 13:23:45.78', '2018-07-13 14:23:45',
                               '<a>b</a>',SYSDATETIMEOFFSET(),CAST('test_varbinary' AS varbinary(100)), 5.32);
EXEC sys.sp_cdc_enable_table @source_schema = 'dbo', @source_name = 'full_types_no_primary_key', @role_name = NULL, @supports_net_changes = 0;

CREATE TABLE full_types_custom_primary_key (
                                           id int NOT NULL,
                                           val_char char(3),
                                           val_varchar varchar(1000),
                                           val_text text,
                                           val_nchar nchar(3),
                                           val_nvarchar nvarchar(1000),
                                           val_ntext ntext,
                                           val_decimal decimal(6,3),
                                           val_numeric numeric,
                                           val_float float,
                                           val_real real,
                                           val_smallmoney smallmoney,
                                           val_money money,
                                           val_bit bit,
                                           val_tinyint tinyint,
                                           val_smallint smallint,
                                           val_int int,
                                           val_bigint bigint,
                                           val_date date,
                                           val_time time,
                                           val_datetime2 datetime2,
                                           val_datetime datetime,
                                           val_smalldatetime smalldatetime,
                                           val_xml xml,
                                           val_datetimeoffset DATETIMEOFFSET(4),
                                           val_varbinary  varbinary(100),
                                           val_udtdecimal UDTDECIMAL
);
INSERT INTO full_types_custom_primary_key VALUES (0,
                                              'cč0', 'vcč', 'tč', N'cč', N'vcč', N'tč',
                                              1.123, 2, 3.323, 4.323, 5.323, 6.323,
                                              1, 22, 333, 4444, 55555,
                                              '2018-07-13', '10:23:45', '2018-07-13 11:23:45.34', '2018-07-13 13:23:45.78', '2018-07-13 14:23:45',
                                              '<a>b</a>',SYSDATETIMEOFFSET(),CAST('test_varbinary' AS varbinary(100)), 5.32);
INSERT INTO full_types_custom_primary_key VALUES (1,
                                              'cč1', 'vcč', 'tč', N'cč', N'vcč', N'tč',
                                              1.123, 2, 3.323, 4.323, 5.323, 6.323,
                                              1, 22, 333, 4444, 55555,
                                              '2018-07-13', '10:23:45', '2018-07-13 11:23:45.34', '2018-07-13 13:23:45.78', '2018-07-13 14:23:45',
                                              '<a>b</a>',SYSDATETIMEOFFSET(),CAST('test_varbinary' AS varbinary(100)), 5.32);
INSERT INTO full_types_custom_primary_key VALUES (2,
                                              'cč2', 'vcč', 'tč', N'cč', N'vcč', N'tč',
                                              1.123, 2, 3.323, 4.323, 5.323, 6.323,
                                              1, 22, 333, 4444, 55555,
                                              '2018-07-13', '10:23:45', '2018-07-13 11:23:45.34', '2018-07-13 13:23:45.78', '2018-07-13 14:23:45',
                                              '<a>b</a>',SYSDATETIMEOFFSET(),CAST('test_varbinary' AS varbinary(100)), 5.32);
EXEC sys.sp_cdc_enable_table @source_schema = 'dbo', @source_name = 'full_types_custom_primary_key', @role_name = NULL, @supports_net_changes = 0;

CREATE TABLE full_types_sink (
                            id int NOT NULL,
                            val_char char(3),
                            val_varchar varchar(1000),
                            val_text text,
                            val_nchar nchar(3),
                            val_nvarchar nvarchar(1000),
                            val_ntext ntext,
                            val_decimal decimal(6,3),
                            val_numeric numeric,
                            val_float float,
                            val_real real,
                            val_smallmoney smallmoney,
                            val_money money,
                            val_bit bit,
                            val_tinyint tinyint,
                            val_smallint smallint,
                            val_int int,
                            val_bigint bigint,
                            val_date date,
                            val_time time,
                            val_datetime2 datetime2,
                            val_datetime datetime,
                            val_smalldatetime smalldatetime,
                            val_xml xml,
                            val_datetimeoffset DATETIMEOFFSET(4),
                            val_varbinary  varbinary(100),
                            val_udtdecimal UDTDECIMAL,
                            PRIMARY KEY (id)
);
