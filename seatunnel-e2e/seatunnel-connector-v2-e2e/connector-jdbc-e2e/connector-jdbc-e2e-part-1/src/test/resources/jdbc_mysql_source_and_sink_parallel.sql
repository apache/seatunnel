--
-- Licensed to the Apache Software Foundation (ASF) under one or more
-- contributor license agreements.  See the NOTICE file distributed with
-- this work for additional information regarding copyright ownership.
-- The ASF licenses this file to You under the Apache License, Version 2.0
-- (the "License"); you may not use this file except in compliance with
-- the License.  You may obtain a copy of the License at
--
-- http://www.apache.org/licenses/LICENSE-2.0
--
-- Unless required by applicable law or agreed to in writing, software
-- distributed under the License is distributed on an "AS IS" BASIS,
-- WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
-- See the License for the specific language governing permissions and
-- limitations under the License.
--

/* config
env {
  parallelism = 3
  job.mode = "BATCH"
}
*/

CREATE TABLE source_table WITH (
  'connector'='jdbc',
  'url' = 'jdbc:mysql://mysql-e2e:3306/seatunnel',
  'driver' = 'com.mysql.cj.jdbc.Driver',
  'user' = 'root',
  'password' = 'Abc!@#135_seatunnel',
  'query' = 'select * from source',
  'partition_column' = 'c_decimal_unsigned_30',
  'partition_num' = '3',
  'type'='source'
);

CREATE TABLE temp1 AS
    SELECT c_bit_1, c_bit_8, c_bit_16, c_bit_32, c_bit_64, c_boolean, c_tinyint, c_tinyint_unsigned, c_smallint, c_smallint_unsigned,
           c_mediumint, c_mediumint_unsigned, c_int, c_integer, c_bigint, c_bigint_unsigned,
           c_decimal, c_decimal_unsigned, c_float, c_float_unsigned, c_double, c_double_unsigned,
           c_char, c_tinytext, c_mediumtext, c_text, c_varchar, c_json, c_longtext, c_date,
           c_datetime, c_time, c_timestamp, c_tinyblob, c_mediumblob, c_blob, c_longblob, c_varbinary,
           c_binary, c_year, c_int_unsigned, c_integer_unsigned,c_bigint_30,c_decimal_unsigned_30,c_decimal_30 FROM source_table;
 
CREATE TABLE sink_table WITH (
  'connector'='jdbc',
  'url' = 'jdbc:mysql://mysql-e2e:3306/seatunnel',
  'driver' = 'com.mysql.cj.jdbc.Driver',
  'user' = 'root',
  'password' = 'Abc!@#135_seatunnel',
  'connection_check_timeout_sec' = '100',
  'generate_sink_sql' = 'true',
  'database' = 'seatunnel',
  'table' = 'sink',
  'source_table_name' = 'temp1',
  'type'='sink'
);