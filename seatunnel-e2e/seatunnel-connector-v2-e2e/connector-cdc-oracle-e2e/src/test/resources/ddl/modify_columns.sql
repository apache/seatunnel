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
-- Set session timezone to fixed Asia/Shanghai for checking TIMESTAMP_LTZ type
-- ALTER SESSION SET TIME_ZONE='Asia/Shanghai';

alter table DEBEZIUM.FULL_TYPES modify VAL_VARCHAR VARCHAR2(2048);

delete from DEBEZIUM.FULL_TYPES where ID < 13;
INSERT INTO DEBEZIUM.FULL_TYPES
VALUES (16, 'vc7', 'vc7', 'nvc7', 'c', 'nc',1.1, 2.22, 3.33, 8.888, 4.4444, 5.555, 6.66, 1234.567891, 1234.567891, 77.323,1, 22, 333, 4444, 5555, 1, 99, 9999, 999999999, 999999999999999999,94, 9949, 999999994, 999999999999999949, 99999999999999999999999999999999999949,
        TO_DATE('2022-11-02', 'yyyy-mm-dd'),TO_TIMESTAMP('2022-11-02 12:34:56.00789', 'yyyy-mm-dd HH24:MI:SS.FF5'),TO_TIMESTAMP('2022-11-02 12:34:56.12545', 'yyyy-mm-dd HH24:MI:SS.FF5'),
        TO_TIMESTAMP('2022-11-02 12:34:56.12545', 'yyyy-mm-dd HH24:MI:SS.FF5'),TO_TIMESTAMP('2022-11-02 12:34:56.125456789', 'yyyy-mm-dd HH24:MI:SS.FF9'),TO_TIMESTAMP_TZ('2022-11-02 01:34:56.00789', 'yyyy-mm-dd HH24:MI:SS.FF5'),
        1
       );
INSERT INTO DEBEZIUM.FULL_TYPES
VALUES (17, 'vc8', 'vc8', 'nvc8', 'c', 'nc',1.1, 2.22, 3.33, 8.888, 4.4444, 5.555, 6.66, 1234.567891, 1234.567891, 77.323,1, 22, 333, 4444, 5555, 1, 99, 9999, 999999999, 999999999999999999,94, 9949, 999999994, 999999999999999949, 99999999999999999999999999999999999949,
        TO_DATE('2022-11-03', 'yyyy-mm-dd'),TO_TIMESTAMP('2022-11-03 12:34:56.00789', 'yyyy-mm-dd HH24:MI:SS.FF5'),TO_TIMESTAMP('2022-11-03 12:34:56.12545', 'yyyy-mm-dd HH24:MI:SS.FF5'),
        TO_TIMESTAMP('2022-11-03 12:34:56.12545', 'yyyy-mm-dd HH24:MI:SS.FF5'),TO_TIMESTAMP('2022-11-03 12:34:56.125456789', 'yyyy-mm-dd HH24:MI:SS.FF9'),TO_TIMESTAMP_TZ('2022-11-03 01:34:56.00789', 'yyyy-mm-dd HH24:MI:SS.FF5'),
        1
       );
