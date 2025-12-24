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

INSERT INTO DEBEZIUM.FULL_TYPES
VALUES (2, 'vc2', 'vc2', 'nvc2', 'c', 'nc',1.1, 2.22, 3.33, 8.888, 4.4444, 5.555, 6.66, 1234.567891, 1234.567891, 77.323,1, 22, 333, 4444, 5555, 1, 99, 9999, 999999999, 999999999999999999,94, 9949, 999999994, 999999999999999949, 99999999999999999999999999999999999949,
        TO_DATE('2022-10-30', 'yyyy-mm-dd'),TO_TIMESTAMP('2022-10-30 12:34:56.00789', 'yyyy-mm-dd HH24:MI:SS.FF5'),TO_TIMESTAMP('2022-10-30 12:34:56.12545', 'yyyy-mm-dd HH24:MI:SS.FF5'),
        TO_TIMESTAMP('2022-10-30 12:34:56.12545', 'yyyy-mm-dd HH24:MI:SS.FF5'),TO_TIMESTAMP('2022-10-30 12:34:56.125456789', 'yyyy-mm-dd HH24:MI:SS.FF9'),TO_TIMESTAMP_TZ('2022-10-30 01:34:56.00789', 'yyyy-mm-dd HH24:MI:SS.FF5')
);
INSERT INTO DEBEZIUM.FULL_TYPES
VALUES (3, 'vc3', 'vc3', 'nvc3', 'c', 'nc',1.1, 2.22, 3.33, 8.888, 4.4444, 5.555, 6.66, 1234.567891, 1234.567891, 77.323,1, 22, 333, 4444, 5555, 1, 99, 9999, 999999999, 999999999999999999,94, 9949, 999999994, 999999999999999949, 99999999999999999999999999999999999949,
        TO_DATE('2022-10-31', 'yyyy-mm-dd'),TO_TIMESTAMP('2022-10-31 12:34:56.00789', 'yyyy-mm-dd HH24:MI:SS.FF5'),TO_TIMESTAMP('2022-10-31 12:34:56.12545', 'yyyy-mm-dd HH24:MI:SS.FF5'),
        TO_TIMESTAMP('2022-10-31 12:34:56.12545', 'yyyy-mm-dd HH24:MI:SS.FF5'),TO_TIMESTAMP('2022-10-31 12:34:56.125456789', 'yyyy-mm-dd HH24:MI:SS.FF9'),TO_TIMESTAMP_TZ('2022-10-31 01:34:56.00789', 'yyyy-mm-dd HH24:MI:SS.FF5')
       );
INSERT INTO DEBEZIUM.FULL_TYPES
VALUES (4, 'vc4', 'vc4', 'nvc4', 'c', 'nc',1.1, 2.22, 3.33, 8.888, 4.4444, 5.555, 6.66, 1234.567891, 1234.567891, 77.323,1, 22, 333, 4444, 5555, 1, 99, 9999, 999999999, 999999999999999999,94, 9949, 999999994, 999999999999999949, 99999999999999999999999999999999999949,
        TO_DATE('2022-11-01', 'yyyy-mm-dd'),TO_TIMESTAMP('2022-11-01 12:34:56.00789', 'yyyy-mm-dd HH24:MI:SS.FF5'),TO_TIMESTAMP('2022-11-01 12:34:56.12545', 'yyyy-mm-dd HH24:MI:SS.FF5'),
        TO_TIMESTAMP('2022-11-01 12:34:56.12545', 'yyyy-mm-dd HH24:MI:SS.FF5'),TO_TIMESTAMP('2022-11-01 12:34:56.125456789', 'yyyy-mm-dd HH24:MI:SS.FF9'),TO_TIMESTAMP_TZ('2022-11-01 01:34:56.00789', 'yyyy-mm-dd HH24:MI:SS.FF5')
       );


update DEBEZIUM.FULL_TYPES set VAL_VARCHAR = 'dailai' where ID = 3;
delete from DEBEZIUM.FULL_TYPES where ID = 2;

alter table DEBEZIUM.FULL_TYPES ADD (ADD_COLUMN1 VARCHAR2(64) default 'yy' not null,ADD_COLUMN2 int default 1 not null);

update DEBEZIUM.FULL_TYPES set VAL_VARCHAR2 = 'dailai' where ID = 3;
INSERT INTO DEBEZIUM.FULL_TYPES
VALUES (5, 'vc5', 'vc5', 'nvc5', 'c', 'nc',1.1, 2.22, 3.33, 8.888, 4.4444, 5.555, 6.66, 1234.567891, 1234.567891, 77.323,1, 22, 333, 4444, 5555, 1, 99, 9999, 999999999, 999999999999999999,94, 9949, 999999994, 999999999999999949, 99999999999999999999999999999999999949,
        TO_DATE('2022-11-02', 'yyyy-mm-dd'),TO_TIMESTAMP('2022-11-02 12:34:56.00789', 'yyyy-mm-dd HH24:MI:SS.FF5'),TO_TIMESTAMP('2022-11-02 12:34:56.12545', 'yyyy-mm-dd HH24:MI:SS.FF5'),
        TO_TIMESTAMP('2022-11-02 12:34:56.12545', 'yyyy-mm-dd HH24:MI:SS.FF5'),TO_TIMESTAMP('2022-11-02 12:34:56.125456789', 'yyyy-mm-dd HH24:MI:SS.FF9'),TO_TIMESTAMP_TZ('2022-11-02 01:34:56.00789', 'yyyy-mm-dd HH24:MI:SS.FF5'),
        'yy5', 1
       );
INSERT INTO DEBEZIUM.FULL_TYPES
VALUES (6, 'vc6', 'vc6', 'nvc6', 'c', 'nc',1.1, 2.22, 3.33, 8.888, 4.4444, 5.555, 6.66, 1234.567891, 1234.567891, 77.323,1, 22, 333, 4444, 5555, 1, 99, 9999, 999999999, 999999999999999999,94, 9949, 999999994, 999999999999999949, 99999999999999999999999999999999999949,
        TO_DATE('2022-11-03', 'yyyy-mm-dd'),TO_TIMESTAMP('2022-11-03 12:34:56.00789', 'yyyy-mm-dd HH24:MI:SS.FF5'),TO_TIMESTAMP('2022-11-03 12:34:56.12545', 'yyyy-mm-dd HH24:MI:SS.FF5'),
        TO_TIMESTAMP('2022-11-03 12:34:56.12545', 'yyyy-mm-dd HH24:MI:SS.FF5'),TO_TIMESTAMP('2022-11-03 12:34:56.125456789', 'yyyy-mm-dd HH24:MI:SS.FF9'),TO_TIMESTAMP_TZ('2022-11-03 01:34:56.00789', 'yyyy-mm-dd HH24:MI:SS.FF5'),
        'yy6', 1
       );
delete from DEBEZIUM.FULL_TYPES where ID = 5;

alter table DEBEZIUM.FULL_TYPES ADD ADD_COLUMN3 float default 1.1 not null ;
alter table DEBEZIUM.FULL_TYPES ADD ADD_COLUMN4 timestamp default current_timestamp not null ;

delete from DEBEZIUM.FULL_TYPES where ID = 3;
INSERT INTO DEBEZIUM.FULL_TYPES
VALUES (7, 'vc7', 'vc7', 'nvc7', 'c', 'nc',1.1, 2.22, 3.33, 8.888, 4.4444, 5.555, 6.66, 1234.567891, 1234.567891, 77.323,1, 22, 333, 4444, 5555, 1, 99, 9999, 999999999, 999999999999999999,94, 9949, 999999994, 999999999999999949, 99999999999999999999999999999999999949,
        TO_DATE('2022-11-02', 'yyyy-mm-dd'),TO_TIMESTAMP('2022-11-02 12:34:56.00789', 'yyyy-mm-dd HH24:MI:SS.FF5'),TO_TIMESTAMP('2022-11-02 12:34:56.12545', 'yyyy-mm-dd HH24:MI:SS.FF5'),
        TO_TIMESTAMP('2022-11-02 12:34:56.12545', 'yyyy-mm-dd HH24:MI:SS.FF5'),TO_TIMESTAMP('2022-11-02 12:34:56.125456789', 'yyyy-mm-dd HH24:MI:SS.FF9'),TO_TIMESTAMP_TZ('2022-11-02 01:34:56.00789', 'yyyy-mm-dd HH24:MI:SS.FF5'),
        'yy7', 1, 1.1, TO_TIMESTAMP('2022-11-02 12:34:56.12545', 'yyyy-mm-dd HH24:MI:SS.FF5')
       );
INSERT INTO DEBEZIUM.FULL_TYPES
VALUES (8, 'vc8', 'vc8', 'nvc8', 'c', 'nc',1.1, 2.22, 3.33, 8.888, 4.4444, 5.555, 6.66, 1234.567891, 1234.567891, 77.323,1, 22, 333, 4444, 5555, 1, 99, 9999, 999999999, 999999999999999999,94, 9949, 999999994, 999999999999999949, 99999999999999999999999999999999999949,
        TO_DATE('2022-11-03', 'yyyy-mm-dd'),TO_TIMESTAMP('2022-11-03 12:34:56.00789', 'yyyy-mm-dd HH24:MI:SS.FF5'),TO_TIMESTAMP('2022-11-03 12:34:56.12545', 'yyyy-mm-dd HH24:MI:SS.FF5'),
        TO_TIMESTAMP('2022-11-03 12:34:56.12545', 'yyyy-mm-dd HH24:MI:SS.FF5'),TO_TIMESTAMP('2022-11-03 12:34:56.125456789', 'yyyy-mm-dd HH24:MI:SS.FF9'),TO_TIMESTAMP_TZ('2022-11-03 01:34:56.00789', 'yyyy-mm-dd HH24:MI:SS.FF5'),
        'yy8', 1, 1.1, TO_TIMESTAMP('2022-11-03 12:34:56.12545', 'yyyy-mm-dd HH24:MI:SS.FF5')
       );

update DEBEZIUM.FULL_TYPES set VAL_VARCHAR = 'dailai' where ID = 7;
