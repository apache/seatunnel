/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 */

CREATE TABLE test1 WITH (
    'connector'='FakeSource',
    'schema' = '{ 
      fields { 
        id = "int", 
        name = "string",
        c_time = "timestamp"
      } 
    }',
    'rows' = '[ 
      { fields = [21, "Eric", null], kind = INSERT },
      { fields = [22, "Andy", null], kind = INSERT } 
    ]',
    'type'='source'
);

CREATE TABLE test09 AS SELECT id,name, CAST(null AS TIMESTAMP) AS c_time FROM test1;

INSERT INTO test11 SELECT * FROM test09;

CREATE TABLE test11
WITH (
    'connector'='jdbc',
    'url' = 'jdbc:mysql://mysql-e2e:3306/seatunnel',
    'driver' = 'com.mysql.cj.jdbc.Driver',
    'user' = 'root',
    'password' = 'Abc!@#135_seatunnel',
    'generate_sink_sql' = 'true',
    'database' = 'seatunnel',
    'table' = 't_user', 
    'type'='sink'
);  
