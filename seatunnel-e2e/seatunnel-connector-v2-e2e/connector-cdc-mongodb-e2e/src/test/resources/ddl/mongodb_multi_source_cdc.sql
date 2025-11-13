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

-- Create tables for MongoDB CDC multi-source test

-- Table for MongoDB source A
CREATE TABLE IF NOT EXISTS products_a (
    _id VARCHAR(50) PRIMARY KEY,
    name VARCHAR(255),
    price INT
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- Table for MongoDB source B
CREATE TABLE IF NOT EXISTS products_b (
    _id VARCHAR(50) PRIMARY KEY,
    name VARCHAR(255),
    price INT
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
