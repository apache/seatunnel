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

USE `multi_schema_shop_a`;

INSERT INTO products
VALUES (104,"a-pre-ddl","Source A row before ddl",10.0);

ALTER TABLE products
    ADD COLUMN add_column1 VARCHAR(64) NOT NULL DEFAULT 'db-a',
    ADD COLUMN add_column2 INT NOT NULL DEFAULT 1;

UPDATE products SET add_column1 = 'a-101', add_column2 = 101 WHERE id = 101;
DELETE FROM products WHERE id = 102;

INSERT INTO products
VALUES (110,"a-post-ddl","Source A row after ddl",11.0,'a-110',110),
       (111,"a-post-ddl-2","Source A row after ddl 2",12.0,'a-111',111);

USE `multi_schema_shop_b`;

INSERT INTO products
VALUES (104,"b-pre-ddl","Source B row before ddl",20.0);

ALTER TABLE products
    ADD COLUMN add_column1 VARCHAR(64) NOT NULL DEFAULT 'db-b',
    ADD COLUMN add_column2 INT NOT NULL DEFAULT 1;

UPDATE products SET add_column1 = 'b-101', add_column2 = 201 WHERE id = 101;
DELETE FROM products WHERE id = 102;

INSERT INTO products
VALUES (110,"b-post-ddl","Source B row after ddl",21.0,'b-110',210),
       (111,"b-post-ddl-2","Source B row after ddl 2",22.0,'b-111',211);
