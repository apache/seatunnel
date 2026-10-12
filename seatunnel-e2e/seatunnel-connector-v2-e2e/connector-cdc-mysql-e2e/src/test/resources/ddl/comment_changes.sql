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
-- DATABASE:  shop
-- Table state at this point:
--   id INTEGER NOT NULL AUTO_INCREMENT PRIMARY KEY
--   name LONGTEXT NULL
--   description VARCHAR(512)
--   weight FLOAT
--   add_column INT NOT NULL DEFAULT 1
-- ----------------------------------------------------------------------------------------------------------------
CREATE DATABASE IF NOT EXISTS `shop`;
use shop;

-- case5: comment changes; the CDC pipeline must survive comment-related DDL
--        and continue replicating data normally. The JDBC sink intentionally
--        ignores comment events, so only data continuity is verified end-to-end.

-- Step 1: Table comment change (comment-only DDL)
ALTER TABLE products COMMENT = 'Product catalog table';

delete from products where id < 182;
insert into products
values (182,"motorcycle","Sport motorcycle",200.0,1),
       (183,"bicycle","Mountain bicycle",15.5,2),
       (184,"skateboard","Street skateboard",3.2,3),
       (185,"rollerblade","Inline rollerblade",4.5,4),
       (186,"football","Standard football",0.45,5),
       (187,"basketball","Standard basketball",0.62,6),
       (188,"tennis racket","Carbon fiber racket",0.28,7),
       (189,"badminton racket","Lightweight racket",0.12,8),
       (190,"ping-pong paddle","Table tennis paddle",0.18,9);

-- Step 2: Column comment embedded in MODIFY COLUMN (comment is part of a structural change)
--         This tests that column comments flow through when bundled with type/nullability changes
ALTER TABLE products MODIFY COLUMN description VARCHAR(512) COMMENT 'Product description text';

insert into products
values (191,"golf club","Steel driver club",0.95,1),
       (192,"baseball bat","Wooden bat",1.2,2),
       (193,"cricket bat","Willow cricket bat",1.1,3);

-- Step 3: Update table comment again (verifies comment updates are idempotent)
ALTER TABLE products COMMENT = 'Updated product catalog with sports equipment';

insert into products
values (194,"hockey stick","Composite hockey stick",0.6,1),
       (195,"rugby ball","Official size rugby ball",0.42,2);
