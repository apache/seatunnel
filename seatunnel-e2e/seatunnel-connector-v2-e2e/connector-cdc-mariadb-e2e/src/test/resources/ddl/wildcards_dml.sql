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
-- DATABASE:  source
-- ----------------------------------------------------------------------------------------------------------------

use `source`;

UPDATE `source`.`products` SET name = 'Illustrated new quality productivity' WHERE id = 102;
INSERT INTO `source`.`customers` VALUES (1005,"Zhangdonghao","","hawk9821@xxx.com");

-- ----------------------------------------------------------------------------------------------------------------
-- DATABASE:  source1
-- ----------------------------------------------------------------------------------------------------------------

use `source1`;
DELETE FROM `source1`.`orders` where order_number < 10004;


