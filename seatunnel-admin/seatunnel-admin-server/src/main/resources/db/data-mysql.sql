/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
*/
use seatunnel_db;

SET NAMES utf8mb4;

DELETE FROM t_st_user;

INSERT INTO `t_st_user` (`id`, `username`, `password`, `salt`, `type`, `email`, `status`, `creator_id`, `create_time`, `mender_id`, `update_time`)
VALUES
('1', 'admin', 'a10176d406e81874d3a9f69e261b4052', 'HVorcWmUKu6dfBQzJvHwEJW9KaKe0JFo', '1', '100@163.com', '1', '1', '2022-06-29 11:17:06', NULL, NULL);
