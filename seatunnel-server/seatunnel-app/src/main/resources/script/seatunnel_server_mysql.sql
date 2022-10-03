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

CREATE DATABASE IF NOT EXISTS seatunnel;


-- ----------------------------
-- Table structure for role
-- ----------------------------
DROP TABLE IF EXISTS `seatunnel`.`role`;
CREATE TABLE `seatunnel`.`role` (
                        `id` int(20) NOT NULL AUTO_INCREMENT,
                        `type` int(2) NOT NULL,
                        `role_name` varchar(255) NOT NULL,
                        `description` varchar(255) DEFAULT NULL,
                        `create_time` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
                        `update_time` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3),
                        PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- ----------------------------
-- Records of role
-- ----------------------------

-- ----------------------------
-- Table structure for role_user_relation
-- ----------------------------
DROP TABLE IF EXISTS `seatunnel`.`role_user_relation`;
CREATE TABLE `seatunnel`.`role_user_relation` (
                                      `id` int(20) NOT NULL AUTO_INCREMENT,
                                      `role_id` int(20) NOT NULL,
                                      `user_id` int(20) NOT NULL,
                                      `create_time` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
                                      `update_time` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3),
                                      PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- ----------------------------
-- Records of role_user_relation
-- ----------------------------

-- ----------------------------
-- Table structure for scheduler_config
-- ----------------------------
DROP TABLE IF EXISTS `seatunnel`.`scheduler_config`;
CREATE TABLE `seatunnel`.`scheduler_config` (
                                    `id` int(11) NOT NULL AUTO_INCREMENT,
                                    `script_id` int(11) DEFAULT NULL,
                                    `trigger_expression` varchar(255) DEFAULT NULL,
                                    `retry_times` int(11) NOT NULL DEFAULT '0',
                                    `retry_interval` int(11) NOT NULL DEFAULT '0',
                                    `active_start_time` datetime(3) NOT NULL,
                                    `active_end_time` datetime(3) NOT NULL,
                                    `create_time` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
                                    `update_time` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3),
                                    `creator_id` int(11) NOT NULL,
                                    `update_id` int(11) NOT NULL,
                                    PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
-- ----------------------------
-- Records of scheduler_config
-- ----------------------------

-- ----------------------------
-- Table structure for script
-- ----------------------------
DROP TABLE IF EXISTS `seatunnel`.`script`;
CREATE TABLE `seatunnel`.`script` (
                          `id` int(11) NOT NULL AUTO_INCREMENT,
                          `name` varchar(255) NOT NULL,
                          `type` tinyint(4) NOT NULL,
                          `status` tinyint(4) NOT NULL,
                          `content` mediumtext,
                          `content_md5` varchar(255) DEFAULT NULL,
                          `creator_id` int(11) NOT NULL,
                          `mender_id` int(11) NOT NULL,
                          `create_time` datetime(3) DEFAULT CURRENT_TIMESTAMP(3),
                          `update_time` datetime(3) DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3),
                          PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- ----------------------------
-- Records of script
-- ----------------------------

-- ----------------------------
-- Table structure for script_job_apply
-- ----------------------------
DROP TABLE IF EXISTS `seatunnel`.`script_job_apply`;
CREATE TABLE `seatunnel`.`script_job_apply` (
                                    `id` int(11) NOT NULL AUTO_INCREMENT,
                                    `script_id` int(11) NOT NULL,
                                    `scheduler_config_id` int(11) NOT NULL,
                                    `job_id` bigint(20) DEFAULT NULL,
                                    `operator_id` int(11) NOT NULL,
                                    `create_time` datetime(3) DEFAULT CURRENT_TIMESTAMP(3),
                                    `update_time` datetime(3) DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3),
                                    PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- ----------------------------
-- Records of script_job_apply
-- ----------------------------

-- ----------------------------
-- Table structure for script_param
-- ----------------------------
DROP TABLE IF EXISTS `seatunnel`.`script_param`;
CREATE TABLE `seatunnel`.`script_param` (
                                `id` int(11) NOT NULL AUTO_INCREMENT,
                                `script_id` int(11) DEFAULT NULL,
                                `key` varchar(255) NOT NULL,
                                `value` varchar(255) DEFAULT NULL,
                                `status` tinyint(4) DEFAULT NULL,
                                `create_time` datetime(3) DEFAULT CURRENT_TIMESTAMP(3),
                                `update_time` datetime(3) DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3),
                                PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- ----------------------------
-- Records of script_param
-- ----------------------------

-- ----------------------------
-- Table structure for user
-- ----------------------------
DROP TABLE IF EXISTS `seatunnel`.`user`;
CREATE TABLE `seatunnel`.`user` (
                        `id` int(11) NOT NULL AUTO_INCREMENT,
                        `username` varchar(255) NOT NULL,
                        `password` varchar(255) NOT NULL,
                        `status` tinyint(4) NOT NULL,
                        `type` tinyint(4) NOT NULL,
                        `create_time` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
                        `update_time` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3),
                        PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- ----------------------------
-- Records of user
-- ----------------------------

-- ----------------------------
-- Table structure for user_login_log
-- ----------------------------
DROP TABLE IF EXISTS `seatunnel`.`user_login_log`;
CREATE TABLE `seatunnel`.`user_login_log` (
                                  `id` bigint(20) NOT NULL AUTO_INCREMENT,
                                  `user_id` int(11) NOT NULL,
                                  `token` mediumtext NOT NULL,
                                  `token_status` tinyint(1) NOT NULL,
                                  `create_time` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
                                  `update_time` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3),
                                  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- ----------------------------
-- Records of user_login_log
-- ----------------------------
