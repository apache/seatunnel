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

CREATE DATABASE IF NOT EXISTS seatunnel_db DEFAULT CHARSET utf8mb4;

SET NAMES utf8mb4;

use seatunnel_db;

DROP TABLE IF EXISTS t_st_datasource;
CREATE TABLE t_st_datasource
(
    `id` bigint(20) NOT NULL AUTO_INCREMENT COMMENT '主键ID',
    `name` varchar(64) NOT NULL COMMENT '名称',
    `type` varchar(32) NOT NULL COMMENT '数据源类型',
    `configuration` varchar(1000)  NOT NULL COMMENT '配置',
    `tags` varchar(256) NULL DEFAULT NULL COMMENT '标签集',
    `description` varchar(256) NULL DEFAULT NULL COMMENT '描述',
    `status` int(11) NULL DEFAULT NULL COMMENT '状态: 1->有效、2->无效',
    `creator_id` int(11) NULL DEFAULT NULL COMMENT '创建人ID',
    `create_time` datetime(0) NULL DEFAULT NULL COMMENT '创建时间',
    `mender_id` int(11) NULL DEFAULT NULL COMMENT '修改人ID',
    `update_time` datetime(0) NULL DEFAULT NULL COMMENT '修改时间',
    PRIMARY KEY (`id`) USING BTREE
) COMMENT = '数据源表';

DROP TABLE IF EXISTS t_st_task;
CREATE TABLE t_st_task
(
    `id` bigint(20) NOT NULL AUTO_INCREMENT COMMENT '主键ID',
    `name` varchar(100) NOT NULL COMMENT '任务名称',
    `type` varchar(32) NOT NULL COMMENT '任务类型: spark、spark-sql,flink、flink-sql',
    `config_content` text NOT NULL COMMENT '任务配置内容',
    `tags` varchar(256) NULL DEFAULT NULL COMMENT '标签集',
    `description` varchar(256) NULL DEFAULT NULL COMMENT '描述',
    `status` int(11) NULL DEFAULT NULL COMMENT '状态: 1->上线、2->下线',
    `creator_id` int(11) NULL DEFAULT NULL COMMENT '创建人ID',
    `create_time` datetime(0) NULL DEFAULT NULL COMMENT '创建时间',
    `mender_id` int(11) NULL DEFAULT NULL COMMENT '修改人ID',
    `update_time` datetime(0) NULL DEFAULT NULL COMMENT '修改时间',
    PRIMARY KEY (`id`) USING BTREE
) COMMENT = '任务配置信息表';

DROP TABLE IF EXISTS t_st_task_instance;
CREATE TABLE t_st_task_instance
(
    `id` bigint(20) NOT NULL AUTO_INCREMENT COMMENT '主键ID',
    `task_id` bigint(20) NOT NULL COMMENT '任务ID',
    `name` varchar(256) NOT NULL COMMENT '任务实例名称',
    `type` varchar(32) NOT NULL COMMENT '任务类型: spark、spark-sql,flink、flink-sql',
    `config_content` text NOT NULL COMMENT '任务配置内容',
    `command` varchar(256) NULL DEFAULT NULL COMMENT '任务执行命令',
    `host_id` bigint(20) NULL DEFAULT NULL COMMENT '主机信息表ID',
    `instance_status` int(11) NULL DEFAULT NULL COMMENT '任务执行状态: 1->启动中、2->运行中、3->已完成、4->失败、5->停止',
    `creator_id` int(11) NULL DEFAULT NULL COMMENT '创建人ID',
    `create_time` datetime(0) NULL DEFAULT NULL COMMENT '创建时间',
    `sync_rate` bigint(20) NULL DEFAULT NULL COMMENT '速率',
    `sync_current_records` bigint(20) NULL DEFAULT NULL COMMENT '当前同步数量',
    `sync_volume` bigint(20) NULL DEFAULT NULL COMMENT '容量',
    `start_time` datetime(0) NULL DEFAULT NULL COMMENT '任务开始时间',
    `end_time` datetime(0) NULL DEFAULT NULL COMMENT '任务结束时间',
    PRIMARY KEY (`id`) USING BTREE
) COMMENT = '任务实例表';


DROP TABLE IF EXISTS t_st_host;
CREATE TABLE t_st_host
(
    `id` bigint(20) NOT NULL AUTO_INCREMENT COMMENT '主键ID',
    `name` varchar(64) NOT NULL COMMENT '名称',
    `description` varchar(256) NOT NULL COMMENT '描述',
    `host` varchar(50) NULL DEFAULT NULL COMMENT '主机IP',
    `host_port` varchar(50) NULL DEFAULT NULL COMMENT '主机端口',
    `host_user` varchar(50) NULL DEFAULT NULL COMMENT '主机用户',
    `host_user_pwd` varchar(50) NULL DEFAULT NULL COMMENT '主机用户密码',
    `root_dir` varchar(256) NULL DEFAULT NULL COMMENT '根目录',
    `status` int(11) NULL DEFAULT NULL COMMENT '状态: 1->有效、2->无效',
    `creator_id` int(11) NULL DEFAULT NULL COMMENT '创建人ID',
    `create_time` datetime(0) NULL DEFAULT NULL COMMENT '创建时间',
    `mender_id` int(11) NULL DEFAULT NULL COMMENT '修改人ID',
    `update_time` datetime(0) NULL DEFAULT NULL COMMENT '修改时间',
    PRIMARY KEY (`id`) USING BTREE
) COMMENT = '服务主机信息表';

DROP TABLE IF EXISTS t_st_alert_config;
CREATE TABLE t_st_alert_config
(
    `id` bigint(20) NOT NULL AUTO_INCREMENT COMMENT '主键ID',
    `name` varchar(64) NOT NULL COMMENT '名称',
    `type` varchar(32) NOT NULL COMMENT '类型: email->邮箱、sms->短信',
    `config_content` text NOT NULL COMMENT '配置内容',
    `status` int(11) NULL DEFAULT NULL COMMENT '状态: 1->有效、2->无效',
    `creator_id` int(11) NULL DEFAULT NULL COMMENT '创建人ID',
    `create_time` datetime(0) NULL DEFAULT NULL COMMENT '创建时间',
    `mender_id` int(11) NULL DEFAULT NULL COMMENT '修改人ID',
    `update_time` datetime(0) NULL DEFAULT NULL COMMENT '修改时间',
    PRIMARY KEY (`id`) USING BTREE
) COMMENT = '告警配置信息表';

DROP TABLE IF EXISTS t_st_alert_group;
CREATE TABLE t_st_alert_group
(
    `id` bigint(20) NOT NULL AUTO_INCREMENT COMMENT '主键ID',
    `name` varchar(64) NOT NULL COMMENT '名称',
    `alert_id` bigint(20) NULL DEFAULT NULL COMMENT '告警配置ID',
    `user_ids` varchar(500) NOT NULL COMMENT '告警用户ID集',
    `status` int(11) NULL DEFAULT NULL COMMENT '状态: 1->有效、2->无效',
    `creator_id` int(11) NULL DEFAULT NULL COMMENT '创建人ID',
    `create_time` datetime(0) NULL DEFAULT NULL COMMENT '创建时间',
    `mender_id` int(11) NULL DEFAULT NULL COMMENT '修改人ID',
    `update_time` datetime(0) NULL DEFAULT NULL COMMENT '修改时间',
    PRIMARY KEY (`id`) USING BTREE
) COMMENT = '告警组信息表';

DROP TABLE IF EXISTS t_st_alert_message;
CREATE TABLE t_st_alert_message
(
    `id` bigint(20) NOT NULL AUTO_INCREMENT COMMENT '主键ID',
    `alert_id` bigint(20) NULL DEFAULT NULL COMMENT '告警配置ID',
    `name` varchar(64) NOT NULL COMMENT '标题',
    `alert_content` text NOT NULL COMMENT '告警内容',
    `status` int(11) NULL DEFAULT NULL COMMENT '状态: 1->成功、2->失败',
    `creator_id` int(11) NULL DEFAULT NULL COMMENT '创建人ID',
    `create_time` datetime(0) NULL DEFAULT NULL COMMENT '创建时间',
    PRIMARY KEY (`id`) USING BTREE
) COMMENT = '告警信息记录表';


DROP TABLE IF EXISTS t_st_user;
CREATE TABLE t_st_user
(
    `id` int(11) NOT NULL AUTO_INCREMENT COMMENT '主键ID',
    `username` VARCHAR(30) NOT NULL COMMENT '用户名',
    `password` varchar(64) NOT NULL COMMENT '用户密码',
    `salt` varchar(64) NOT NULL COMMENT '盐值',
    `type` int(11) NOT NULL COMMENT '用户类型: 1->管理员、2->普通用户',
    `email` varchar(100) NULL DEFAULT NULL COMMENT '邮箱',
    `status` int(11) NOT NULL COMMENT '状态: 1->有效、2->无效',
    `creator_id` int(11) NULL DEFAULT NULL COMMENT '创建人ID',
    `create_time` datetime(0) NULL DEFAULT NULL COMMENT '创建时间',
    `mender_id` int(11) NULL DEFAULT NULL COMMENT '修改人ID',
    `update_time` datetime(0) NULL DEFAULT NULL COMMENT '修改时间',
    PRIMARY KEY (`id`) USING BTREE,
    UNIQUE KEY `idx_st_user_username` (`username`)
) COMMENT = '用户表';
