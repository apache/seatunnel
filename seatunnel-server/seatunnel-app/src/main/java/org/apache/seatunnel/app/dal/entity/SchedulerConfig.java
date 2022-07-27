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

package org.apache.seatunnel.app.dal.entity;

import lombok.Data;

import java.util.Date;

@Data
public class SchedulerConfig {
    private Integer id;

    private Integer scriptId;

    private String triggerExpression;

    private Integer retryTimes;

    private Integer retryInterval;

    private Date activeStartTime;

    private Date activeEndTime;

    private Date createTime;

    private Date updateTime;

    private Integer creatorId;

    private Integer updateId;
}
