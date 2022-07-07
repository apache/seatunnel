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

package org.apache.seatunnel.scheduler.dolphinscheduler.dto;

import static org.apache.seatunnel.server.common.DateUtils.DEFAULT_DATETIME_FORMAT_WITH_TIMEZONE;

import org.apache.seatunnel.server.common.DateUtils;

import lombok.Builder;
import lombok.Data;

import java.util.Date;
import java.util.Map;

@Data
@Builder
public class TaskInstanceDto {

    private boolean taskComplete;
    private boolean firstRun;
    private int dryRun;
    private String flag;
    private int environmentCode;
    private String processInstance;
    private int pid;
    private String taskParams;
    private String duration;
    private String taskType;
    private long taskCode;
    private String taskInstancePriority;
    private String host;
    private Date startTime;
    private int id;
    private String state;
    private String workerGroup;
    private String processInstancePriority;
    private int processInstanceId;
    private int executorId;
    private String firstSubmitTime;
    private String resources;
    private int maxRetryTimes;
    private int retryTimes;
    private String executorName;
    private Date submitTime;
    private String name;
    private int retryInterval;
    private Date endTime;
    private String processInstanceName;

    public static TaskInstanceDto fromMap(Map<String, Object> map) {
        return TaskInstanceDto.builder()
                .taskComplete((Boolean) map.get("taskComplete"))
                .firstRun((Boolean) map.get("firstRun"))
                .dryRun((Integer) map.get("dryRun"))
                .flag((String) map.get("flag"))
                .environmentCode((Integer) map.get("environmentCode"))
                .processInstance((String) map.get("processInstance"))
                .pid((Integer) map.get("pid"))
                .taskParams((String) map.get("taskParams"))
                .duration((String) map.get("duration"))
                .taskType((String) map.get("taskType"))
                .taskCode((Long) map.get("taskCode"))
                .taskInstancePriority((String) map.get("taskInstancePriority"))
                .host((String) map.get("host"))
                .startTime(DateUtils.parse((String) map.get("startTime"), DEFAULT_DATETIME_FORMAT_WITH_TIMEZONE))
                .id((Integer) map.get("id"))
                .state((String) map.get("state"))
                .workerGroup((String) map.get("workerGroup"))
                .processInstancePriority((String) map.get("processInstancePriority"))
                .processInstanceId((Integer) map.get("processInstanceId"))
                .executorId((Integer) map.get("executorId"))
                .firstSubmitTime((String) map.get("firstSubmitTime"))
                .resources((String) map.get("resources"))
                .maxRetryTimes((Integer) map.get("maxRetryTimes"))
                .retryTimes((Integer) map.get("retryTimes"))
                .executorName((String) map.get("executorName"))
                .submitTime(DateUtils.parse((String) map.get("submitTime"), DEFAULT_DATETIME_FORMAT_WITH_TIMEZONE))
                .name((String) map.get("name"))
                .retryInterval((Integer) map.get("retryInterval"))
                .endTime(DateUtils.parse((String) map.get("endTime"), DEFAULT_DATETIME_FORMAT_WITH_TIMEZONE))
                .processInstanceName((String) map.get("processInstanceName"))
                .build();
    }
}
