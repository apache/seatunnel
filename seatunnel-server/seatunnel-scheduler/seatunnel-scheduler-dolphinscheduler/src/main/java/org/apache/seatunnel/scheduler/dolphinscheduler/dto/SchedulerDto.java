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

import org.apache.seatunnel.server.common.DateUtils;

import lombok.Builder;
import lombok.Data;

import java.util.Date;
import java.util.Map;

@Data
@Builder
public class SchedulerDto {
    private int id;
    private long processDefinitionCode;
    private String processDefinitionName;
    private String projectName;
    private String definitionDescription;
    private Date startTime;
    private Date endTime;
    private String timezoneId;
    private String crontab;
    private String failureStrategy;
    private String warningType;
    private Date createTime;
    private Date updateTime;
    private int userId;
    private String userName;
    private String releaseState;
    private int warningGroupId;
    private String processInstancePriority;
    private String workerGroup;
    private int environmentCode;

    public static SchedulerDto fromMap(Map<String, Object> map) {
        return SchedulerDto.builder()
                .id((Integer) map.get("id"))
                .processDefinitionCode((Long) map.get("processDefinitionCode"))
                .processDefinitionName((String) map.get("processDefinitionName"))
                .projectName((String) map.get("projectName"))
                .definitionDescription((String) map.get("definitionDescription"))
                .startTime(DateUtils.parse((String) map.get("startTime")))
                .endTime(DateUtils.parse((String) map.get("endTime")))
                .timezoneId((String) map.get("timezoneId"))
                .crontab((String) map.get("crontab"))
                .failureStrategy((String) map.get("failureStrategy"))
                .warningType((String) map.get("warningType"))
                .createTime(DateUtils.parse((String) map.get("createTime")))
                .updateTime(DateUtils.parse((String) map.get("updateTime")))
                .userId((Integer) map.get("userId"))
                .userName((String) map.get("userName"))
                .releaseState((String) map.get("releaseState"))
                .warningGroupId((Integer) map.get("warningGroupId"))
                .processInstancePriority((String) map.get("processInstancePriority"))
                .workerGroup((String) map.get("workerGroup"))
                .environmentCode((Integer) map.get("environmentCode"))
                .build();
    }
}
