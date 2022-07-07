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
public class ProcessDefinitionDto {
    private int id;
    private long code;
    private String name;
    private String releaseState;
    private long projectCode;
    private String description;
    private Date createTime;
    private Date updateTime;
    private String userName;
    private String projectName;
    private String locations;
    private String scheduleReleaseState;
    private int timeout;
    private int tenantId;
    private String tenantCode;
    private String modifyBy;
    private int warningGroupId;

    public static ProcessDefinitionDto fromMap(Map<String, Object> map) {
        return ProcessDefinitionDto.builder()
                .id((Integer) map.get("id"))
                .code((Long) map.get("code"))
                .name((String) map.get("name"))
                .releaseState((String) map.get("releaseState"))
                .projectCode((Long) map.get("projectCode"))
                .description((String) map.get("description"))
                .createTime(DateUtils.parse((String) map.get("createTime")))
                .updateTime(DateUtils.parse((String) map.get("updateTime")))
                .userName((String) map.get("userName"))
                .projectName((String) map.get("projectName"))
                .locations((String) map.get("locations"))
                .scheduleReleaseState((String) map.get("scheduleReleaseState"))
                .timeout((Integer) map.get("timeout"))
                .tenantId((Integer) map.get("tenantId"))
                .tenantCode((String) map.get("tenantCode"))
                .modifyBy((String) map.get("modifyBy"))
                .warningGroupId((Integer) map.get("warningGroupId"))
                .build();
    }
}
