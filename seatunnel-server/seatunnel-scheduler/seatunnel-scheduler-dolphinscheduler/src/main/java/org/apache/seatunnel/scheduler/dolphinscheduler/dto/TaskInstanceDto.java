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

import static org.apache.seatunnel.server.common.DateUtils.DEFAULT_DATETIME_FORMAT;

import com.fasterxml.jackson.annotation.JsonFormat;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import lombok.Data;

import java.util.Date;

@Data
@JsonIgnoreProperties(ignoreUnknown = true)
public class TaskInstanceDto {
    private boolean taskComplete;
    private boolean firstRun;
    private int dryRun;
    private String flag;
    private long environmentCode;
    private String processInstance;
    private int pid;
    private String taskParams;
    private String duration;
    private String taskType;
    private long taskCode;
    private String taskInstancePriority;
    private String host;
    @JsonFormat(pattern = DEFAULT_DATETIME_FORMAT)
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
    @JsonFormat(pattern = DEFAULT_DATETIME_FORMAT)
    private Date submitTime;
    private String name;
    private int retryInterval;
    @JsonFormat(pattern = DEFAULT_DATETIME_FORMAT)
    private Date endTime;
    private String processInstanceName;
}
