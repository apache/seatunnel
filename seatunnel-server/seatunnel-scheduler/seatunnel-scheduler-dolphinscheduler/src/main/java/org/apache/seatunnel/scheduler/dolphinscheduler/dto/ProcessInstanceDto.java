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
import lombok.Data;

import java.util.Date;

@Data
public class ProcessInstanceDto {
    private int id;
    private long processDefinitionCode;
    private int processDefinitionVersion;
    private String state;
    private String recovery;
    @JsonFormat(pattern = DEFAULT_DATETIME_FORMAT)
    private Date startTime;
    @JsonFormat(pattern = DEFAULT_DATETIME_FORMAT)
    private Date endTime;
    private int runTimes;
    private String name;
    private String host;
    private String processDefinition;
    private String commandType;
    private String commandParam;
    private String taskDependType;
    private int maxTryTimes;
    private String failureStrategy;
    private String warningType;
    private String warningGroupId;
    @JsonFormat(pattern = DEFAULT_DATETIME_FORMAT)
    private Date scheduleTime;
    private String commandStartTime;
    private String globalParams;
    private String dagData;
    private int executorId;
    private String executorName;
    private String tenantCode;
    private String queue;
    private String isSubProcess;
    private String locations;
    private String historyCmd;
    private String dependenceScheduleTimes;
    private String duration;
    private String processInstancePriority;
    private String workerGroup;
    private String environmentCode;
    private int timeout;
    private int tenantId;
    private String varPool;
    private int nextProcessInstanceId;
    private int dryRun;
    @JsonFormat(pattern = DEFAULT_DATETIME_FORMAT)
    private Date restartTime;
    private String cmdTypeIfComplement;
    private boolean complementData;
    private boolean blocked;
    private boolean processInstanceStop;
}
