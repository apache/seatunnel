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

import com.google.common.collect.Maps;
import lombok.Builder;
import lombok.Data;

import java.util.Map;

@Builder
@Data
public class StartProcessDefinitionDto {
    private long processDefinitionCode;
    private String failureStrategy;
    private String warningType;
    private int warningGroupId;
    private String taskDependType;
    private String runMode;
    private String processInstancePriority;
    private String workerGroup;
    private int dryRun;
    private String scheduleTime;
    private String execType;
    private String dependentMode;
    private Integer expectedParallelismNumber;

    public Map<String, String> toMap() {
        final Map<String, String> map = Maps.newHashMap();
        map.put("processDefinitionCode", String.valueOf(processDefinitionCode));
        map.put("failureStrategy", failureStrategy);
        map.put("warningType", warningType);
        map.put("warningGroupId", String.valueOf(warningGroupId));
        map.put("taskDependType", taskDependType);
        map.put("runMode", runMode);
        map.put("processInstancePriority", processInstancePriority);
        map.put("workerGroup", workerGroup);
        map.put("dryRun", String.valueOf(dryRun));
        map.put("scheduleTime", scheduleTime);
        map.put("execType", execType);
        map.put("dependentMode", dependentMode);
        map.put("expectedParallelismNumber", String.valueOf(expectedParallelismNumber));
        return map;
    }
}
