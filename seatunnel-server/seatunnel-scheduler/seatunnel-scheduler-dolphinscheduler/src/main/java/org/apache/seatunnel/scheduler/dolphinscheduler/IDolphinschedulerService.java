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

package org.apache.seatunnel.scheduler.dolphinscheduler;

import org.apache.seatunnel.scheduler.dolphinscheduler.dto.ListProcessDefinitionDto;
import org.apache.seatunnel.scheduler.dolphinscheduler.dto.ListProcessInstanceDto;
import org.apache.seatunnel.scheduler.dolphinscheduler.dto.ListTaskInstanceDto;
import org.apache.seatunnel.scheduler.dolphinscheduler.dto.ProcessDefinitionDto;
import org.apache.seatunnel.scheduler.dolphinscheduler.dto.ProcessInstanceDto;
import org.apache.seatunnel.scheduler.dolphinscheduler.dto.ResourceDto;
import org.apache.seatunnel.scheduler.dolphinscheduler.dto.SchedulerDto;
import org.apache.seatunnel.scheduler.dolphinscheduler.dto.StartProcessDefinitionDto;
import org.apache.seatunnel.scheduler.dolphinscheduler.dto.TaskInstanceDto;
import org.apache.seatunnel.scheduler.dolphinscheduler.dto.UpdateProcessDefinitionDto;
import org.apache.seatunnel.server.common.PageData;
import org.apache.seatunnel.spi.scheduler.dto.InstanceLogDto;
import org.apache.seatunnel.spi.scheduler.dto.JobDto;

import java.util.List;

public interface IDolphinschedulerService {

    ProcessDefinitionDto createOrUpdateProcessDefinition(UpdateProcessDefinitionDto dto);

    PageData<ProcessDefinitionDto> listProcessDefinition(ListProcessDefinitionDto dto);

    ProcessDefinitionDto fetchProcessDefinitionByName(String processDefinitionName);

    void startProcessDefinition(StartProcessDefinitionDto dto);

    void updateProcessDefinitionState(long processDefinitionCode, String processDefinitionName, String state);

    SchedulerDto createOrUpdateSchedule(JobDto dto);

    List<SchedulerDto> listSchedule(long processDefinitionCode);

    void scheduleOnline(int scheduleId);

    void scheduleOffline(int scheduleId);

    List<Long> genTaskCodes(long projectCode, int num);

    ResourceDto createOrUpdateScriptContent(String resourceName, String content);

    PageData<TaskInstanceDto> listTaskInstance(ListTaskInstanceDto dto);

    void deleteProcessDefinition(long code);

    PageData<ProcessInstanceDto> listProcessInstance(ListProcessInstanceDto dto);

    InstanceLogDto queryInstanceLog(long instanceId);

    void killProcessInstance(long processInstanceId);
}
