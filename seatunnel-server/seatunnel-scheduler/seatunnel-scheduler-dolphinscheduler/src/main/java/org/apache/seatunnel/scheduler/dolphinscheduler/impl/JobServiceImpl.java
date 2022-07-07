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

package org.apache.seatunnel.scheduler.dolphinscheduler.impl;

import static org.apache.seatunnel.scheduler.dolphinscheduler.constants.DolphinschedulerConstants.RELEASE_STATE_OFFLINE;
import static org.apache.seatunnel.scheduler.dolphinscheduler.constants.DolphinschedulerConstants.RELEASE_STATE_ONLINE;

import org.apache.seatunnel.scheduler.dolphinscheduler.IDolphinschedulerService;
import org.apache.seatunnel.scheduler.dolphinscheduler.dto.ListProcessDefinitionDto;
import org.apache.seatunnel.scheduler.dolphinscheduler.dto.ProcessDefinitionDto;
import org.apache.seatunnel.scheduler.dolphinscheduler.dto.SchedulerDto;
import org.apache.seatunnel.scheduler.dolphinscheduler.dto.TaskDefinitionDto;
import org.apache.seatunnel.scheduler.dolphinscheduler.dto.UpdateProcessDefinitionDto;
import org.apache.seatunnel.spi.scheduler.IJobService;
import org.apache.seatunnel.spi.scheduler.dto.JobDto;
import org.apache.seatunnel.spi.scheduler.dto.JobListDto;
import org.apache.seatunnel.spi.scheduler.dto.JobSimpleInfoDto;

import org.springframework.stereotype.Component;

import javax.annotation.Resource;

import java.util.List;
import java.util.stream.Collectors;

@Component
public class JobServiceImpl implements IJobService {
    @Resource
    private IDolphinschedulerService iDolphinschedulerService;

    @Override
    public long submitJob(JobDto dto) {
        // one process == one seatunnel script == one job

        final TaskDefinitionDto taskDefinitionDto = TaskDefinitionDto.builder()
                .name(dto.getJobName())
                .executeScript(dto.getExecutorScript())
                .content(dto.getJobContent())
                .params(dto.getParams())
                .retryInterval(dto.getSchedulerConfigDto().getRetryInterval())
                .retryTimes(dto.getSchedulerConfigDto().getRetryTimes())
                .build();

        final UpdateProcessDefinitionDto processDto = UpdateProcessDefinitionDto.builder()
                .name(dto.getJobName())
                .startTime(dto.getSchedulerConfigDto().getStartTime())
                .endTime(dto.getSchedulerConfigDto().getEndTime())
                .cronExpression(dto.getSchedulerConfigDto().getTriggerExpression())
                .taskDefinitionDto(taskDefinitionDto)
                .processDefinitionCode(dto.getJobId())
                .build();

        final ProcessDefinitionDto processDefinition = iDolphinschedulerService.createOrUpdateProcessDefinition(processDto);
        dto.setJobId(processDefinition.getCode());

        iDolphinschedulerService.updateProcessDefinitionState(processDefinition.getCode(), processDefinition.getName(), RELEASE_STATE_ONLINE);
        final SchedulerDto schedulerDto = iDolphinschedulerService.createOrUpdateSchedule(dto);
        iDolphinschedulerService.scheduleOnline(schedulerDto.getId());

        return processDefinition.getCode();
    }

    @Override
    public void offlineJob(JobDto dto) {
        iDolphinschedulerService.updateProcessDefinitionState(dto.getJobId(), dto.getJobName(), RELEASE_STATE_OFFLINE);
    }

    @Override
    public List<JobSimpleInfoDto> list(JobListDto dto) {
        final ListProcessDefinitionDto listDto = ListProcessDefinitionDto.builder()
                .name(dto.getName())
                .pageNo(dto.getPageNo())
                .pageSize(dto.getPageSize())
                .build();
        final List<ProcessDefinitionDto> processDefinitionDtos = iDolphinschedulerService.listProcessDefinition(listDto);
        return processDefinitionDtos.stream().map(p -> JobSimpleInfoDto.builder()
                    .jobId(p.getCode())
                    .jobStatus(p.getReleaseState())
                    .createTime(p.getCreateTime())
                    .updateTime(p.getUpdateTime())
                    .creatorName(p.getUserName())
                    .menderName(p.getUserName())
                    .build())
                .collect(Collectors.toList());
    }

}
