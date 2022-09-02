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

import org.apache.seatunnel.scheduler.api.IInstanceService;
import org.apache.seatunnel.scheduler.api.dto.InstanceDto;
import org.apache.seatunnel.scheduler.api.dto.InstanceListDto;
import org.apache.seatunnel.scheduler.dolphinscheduler.IDolphinSchedulerService;
import org.apache.seatunnel.scheduler.dolphinscheduler.dto.ListProcessInstanceDto;
import org.apache.seatunnel.scheduler.dolphinscheduler.dto.TaskInstanceDto;
import org.apache.seatunnel.server.common.PageData;

import java.util.List;
import java.util.stream.Collectors;

public class InstanceServiceImpl implements IInstanceService {

    private final IDolphinSchedulerService dolphinSchedulerService;

    public InstanceServiceImpl(IDolphinSchedulerService dolphinSchedulerService) {
        this.dolphinSchedulerService = dolphinSchedulerService;
    }

    @Override
    public PageData<InstanceDto> list(InstanceListDto dto) {

        final ListProcessInstanceDto listDto = ListProcessInstanceDto.builder()
                .processInstanceName(dto.getName())
                .pageNo(dto.getPageNo())
                .pageSize(dto.getPageSize())
                .build();
        final PageData<TaskInstanceDto> instancePageData = dolphinSchedulerService.listTaskInstance(listDto);

        final List<InstanceDto> data = instancePageData.getData().stream().map(t -> InstanceDto.builder()
                .instanceId(t.getId())
                .instanceCode(t.getProcessInstanceId())
                .instanceName(t.getProcessInstanceName())
                .status(t.getState())
                .startTime(t.getStartTime())
                .endTime(t.getEndTime())
                .submitTime(t.getSubmitTime())
                .executionDuration(t.getDuration())
                .retryTimes(t.getRetryTimes())
                .build()).collect(Collectors.toList());
        return new PageData<>(instancePageData.getTotalCount(), data);
    }
}
