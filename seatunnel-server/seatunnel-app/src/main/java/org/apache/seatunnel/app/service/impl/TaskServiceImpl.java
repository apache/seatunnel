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

package org.apache.seatunnel.app.service.impl;

import static org.apache.seatunnel.server.common.SeatunnelErrorEnum.NO_SUCH_SCRIPT;
import static org.apache.seatunnel.server.common.SeatunnelErrorEnum.SCHEDULER_CONFIG_NOT_EXIST;

import org.apache.seatunnel.app.dal.dao.ISchedulerConfigDao;
import org.apache.seatunnel.app.dal.dao.IScriptDao;
import org.apache.seatunnel.app.dal.dao.IScriptJobApplyDao;
import org.apache.seatunnel.app.dal.dao.IScriptParamDao;
import org.apache.seatunnel.app.dal.entity.SchedulerConfig;
import org.apache.seatunnel.app.dal.entity.Script;
import org.apache.seatunnel.app.dal.entity.ScriptJobApply;
import org.apache.seatunnel.app.dal.entity.ScriptParam;
import org.apache.seatunnel.app.domain.dto.job.PushScriptDto;
import org.apache.seatunnel.app.domain.dto.job.ScriptJobApplyDto;
import org.apache.seatunnel.app.domain.request.task.InstanceListReq;
import org.apache.seatunnel.app.domain.request.task.JobListReq;
import org.apache.seatunnel.app.domain.request.task.RecycleScriptReq;
import org.apache.seatunnel.app.domain.response.task.InstanceSimpleInfoRes;
import org.apache.seatunnel.app.domain.response.task.JobSimpleInfoRes;
import org.apache.seatunnel.app.service.ITaskService;
import org.apache.seatunnel.server.common.SeatunnelException;
import org.apache.seatunnel.spi.scheduler.IInstanceService;
import org.apache.seatunnel.spi.scheduler.IJobService;
import org.apache.seatunnel.spi.scheduler.dto.InstanceDto;
import org.apache.seatunnel.spi.scheduler.dto.InstanceListDto;
import org.apache.seatunnel.spi.scheduler.dto.JobDto;
import org.apache.seatunnel.spi.scheduler.dto.JobListDto;
import org.apache.seatunnel.spi.scheduler.dto.JobSimpleInfoDto;
import org.apache.seatunnel.spi.scheduler.dto.SchedulerConfigDto;

import com.google.common.collect.Maps;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import org.springframework.util.CollectionUtils;

import javax.annotation.Resource;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

@Component
@Slf4j
public class TaskServiceImpl implements ITaskService {

    @Resource
    private IJobService iJobService;

    @Resource
    private IInstanceService iInstanceService;

    @Resource
    private IScriptDao scriptDaoImpl;

    @Resource
    private IScriptParamDao scriptParamDaoImpl;

    @Resource
    private ISchedulerConfigDao schedulerConfigDaoImpl;

    @Resource
    private IScriptJobApplyDao scriptJobApplyDaoImpl;

    @Override
    public Long pushScriptToScheduler(PushScriptDto pushScriptDto) {
        final int scriptId = pushScriptDto.getScriptId();
        final int userId = pushScriptDto.getUserId();

        // check scheduler param
        SchedulerConfig config = schedulerConfigDaoImpl.getSchedulerConfig(scriptId);
        if (Objects.isNull(config)) {
            throw new SeatunnelException(SCHEDULER_CONFIG_NOT_EXIST);
        }

        final Script script = checkAndGetScript(scriptId);
        final List<ScriptParam> scriptParams = scriptParamDaoImpl.getParamsByScriptId(scriptId);
        Map<String, Object> params = Maps.newHashMap();

        if (!CollectionUtils.isEmpty(params)) {
            scriptParams.forEach(scriptParam -> params.put(scriptParam.getKey(), scriptParam.getValue()));
        }

        final SchedulerConfigDto schedulerConfigDto = SchedulerConfigDto.builder()
                .retryInterval(config.getRetryInterval())
                .retryTimes(config.getRetryTimes())
                .startTime(config.getActiveStartTime())
                .endTime(config.getActiveEndTime())
                .triggerExpression(config.getTriggerExpression())
                .build();

        final JobDto jobDto = JobDto.builder()
                .jobName(script.getName())
                .jobContent(script.getContent())
                .params(params)
                .operatorId(userId)
                .schedulerConfigDto(schedulerConfigDto)
                //todo fix to real execute script
                .executorScript(script.getContent())
                .jobId(null)
                .build();

        ScriptJobApply apply = scriptJobApplyDaoImpl.getByScriptId(script.getId());
        if (Objects.nonNull(apply)) {
            jobDto.setJobId(apply.getJobId());
        }

        // push script
        final long jobId = iJobService.submitJob(jobDto);

        // Use future to ensure that the page does not show exceptions due to database errors.
        syncScriptJobMapping(scriptId, userId, config.getId(), jobId);
        return jobId;
    }

    @Override
    public void recycleScriptFromScheduler(RecycleScriptReq req) {
        final Script script = checkAndGetScript(req.getScriptId());
        ScriptJobApply apply = scriptJobApplyDaoImpl.getByScriptId(script.getId());

        final JobDto jobDto = JobDto.builder()
                .jobId(apply.getJobId())
                .jobName(script.getName())
                .operatorId(req.getOperatorId())
                .build();

        iJobService.offlineJob(jobDto);

        syncScriptJobMapping(script.getId(), req.getOperatorId(), apply.getSchedulerConfigId(), apply.getJobId());
    }

    @Override
    public List<JobSimpleInfoRes> listJob(JobListReq req) {
        // Search from scheduler.
        final JobListDto dto = JobListDto.builder()
                .name(req.getName())
                .pageNo(req.getPageNo())
                .pageSize(req.getPageSize())
                .build();
        final List<JobSimpleInfoDto> list = iJobService.list(dto);
        return list.stream().map(this::translate).collect(Collectors.toList());
    }

    @Override
    public List<InstanceSimpleInfoRes> listInstance(InstanceListReq req) {
        // Search from scheduler.
        final InstanceListDto dto = InstanceListDto.builder()
                .name(req.getName())
                .pageNo(req.getPageNo())
                .pageSize(req.getPageSize())
                .build();
        final List<InstanceDto> list = iInstanceService.list(dto);
        return list.stream().map(this::translate).collect(Collectors.toList());
    }

    private JobSimpleInfoRes translate(JobSimpleInfoDto dto) {
        return JobSimpleInfoRes.builder()
                .jobId(dto.getJobId())
                .jobStatus(dto.getJobStatus())
                .creatorName(dto.getCreatorName())
                .menderName(dto.getMenderName())
                .createTime(dto.getCreateTime())
                .updateTime(dto.getUpdateTime())
                .build();
    }

    private InstanceSimpleInfoRes translate(InstanceDto dto) {
        return InstanceSimpleInfoRes.builder()
                .instanceId(dto.getInstanceId())
                .instanceCode(dto.getInstanceCode())
                .instanceName(dto.getInstanceName())
                .submitTime(dto.getSubmitTime())
                .startTime(dto.getStartTime())
                .endTime(dto.getEndTime())
                .status(dto.getStatus())
                .executionDuration(dto.getExecutionDuration())
                .retryTimes(dto.getRetryTimes())
                .build();
    }

    private Script checkAndGetScript(int scriptId) {
        final Script script = scriptDaoImpl.getScript(scriptId);
        if (Objects.isNull(script)) {
            throw new SeatunnelException(NO_SUCH_SCRIPT);
        }
        return script;
    }

    private void syncScriptJobMapping(int scriptId, int userId, int schedulerConfigId, long jobId) {
        CompletableFuture.runAsync(() -> {
            // store script and job mapping
            final ScriptJobApplyDto dto = ScriptJobApplyDto.builder()
                    .scriptId(scriptId)
                    .schedulerConfigId(schedulerConfigId)
                    .jobId(jobId)
                    .userId(userId)
                    .build();
            scriptJobApplyDaoImpl.insertOrUpdate(dto);
        }).whenComplete((_return, e) -> {
            if (Objects.nonNull(e)) {
                log.error("Store script and job mapping failed, please maintain this mapping manually. \n" +
                        "scriptId [{}], schedulerConfigId [{}], jobId [{}], userId [{}]", scriptId, schedulerConfigId, jobId, userId, e);
            }
        });
    }
}
