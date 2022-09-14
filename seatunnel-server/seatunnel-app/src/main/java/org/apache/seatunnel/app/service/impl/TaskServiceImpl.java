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

import static org.apache.seatunnel.server.common.Constants.UNDERLINE;
import static org.apache.seatunnel.server.common.SeatunnelErrorEnum.NO_SUCH_SCRIPT;
import static org.apache.seatunnel.server.common.SeatunnelErrorEnum.SCHEDULER_CONFIG_NOT_EXIST;
import static org.apache.seatunnel.spi.scheduler.constants.SchedulerConstant.NEVER_TRIGGER_EXPRESSION;
import static org.apache.seatunnel.spi.scheduler.constants.SchedulerConstant.RETRY_INTERVAL_DEFAULT;
import static org.apache.seatunnel.spi.scheduler.constants.SchedulerConstant.RETRY_TIMES_DEFAULT;

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
import org.apache.seatunnel.app.domain.request.task.ExecuteReq;
import org.apache.seatunnel.app.domain.request.task.InstanceListReq;
import org.apache.seatunnel.app.domain.request.task.InstanceLogRes;
import org.apache.seatunnel.app.domain.request.task.JobListReq;
import org.apache.seatunnel.app.domain.request.task.RecycleScriptReq;
import org.apache.seatunnel.app.domain.response.PageInfo;
import org.apache.seatunnel.app.domain.response.task.InstanceSimpleInfoRes;
import org.apache.seatunnel.app.domain.response.task.JobSimpleInfoRes;
import org.apache.seatunnel.app.service.ITaskService;
import org.apache.seatunnel.server.common.PageData;
import org.apache.seatunnel.server.common.SeatunnelException;
import org.apache.seatunnel.spi.scheduler.IInstanceService;
import org.apache.seatunnel.spi.scheduler.IJobService;
import org.apache.seatunnel.spi.scheduler.dto.ExecuteDto;
import org.apache.seatunnel.spi.scheduler.dto.InstanceDto;
import org.apache.seatunnel.spi.scheduler.dto.InstanceListDto;
import org.apache.seatunnel.spi.scheduler.dto.InstanceLogDto;
import org.apache.seatunnel.spi.scheduler.dto.JobDto;
import org.apache.seatunnel.spi.scheduler.dto.JobListDto;
import org.apache.seatunnel.spi.scheduler.dto.JobSimpleInfoDto;
import org.apache.seatunnel.spi.scheduler.dto.SchedulerConfigDto;
import org.apache.seatunnel.spi.scheduler.enums.ExecuteTypeEnum;

import com.google.common.collect.Maps;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import org.springframework.util.CollectionUtils;

import javax.annotation.Resource;

import java.util.Date;
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
        Map<String, Object> params = getScriptParamMap(scriptParams);

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
    public PageInfo<JobSimpleInfoRes> listJob(JobListReq req) {
        // Search from scheduler.
        final JobListDto dto = JobListDto.builder()
                .name(req.getName())
                .pageNo(req.getPageNo())
                .pageSize(req.getPageSize())
                .build();
        final PageData<JobSimpleInfoDto> jobPageData = iJobService.list(dto);
        final List<JobSimpleInfoRes> data = jobPageData.getData().stream().map(this::translate).collect(Collectors.toList());
        final PageInfo<JobSimpleInfoRes> pageInfo = new PageInfo<>();
        pageInfo.setData(data);
        pageInfo.setTotalCount(jobPageData.getTotalCount());
        pageInfo.setPageNo(req.getPageNo());
        pageInfo.setPageSize(req.getPageSize());

        return pageInfo;
    }

    @Override
    public PageInfo<InstanceSimpleInfoRes> listInstance(InstanceListReq req) {
        // Search from scheduler.
        final InstanceListDto dto = InstanceListDto.builder()
                .name(req.getName())
                .pageNo(req.getPageNo())
                .pageSize(req.getPageSize())
                .build();
        final PageData<InstanceDto> instancePageData = iInstanceService.list(dto);
        final List<InstanceSimpleInfoRes> data = instancePageData.getData().stream().map(this::translate).collect(Collectors.toList());
        final PageInfo<InstanceSimpleInfoRes> pageInfo = new PageInfo<>();
        pageInfo.setData(data);
        pageInfo.setTotalCount(instancePageData.getTotalCount());
        pageInfo.setPageNo(req.getPageNo());
        pageInfo.setPageSize(req.getPageSize());

        return pageInfo;
    }

    @Override
    public InstanceSimpleInfoRes tmpExecute(ExecuteReq req) {
        final Script script = checkAndGetScript(req.getScriptId());

        final SchedulerConfigDto schedulerConfigDto = SchedulerConfigDto.builder()
                .retryInterval(RETRY_INTERVAL_DEFAULT)
                .retryTimes(RETRY_TIMES_DEFAULT)
                .startTime(new Date())
                .endTime(new Date())
                .triggerExpression(NEVER_TRIGGER_EXPRESSION)
                .build();

        final JobDto jobDto = JobDto.builder()
                .jobName(script.getName().concat(UNDERLINE).concat(String.valueOf(System.currentTimeMillis())))
                .jobContent(req.getContent())
                .params(req.getParams())
                .operatorId(req.getOperatorId())
                .schedulerConfigDto(schedulerConfigDto)
                //todo fix to real execute script
                .executorScript(script.getContent())
                .jobId(null)
                .build();

        final ExecuteDto dto = ExecuteDto.builder()
                .jobDto(jobDto)
                .executeTypeEnum(ExecuteTypeEnum.parse(req.getExecuteType()))
                .complementDataDto(null)
                .build();

        return this.translate(iJobService.execute(dto));
    }

    @Override
    public InstanceLogRes queryInstanceLog(Long instanceId) {
        final InstanceLogDto dto = iInstanceService.queryInstanceLog(instanceId);

        return InstanceLogRes.builder()
            .instanceId(instanceId)
            .logContent(dto.getLogContent())
            .build();
    }

    @Override
    public void kill(Long instanceId) {
        iJobService.kill(instanceId);
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
                .jobId(dto.getJobId())
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

    private Map<String, Object> getScriptParamMap(List<ScriptParam> scriptParams) {
        Map<String, Object> params = Maps.newHashMap();

        if (!CollectionUtils.isEmpty(params)) {
            scriptParams.forEach(scriptParam -> params.put(scriptParam.getKey(), scriptParam.getValue()));
        }
        return params;
    }
}
