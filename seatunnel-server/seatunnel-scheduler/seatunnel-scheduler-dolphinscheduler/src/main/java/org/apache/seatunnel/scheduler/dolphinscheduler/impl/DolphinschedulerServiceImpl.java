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

import static org.apache.seatunnel.scheduler.dolphinscheduler.constants.DolphinschedulerConstants.CODE;
import static org.apache.seatunnel.scheduler.dolphinscheduler.constants.DolphinschedulerConstants.CODE_SUCCESS;
import static org.apache.seatunnel.scheduler.dolphinscheduler.constants.DolphinschedulerConstants.CONDITION_PARAMS;
import static org.apache.seatunnel.scheduler.dolphinscheduler.constants.DolphinschedulerConstants.CONDITION_RESULT;
import static org.apache.seatunnel.scheduler.dolphinscheduler.constants.DolphinschedulerConstants.CONDITION_TYPE;
import static org.apache.seatunnel.scheduler.dolphinscheduler.constants.DolphinschedulerConstants.CONDITION_TYPE_DEFAULT;
import static org.apache.seatunnel.scheduler.dolphinscheduler.constants.DolphinschedulerConstants.CREATE_PROCESS_DEFINITION;
import static org.apache.seatunnel.scheduler.dolphinscheduler.constants.DolphinschedulerConstants.CREATE_SCHEDULE;
import static org.apache.seatunnel.scheduler.dolphinscheduler.constants.DolphinschedulerConstants.CRONTAB;
import static org.apache.seatunnel.scheduler.dolphinscheduler.constants.DolphinschedulerConstants.DATA;
import static org.apache.seatunnel.scheduler.dolphinscheduler.constants.DolphinschedulerConstants.DATA_TOTAL_LIST;
import static org.apache.seatunnel.scheduler.dolphinscheduler.constants.DolphinschedulerConstants.DEFAULT_FILE_SUFFIX;
import static org.apache.seatunnel.scheduler.dolphinscheduler.constants.DolphinschedulerConstants.DELAY_TIME;
import static org.apache.seatunnel.scheduler.dolphinscheduler.constants.DolphinschedulerConstants.DELAY_TIME_DEFAULT;
import static org.apache.seatunnel.scheduler.dolphinscheduler.constants.DolphinschedulerConstants.DEPENDENCE;
import static org.apache.seatunnel.scheduler.dolphinscheduler.constants.DolphinschedulerConstants.DEPENDENCE_DEFAULT;
import static org.apache.seatunnel.scheduler.dolphinscheduler.constants.DolphinschedulerConstants.DESCRIPTION;
import static org.apache.seatunnel.scheduler.dolphinscheduler.constants.DolphinschedulerConstants.DESCRIPTION_DEFAULT;
import static org.apache.seatunnel.scheduler.dolphinscheduler.constants.DolphinschedulerConstants.DRY_RUN;
import static org.apache.seatunnel.scheduler.dolphinscheduler.constants.DolphinschedulerConstants.END_TIME;
import static org.apache.seatunnel.scheduler.dolphinscheduler.constants.DolphinschedulerConstants.ENVIRONMENT_CODE;
import static org.apache.seatunnel.scheduler.dolphinscheduler.constants.DolphinschedulerConstants.ENVIRONMENT_CODE_DEFAULT;
import static org.apache.seatunnel.scheduler.dolphinscheduler.constants.DolphinschedulerConstants.FAILED_NODE;
import static org.apache.seatunnel.scheduler.dolphinscheduler.constants.DolphinschedulerConstants.FAILED_NODE_DEFAULT;
import static org.apache.seatunnel.scheduler.dolphinscheduler.constants.DolphinschedulerConstants.FAILURE_STRATEGY;
import static org.apache.seatunnel.scheduler.dolphinscheduler.constants.DolphinschedulerConstants.FAILURE_STRATEGY_DEFAULT;
import static org.apache.seatunnel.scheduler.dolphinscheduler.constants.DolphinschedulerConstants.FAIL_RETRY_INTERVAL;
import static org.apache.seatunnel.scheduler.dolphinscheduler.constants.DolphinschedulerConstants.FAIL_RETRY_TIMES;
import static org.apache.seatunnel.scheduler.dolphinscheduler.constants.DolphinschedulerConstants.FLAG;
import static org.apache.seatunnel.scheduler.dolphinscheduler.constants.DolphinschedulerConstants.FLAG_DEFAULT;
import static org.apache.seatunnel.scheduler.dolphinscheduler.constants.DolphinschedulerConstants.FULL_NAME;
import static org.apache.seatunnel.scheduler.dolphinscheduler.constants.DolphinschedulerConstants.GEN_NUM;
import static org.apache.seatunnel.scheduler.dolphinscheduler.constants.DolphinschedulerConstants.GEN_NUM_DEFAULT;
import static org.apache.seatunnel.scheduler.dolphinscheduler.constants.DolphinschedulerConstants.GEN_TASK_CODE_LIST;
import static org.apache.seatunnel.scheduler.dolphinscheduler.constants.DolphinschedulerConstants.LOCAL_PARAMS;
import static org.apache.seatunnel.scheduler.dolphinscheduler.constants.DolphinschedulerConstants.LOCAL_PARAMS_DIRECT;
import static org.apache.seatunnel.scheduler.dolphinscheduler.constants.DolphinschedulerConstants.LOCAL_PARAMS_DIRECT_DEFAULT;
import static org.apache.seatunnel.scheduler.dolphinscheduler.constants.DolphinschedulerConstants.LOCAL_PARAMS_PROP;
import static org.apache.seatunnel.scheduler.dolphinscheduler.constants.DolphinschedulerConstants.LOCAL_PARAMS_TYPE;
import static org.apache.seatunnel.scheduler.dolphinscheduler.constants.DolphinschedulerConstants.LOCAL_PARAMS_TYPE_DEFAULT;
import static org.apache.seatunnel.scheduler.dolphinscheduler.constants.DolphinschedulerConstants.LOCAL_PARAMS_VALUE;
import static org.apache.seatunnel.scheduler.dolphinscheduler.constants.DolphinschedulerConstants.LOCATIONS;
import static org.apache.seatunnel.scheduler.dolphinscheduler.constants.DolphinschedulerConstants.LOCATIONS_X;
import static org.apache.seatunnel.scheduler.dolphinscheduler.constants.DolphinschedulerConstants.LOCATIONS_X_DEFAULT;
import static org.apache.seatunnel.scheduler.dolphinscheduler.constants.DolphinschedulerConstants.LOCATIONS_Y;
import static org.apache.seatunnel.scheduler.dolphinscheduler.constants.DolphinschedulerConstants.LOCATIONS_Y_DEFAULT;
import static org.apache.seatunnel.scheduler.dolphinscheduler.constants.DolphinschedulerConstants.LOG_DETAIL;
import static org.apache.seatunnel.scheduler.dolphinscheduler.constants.DolphinschedulerConstants.LOG_LIMIT_NUM;
import static org.apache.seatunnel.scheduler.dolphinscheduler.constants.DolphinschedulerConstants.LOG_SKIP_LINE_NUM;
import static org.apache.seatunnel.scheduler.dolphinscheduler.constants.DolphinschedulerConstants.MSG;
import static org.apache.seatunnel.scheduler.dolphinscheduler.constants.DolphinschedulerConstants.ONLINE_CREATE_RESOURCE;
import static org.apache.seatunnel.scheduler.dolphinscheduler.constants.DolphinschedulerConstants.PAGE_NO;
import static org.apache.seatunnel.scheduler.dolphinscheduler.constants.DolphinschedulerConstants.PAGE_NO_DEFAULT;
import static org.apache.seatunnel.scheduler.dolphinscheduler.constants.DolphinschedulerConstants.PAGE_SIZE;
import static org.apache.seatunnel.scheduler.dolphinscheduler.constants.DolphinschedulerConstants.PAGE_SIZE_DEFAULT;
import static org.apache.seatunnel.scheduler.dolphinscheduler.constants.DolphinschedulerConstants.POST_TASK_CODE;
import static org.apache.seatunnel.scheduler.dolphinscheduler.constants.DolphinschedulerConstants.POST_TASK_VERSION;
import static org.apache.seatunnel.scheduler.dolphinscheduler.constants.DolphinschedulerConstants.POST_TASK_VERSION_DEFAULT;
import static org.apache.seatunnel.scheduler.dolphinscheduler.constants.DolphinschedulerConstants.PRE_TASK_CODE;
import static org.apache.seatunnel.scheduler.dolphinscheduler.constants.DolphinschedulerConstants.PRE_TASK_CODE_DEFAULT;
import static org.apache.seatunnel.scheduler.dolphinscheduler.constants.DolphinschedulerConstants.PRE_TASK_VERSION;
import static org.apache.seatunnel.scheduler.dolphinscheduler.constants.DolphinschedulerConstants.PRE_TASK_VERSION_DEFAULT;
import static org.apache.seatunnel.scheduler.dolphinscheduler.constants.DolphinschedulerConstants.PROCESS_DEFINITION;
import static org.apache.seatunnel.scheduler.dolphinscheduler.constants.DolphinschedulerConstants.PROCESS_DEFINITION_CODE;
import static org.apache.seatunnel.scheduler.dolphinscheduler.constants.DolphinschedulerConstants.PROCESS_DEFINITION_NAME;
import static org.apache.seatunnel.scheduler.dolphinscheduler.constants.DolphinschedulerConstants.PROCESS_INSTANCE_NAME;
import static org.apache.seatunnel.scheduler.dolphinscheduler.constants.DolphinschedulerConstants.PROCESS_INSTANCE_PRIORITY;
import static org.apache.seatunnel.scheduler.dolphinscheduler.constants.DolphinschedulerConstants.PROCESS_INSTANCE_PRIORITY_DEFAULT;
import static org.apache.seatunnel.scheduler.dolphinscheduler.constants.DolphinschedulerConstants.QUERY_LIST_PAGING;
import static org.apache.seatunnel.scheduler.dolphinscheduler.constants.DolphinschedulerConstants.QUERY_PROCESS_DEFINITION_BY_NAME;
import static org.apache.seatunnel.scheduler.dolphinscheduler.constants.DolphinschedulerConstants.QUERY_PROJECT_LIST_PAGING;
import static org.apache.seatunnel.scheduler.dolphinscheduler.constants.DolphinschedulerConstants.QUERY_RESOURCE;
import static org.apache.seatunnel.scheduler.dolphinscheduler.constants.DolphinschedulerConstants.QUERY_SCHEDULE_LIST_PAGING;
import static org.apache.seatunnel.scheduler.dolphinscheduler.constants.DolphinschedulerConstants.QUERY_TASK_LIST_PAGING;
import static org.apache.seatunnel.scheduler.dolphinscheduler.constants.DolphinschedulerConstants.RAW_SCRIPT;
import static org.apache.seatunnel.scheduler.dolphinscheduler.constants.DolphinschedulerConstants.RELEASE;
import static org.apache.seatunnel.scheduler.dolphinscheduler.constants.DolphinschedulerConstants.RELEASE_STATE;
import static org.apache.seatunnel.scheduler.dolphinscheduler.constants.DolphinschedulerConstants.RELEASE_STATE_OFFLINE;
import static org.apache.seatunnel.scheduler.dolphinscheduler.constants.DolphinschedulerConstants.RESOURCE_ID;
import static org.apache.seatunnel.scheduler.dolphinscheduler.constants.DolphinschedulerConstants.RESOURCE_ID_DEFAULT;
import static org.apache.seatunnel.scheduler.dolphinscheduler.constants.DolphinschedulerConstants.RESOURCE_LIST;
import static org.apache.seatunnel.scheduler.dolphinscheduler.constants.DolphinschedulerConstants.RESOURCE_SEPARATOR;
import static org.apache.seatunnel.scheduler.dolphinscheduler.constants.DolphinschedulerConstants.RESOURCE_TYPE;
import static org.apache.seatunnel.scheduler.dolphinscheduler.constants.DolphinschedulerConstants.RESOURCE_TYPE_FILE;
import static org.apache.seatunnel.scheduler.dolphinscheduler.constants.DolphinschedulerConstants.RESOURCE_TYPE_FILE_CONTENT;
import static org.apache.seatunnel.scheduler.dolphinscheduler.constants.DolphinschedulerConstants.RESOURCE_TYPE_FILE_SUFFIX_DEFAULT;
import static org.apache.seatunnel.scheduler.dolphinscheduler.constants.DolphinschedulerConstants.RUN_MODE_DEFAULT;
import static org.apache.seatunnel.scheduler.dolphinscheduler.constants.DolphinschedulerConstants.SCHEDULE;
import static org.apache.seatunnel.scheduler.dolphinscheduler.constants.DolphinschedulerConstants.SCHEDULE_ID;
import static org.apache.seatunnel.scheduler.dolphinscheduler.constants.DolphinschedulerConstants.SCHEDULE_OFFLINE;
import static org.apache.seatunnel.scheduler.dolphinscheduler.constants.DolphinschedulerConstants.SCHEDULE_ONLINE;
import static org.apache.seatunnel.scheduler.dolphinscheduler.constants.DolphinschedulerConstants.SEARCH_VAL;
import static org.apache.seatunnel.scheduler.dolphinscheduler.constants.DolphinschedulerConstants.START_PROCESS_INSTANCE;
import static org.apache.seatunnel.scheduler.dolphinscheduler.constants.DolphinschedulerConstants.START_TIME;
import static org.apache.seatunnel.scheduler.dolphinscheduler.constants.DolphinschedulerConstants.SUCCESS_NODE;
import static org.apache.seatunnel.scheduler.dolphinscheduler.constants.DolphinschedulerConstants.SUCCESS_NODE_DEFAULT;
import static org.apache.seatunnel.scheduler.dolphinscheduler.constants.DolphinschedulerConstants.SWITCH_RESULT;
import static org.apache.seatunnel.scheduler.dolphinscheduler.constants.DolphinschedulerConstants.SWITCH_RESULT_DEFAULT;
import static org.apache.seatunnel.scheduler.dolphinscheduler.constants.DolphinschedulerConstants.TASK_CODE;
import static org.apache.seatunnel.scheduler.dolphinscheduler.constants.DolphinschedulerConstants.TASK_DEFINITION_JSON;
import static org.apache.seatunnel.scheduler.dolphinscheduler.constants.DolphinschedulerConstants.TASK_DEFINITION_JSON_CODE;
import static org.apache.seatunnel.scheduler.dolphinscheduler.constants.DolphinschedulerConstants.TASK_DEFINITION_JSON_NAME;
import static org.apache.seatunnel.scheduler.dolphinscheduler.constants.DolphinschedulerConstants.TASK_DEPEND_TYPE_DEFAULT;
import static org.apache.seatunnel.scheduler.dolphinscheduler.constants.DolphinschedulerConstants.TASK_INSTANCE_ID;
import static org.apache.seatunnel.scheduler.dolphinscheduler.constants.DolphinschedulerConstants.TASK_PARAMS;
import static org.apache.seatunnel.scheduler.dolphinscheduler.constants.DolphinschedulerConstants.TASK_PRIORITY;
import static org.apache.seatunnel.scheduler.dolphinscheduler.constants.DolphinschedulerConstants.TASK_PRIORITY_DEFAULT;
import static org.apache.seatunnel.scheduler.dolphinscheduler.constants.DolphinschedulerConstants.TASK_RELATION_JSON;
import static org.apache.seatunnel.scheduler.dolphinscheduler.constants.DolphinschedulerConstants.TASK_RELATION_JSON_NAME;
import static org.apache.seatunnel.scheduler.dolphinscheduler.constants.DolphinschedulerConstants.TASK_TYPE;
import static org.apache.seatunnel.scheduler.dolphinscheduler.constants.DolphinschedulerConstants.TASK_TYPE_DEFAULT;
import static org.apache.seatunnel.scheduler.dolphinscheduler.constants.DolphinschedulerConstants.TENANT_CODE;
import static org.apache.seatunnel.scheduler.dolphinscheduler.constants.DolphinschedulerConstants.TIMEOUT;
import static org.apache.seatunnel.scheduler.dolphinscheduler.constants.DolphinschedulerConstants.TIMEOUT_DEFAULT;
import static org.apache.seatunnel.scheduler.dolphinscheduler.constants.DolphinschedulerConstants.TIMEOUT_FLAG;
import static org.apache.seatunnel.scheduler.dolphinscheduler.constants.DolphinschedulerConstants.TIMEOUT_FLAG_DEFAULT;
import static org.apache.seatunnel.scheduler.dolphinscheduler.constants.DolphinschedulerConstants.TIMEOUT_NOTIFY_STRATEGY;
import static org.apache.seatunnel.scheduler.dolphinscheduler.constants.DolphinschedulerConstants.TIMEOUT_NOTIFY_STRATEGY_DEFAULT;
import static org.apache.seatunnel.scheduler.dolphinscheduler.constants.DolphinschedulerConstants.TIMEZONE_ID;
import static org.apache.seatunnel.scheduler.dolphinscheduler.constants.DolphinschedulerConstants.TIMEZONE_ID_DEFAULT;
import static org.apache.seatunnel.scheduler.dolphinscheduler.constants.DolphinschedulerConstants.TOKEN;
import static org.apache.seatunnel.scheduler.dolphinscheduler.constants.DolphinschedulerConstants.UPDATE_CONTENT;
import static org.apache.seatunnel.scheduler.dolphinscheduler.constants.DolphinschedulerConstants.VERSION;
import static org.apache.seatunnel.scheduler.dolphinscheduler.constants.DolphinschedulerConstants.VERSION_DEFAULT;
import static org.apache.seatunnel.scheduler.dolphinscheduler.constants.DolphinschedulerConstants.WAIT_START_TIMEOUT;
import static org.apache.seatunnel.scheduler.dolphinscheduler.constants.DolphinschedulerConstants.WAIT_START_TIMEOUT_DEFAULT;
import static org.apache.seatunnel.scheduler.dolphinscheduler.constants.DolphinschedulerConstants.WARNING_GROUP_ID;
import static org.apache.seatunnel.scheduler.dolphinscheduler.constants.DolphinschedulerConstants.WARNING_GROUP_ID_DEFAULT;
import static org.apache.seatunnel.scheduler.dolphinscheduler.constants.DolphinschedulerConstants.WARNING_TYPE;
import static org.apache.seatunnel.scheduler.dolphinscheduler.constants.DolphinschedulerConstants.WARNING_TYPE_DEFAULT;
import static org.apache.seatunnel.scheduler.dolphinscheduler.constants.DolphinschedulerConstants.WORKER_GROUP;
import static org.apache.seatunnel.scheduler.dolphinscheduler.constants.DolphinschedulerConstants.WORKER_GROUP_DEFAULT;
import static org.apache.seatunnel.scheduler.dolphinscheduler.utils.HttpUtils.createParamMap;
import static org.apache.seatunnel.server.common.Constants.BLANK_SPACE;
import static org.apache.seatunnel.server.common.SeatunnelErrorEnum.NO_MATCHED_PROJECT;
import static org.apache.seatunnel.server.common.SeatunnelErrorEnum.UNEXPECTED_RETURN_CODE;

import org.apache.seatunnel.scheduler.dolphinscheduler.IDolphinschedulerService;
import org.apache.seatunnel.scheduler.dolphinscheduler.dto.ListProcessDefinitionDto;
import org.apache.seatunnel.scheduler.dolphinscheduler.dto.ListProcessInstanceDto;
import org.apache.seatunnel.scheduler.dolphinscheduler.dto.OnlineCreateResourceDto;
import org.apache.seatunnel.scheduler.dolphinscheduler.dto.ProcessDefinitionDto;
import org.apache.seatunnel.scheduler.dolphinscheduler.dto.ProjectDto;
import org.apache.seatunnel.scheduler.dolphinscheduler.dto.ResourceDto;
import org.apache.seatunnel.scheduler.dolphinscheduler.dto.SchedulerDto;
import org.apache.seatunnel.scheduler.dolphinscheduler.dto.StartProcessDefinitionDto;
import org.apache.seatunnel.scheduler.dolphinscheduler.dto.TaskDefinitionDto;
import org.apache.seatunnel.scheduler.dolphinscheduler.dto.TaskInstanceDto;
import org.apache.seatunnel.scheduler.dolphinscheduler.dto.UpdateProcessDefinitionDto;
import org.apache.seatunnel.scheduler.dolphinscheduler.utils.HttpUtils;
import org.apache.seatunnel.server.common.DateUtils;
import org.apache.seatunnel.server.common.SeatunnelErrorEnum;
import org.apache.seatunnel.server.common.SeatunnelException;
import org.apache.seatunnel.spi.scheduler.dto.InstanceLogDto;
import org.apache.seatunnel.spi.scheduler.dto.JobDto;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.gson.Gson;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.MapUtils;
import org.jsoup.Connection;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import org.springframework.util.CollectionUtils;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

@Slf4j
@Component
public class DolphinschedulerServiceImpl implements IDolphinschedulerService, InitializingBean {

    private static final Gson GSON = new Gson();
    @Value("${ds.api.prefix}")
    private String apiPrefix;
    @Value("${ds.api.token}")
    private String token;
    @Value("${ds.tenant.default}")
    private String defaultTenantName;
    @Value("${ds.project.default}")
    private String defaultProjectName;
    @Value("${ds.script.dir}")
    private String defaultScriptDir;
    private long defaultProjectCode;

    @Override
    public void afterPropertiesSet() throws Exception {
        final ProjectDto projectDto = queryProjectCodeByName(defaultProjectName);
        defaultProjectCode = projectDto.getCode();
    }

    private ProjectDto queryProjectCodeByName(String projectName) throws IOException {
        final Map result = HttpUtils.builder()
                .withUrl(apiPrefix.concat(QUERY_PROJECT_LIST_PAGING))
                .withMethod(Connection.Method.GET)
                .withData(createParamMap(SEARCH_VAL, projectName, PAGE_NO, PAGE_NO_DEFAULT, PAGE_SIZE, PAGE_SIZE_DEFAULT))
                .withToken(TOKEN, token)
                .execute(Map.class);
        checkResult(result, false);

        final List<Map<String, Object>> projectList = (List<Map<String, Object>>) MapUtils.getMap(result, DATA).get(DATA_TOTAL_LIST);
        final ProjectDto projectDto = projectList.stream().map(ProjectDto::fromMap).filter(p -> p.getName().equalsIgnoreCase(projectName)).findAny().orElse(null);
        if (Objects.isNull(projectDto)) {
            throw new SeatunnelException(NO_MATCHED_PROJECT, projectName);
        }
        return projectDto;
    }

    @Override
    public ProcessDefinitionDto createOrUpdateProcessDefinition(UpdateProcessDefinitionDto dto) {
        // gen task code
        final List<Long> taskCodes = genTaskCodes(defaultProjectCode, GEN_NUM_DEFAULT);

        // build taskDefinitionJson and taskRelationJson.
        final Long taskCode = taskCodes.get(0);
        List<Map<String, Object>> taskDefinitionJson = buildTaskDefinitionJson(taskCode, dto.getTaskDefinitionDto());
        List<Map<String, Object>> taskRelationJson = buildTaskRelationJson(taskCode, dto.getTaskDefinitionDto());
        List<Map<String, Object>> locations = buildLocation(taskCodes);

        String url = apiPrefix.concat(String.format(CREATE_PROCESS_DEFINITION, defaultProjectCode));
        Connection.Method method = Connection.Method.POST;
        if (Objects.nonNull(dto.getProcessDefinitionCode())) {
            method = Connection.Method.PUT;
            url = url.concat(String.valueOf(dto.getProcessDefinitionCode()));
            // offline process
            updateProcessDefinitionState(dto.getProcessDefinitionCode(), dto.getName(), RELEASE_STATE_OFFLINE);
        }

        final Map<String, String> paramMap = createParamMap(LOCATIONS, locations,
                TASK_DEFINITION_JSON, GSON.toJson(taskDefinitionJson),
                TASK_RELATION_JSON, GSON.toJson(taskRelationJson),
                TENANT_CODE, defaultTenantName,
                PROCESS_DEFINITION_NAME, dto.getName());

        final Map result = HttpUtils.builder()
                .withUrl(url)
                .withMethod(method)
                .withData(paramMap)
                .withRequestBody(GSON.toJson(null))
                .withToken(TOKEN, token)
                .execute(Map.class);

        checkResult(result, false);
        final Map<String, Object> map = MapUtils.getMap(result, DATA);
        return ProcessDefinitionDto.fromMap(map);
    }

    private List<Map<String, Object>> buildTaskDefinitionJson(Long taskCode, TaskDefinitionDto taskDefinitionDto) {
        final ResourceDto resourceDto = createOrUpdateScriptContent(taskDefinitionDto.getName(), taskDefinitionDto.getContent());
        final Map<String, Object> map = Maps.newHashMap();
        map.put(TASK_DEFINITION_JSON_CODE, taskCode);
        map.put(TASK_DEFINITION_JSON_NAME, taskDefinitionDto.getName());
        map.put(DESCRIPTION, DESCRIPTION_DEFAULT);
        map.put(TASK_TYPE, TASK_TYPE_DEFAULT);

        Map<String, Object> taskParamsMap = Maps.newHashMap();
        taskParamsMap.put(RESOURCE_LIST, ImmutableList.of(Collections.singletonMap(RESOURCE_ID, resourceDto.getId())));

        final Map<String, Object> params = taskDefinitionDto.getParams();
        final List<Map<String, Object>> localParams = Lists.newArrayListWithCapacity(params.size());
        params.forEach((k, v) -> localParams.add(ImmutableMap.of(LOCAL_PARAMS_PROP, k, LOCAL_PARAMS_DIRECT, LOCAL_PARAMS_DIRECT_DEFAULT,
                LOCAL_PARAMS_TYPE, LOCAL_PARAMS_TYPE_DEFAULT, LOCAL_PARAMS_VALUE, v)));
        taskParamsMap.put(LOCAL_PARAMS, localParams);

        taskParamsMap.put(RAW_SCRIPT, taskDefinitionDto.getExecuteScript());
        taskParamsMap.put(DEPENDENCE, DEPENDENCE_DEFAULT);
        taskParamsMap.put(CONDITION_RESULT, ImmutableMap.of(SUCCESS_NODE, SUCCESS_NODE_DEFAULT, FAILED_NODE, FAILED_NODE_DEFAULT));
        taskParamsMap.put(WAIT_START_TIMEOUT, WAIT_START_TIMEOUT_DEFAULT);
        taskParamsMap.put(SWITCH_RESULT, SWITCH_RESULT_DEFAULT);
        map.put(TASK_PARAMS, taskParamsMap);

        map.put(FLAG, FLAG_DEFAULT);
        map.put(TASK_PRIORITY, TASK_PRIORITY_DEFAULT);
        map.put(WORKER_GROUP, WORKER_GROUP_DEFAULT);
        map.put(FAIL_RETRY_TIMES, taskDefinitionDto.getRetryTimes());
        map.put(FAIL_RETRY_INTERVAL, taskDefinitionDto.getRetryInterval());
        map.put(TIMEOUT_FLAG, TIMEOUT_FLAG_DEFAULT);
        map.put(TIMEOUT_NOTIFY_STRATEGY, TIMEOUT_NOTIFY_STRATEGY_DEFAULT);
        map.put(TIMEOUT, TIMEOUT_DEFAULT);
        map.put(DELAY_TIME, DELAY_TIME_DEFAULT);
        map.put(ENVIRONMENT_CODE, ENVIRONMENT_CODE_DEFAULT);
        map.put(VERSION, VERSION_DEFAULT);
        return Collections.singletonList(map);
    }

    private List<Map<String, Object>> buildTaskRelationJson(Long taskCode, TaskDefinitionDto taskDefinitionDto) {
        Map<String, Object> map = Maps.newHashMap();
        map.put(TASK_RELATION_JSON_NAME, BLANK_SPACE);
        map.put(PRE_TASK_CODE, PRE_TASK_CODE_DEFAULT);
        map.put(PRE_TASK_VERSION, PRE_TASK_VERSION_DEFAULT);
        map.put(POST_TASK_CODE, taskCode);
        map.put(POST_TASK_VERSION, POST_TASK_VERSION_DEFAULT);
        map.put(CONDITION_TYPE, CONDITION_TYPE_DEFAULT);
        map.put(CONDITION_PARAMS, null);
        return Collections.singletonList(map);
    }

    private List<Map<String, Object>> buildLocation(List<Long> taskCode) {
        final List<Map<String, Object>> locations = Lists.newArrayListWithCapacity(taskCode.size());
        for (int i = 0; i < taskCode.size(); i++) {
            final Map<String, Object> map = Maps.newHashMap();
            map.put(TASK_CODE, taskCode);
            map.put(LOCATIONS_X, LOCATIONS_X_DEFAULT * i);
            map.put(LOCATIONS_Y, LOCATIONS_Y_DEFAULT * i);
            locations.add(map);
        }
        return locations;
    }

    @Override
    public List<ProcessDefinitionDto> listProcessDefinition(ListProcessDefinitionDto dto) {
        final Map result = HttpUtils.builder()
                .withUrl(apiPrefix.concat(String.format(QUERY_LIST_PAGING, defaultProjectCode)))
                .withMethod(Connection.Method.GET)
                .withData(createParamMap(SEARCH_VAL, dto.getName(), PAGE_NO, dto.getPageNo(), PAGE_SIZE, dto.getPageSize()))
                .withToken(TOKEN, token)
                .execute(Map.class);
        checkResult(result, false);

        final List<Map<String, Object>> processDefinitionList = (List<Map<String, Object>>) MapUtils.getMap(result, DATA).get(DATA_TOTAL_LIST);
        if (CollectionUtils.isEmpty(processDefinitionList)) {
            return Collections.emptyList();
        }
        return processDefinitionList.stream().map(ProcessDefinitionDto::fromMap).collect(Collectors.toList());
    }

    @Override
    public ProcessDefinitionDto fetchProcessDefinitionByName(String processDefinitionName) {
        final Map result = HttpUtils.builder()
                .withUrl(apiPrefix.concat(String.format(QUERY_PROCESS_DEFINITION_BY_NAME, defaultProjectCode)))
                .withMethod(Connection.Method.GET)
                .withData(createParamMap(PROCESS_DEFINITION_NAME, processDefinitionName))
                .withToken(TOKEN, token)
                .execute(Map.class);
        checkResult(result, false);

        final Map<String, Object> map = (Map<String, Object>) MapUtils.getMap(result, DATA).get(PROCESS_DEFINITION);
        return ProcessDefinitionDto.fromMap(map);
    }

    @Override
    public void startProcessDefinition(long processDefinitionCode) {
        final StartProcessDefinitionDto dto = StartProcessDefinitionDto.builder()
                .processDefinitionCode(processDefinitionCode)
                .failureStrategy(FAILURE_STRATEGY_DEFAULT)
                .warningType(WARNING_TYPE_DEFAULT)
                .warningGroupId(WARNING_GROUP_ID_DEFAULT)
                .taskDependType(TASK_DEPEND_TYPE_DEFAULT)
                .runMode(RUN_MODE_DEFAULT)
                .processInstancePriority(PROCESS_INSTANCE_PRIORITY_DEFAULT)
                .workerGroup(WORKER_GROUP_DEFAULT)
                .dryRun(DRY_RUN)
                .build();

        final Map result = HttpUtils.builder()
                .withUrl(apiPrefix.concat(String.format(START_PROCESS_INSTANCE, processDefinitionCode)))
                .withMethod(Connection.Method.POST)
                .withRequestBody(GSON.toJson(dto))
                .withToken(TOKEN, token)
                .execute(Map.class);
        checkResult(result, false);
    }

    @Override
    public void updateProcessDefinitionState(long processDefinitionCode, String processDefinitionName, String state) {
        final Map result = HttpUtils.builder()
                .withUrl(apiPrefix.concat(String.format(RELEASE, defaultProjectCode, processDefinitionCode)))
                .withMethod(Connection.Method.POST)
                .withData(createParamMap(PROCESS_DEFINITION_NAME, processDefinitionName, RELEASE_STATE, state))
                .withRequestBody(GSON.toJson(null))
                .withToken(TOKEN, token)
                .execute(Map.class);
        checkResult(result, false);
    }

    @Override
    public SchedulerDto createOrUpdateSchedule(JobDto dto) {
        final Map<String, Object> map = Maps.newHashMap();
        map.put(FAILURE_STRATEGY, FAILURE_STRATEGY_DEFAULT);
        map.put(WARNING_TYPE, WARNING_TYPE_DEFAULT);
        map.put(PROCESS_INSTANCE_PRIORITY, PROCESS_INSTANCE_PRIORITY_DEFAULT);
        map.put(WARNING_GROUP_ID, WARNING_GROUP_ID_DEFAULT);
        map.put(WORKER_GROUP, WORKER_GROUP_DEFAULT);
        map.put(ENVIRONMENT_CODE, ENVIRONMENT_CODE_DEFAULT);
        map.put(PROCESS_DEFINITION_CODE, dto.getJobId());

        final Map<String, Object> schedule = Maps.newHashMap();
        schedule.put(START_TIME, DateUtils.format(dto.getSchedulerConfigDto().getStartTime()));
        schedule.put(END_TIME, DateUtils.format(dto.getSchedulerConfigDto().getEndTime()));
        schedule.put(CRONTAB, dto.getSchedulerConfigDto().getTriggerExpression());
        schedule.put(TIMEZONE_ID, TIMEZONE_ID_DEFAULT);

        map.put(SCHEDULE, GSON.toJson(schedule));

        String url = String.format(CREATE_SCHEDULE, defaultProjectCode);

        final List<SchedulerDto> schedulerDtos = listSchedule(dto.getJobId());
        boolean flag = false;
        if (!CollectionUtils.isEmpty(schedulerDtos)) {
            url = url.concat(String.valueOf(schedulerDtos.get(0).getId()));
            flag = true;
        }

        final Map result = HttpUtils.builder()
                .withUrl(apiPrefix.concat(url))
                .withData(translate(map))
                .withMethod(flag ? Connection.Method.PUT : Connection.Method.POST)
                .withRequestBody(GSON.toJson(null))
                .withToken(TOKEN, token)
                .execute(Map.class);
        checkResult(result, false);

        if (flag){
            schedulerDtos.clear();
            schedulerDtos.addAll(listSchedule(dto.getJobId()));
            return schedulerDtos.get(0);
        }

        Map<String, Object> resultMap = MapUtils.getMap(result, DATA);
        final SchedulerDto schedulerDto = SchedulerDto.fromMap(resultMap);
        return schedulerDto;
    }

    @Override
    public List<SchedulerDto> listSchedule(long processDefinitionCode) {
        final Map result = HttpUtils.builder()
                .withUrl(apiPrefix.concat(String.format(QUERY_SCHEDULE_LIST_PAGING, defaultProjectCode)))
                .withMethod(Connection.Method.GET)
                .withData(createParamMap(PROCESS_DEFINITION_CODE, processDefinitionCode, PAGE_NO, PAGE_NO_DEFAULT, PAGE_SIZE, PAGE_SIZE_DEFAULT))
                .withToken(TOKEN, token)
                .execute(Map.class);
        checkResult(result, false);

        final List<Map<String, Object>> scheduleList = (List<Map<String, Object>>) MapUtils.getMap(result, DATA).get(DATA_TOTAL_LIST);
        if (CollectionUtils.isEmpty(scheduleList)) {
            return Collections.emptyList();
        }
        return scheduleList.stream().map(SchedulerDto::fromMap).collect(Collectors.toList());
    }

    @Override
    public void scheduleOnline(int scheduleId) {
        final Map result = HttpUtils.builder()
                .withUrl(apiPrefix.concat(String.format(SCHEDULE_ONLINE, defaultProjectCode, scheduleId)))
                .withMethod(Connection.Method.POST)
                .withData(createParamMap(SCHEDULE_ID, scheduleId))
                .withToken(TOKEN, token)
                .execute(Map.class);
        checkResult(result, false);
    }

    @Override
    public void scheduleOffline(int scheduleId) {
        final Map result = HttpUtils.builder()
                .withUrl(apiPrefix.concat(String.format(SCHEDULE_OFFLINE, defaultProjectCode, scheduleId)))
                .withMethod(Connection.Method.POST)
                .withData(createParamMap(SCHEDULE_ID, scheduleId))
                .withToken(TOKEN, token)
                .execute(Map.class);
        checkResult(result, false);
    }

    @Override
    public List<Long> genTaskCodes(long projectCode, int num) {
        final String url = String.format(GEN_TASK_CODE_LIST, projectCode);
        final Map result = HttpUtils.builder()
                .withUrl(apiPrefix.concat(url))
                .withMethod(Connection.Method.GET)
                .withData(createParamMap(GEN_NUM, num))
                .withToken(TOKEN, token)
                .execute(Map.class);
        checkResult(result, false);

        return (List<Long>) result.get(DATA);
    }

    @Override
    public ResourceDto createOrUpdateScriptContent(String resourceName, String content) {
        // check resource exists
        final String fullName = defaultScriptDir.concat(RESOURCE_SEPARATOR.concat(resourceName));
        final ResourceDto parentResourceDto = getResourceDto(defaultScriptDir, RESOURCE_TYPE_FILE);
        if (Objects.isNull(parentResourceDto)) {
            throw new SeatunnelException(SeatunnelErrorEnum.NO_MATCHED_SCRIPT_SAVE_DIR, defaultScriptDir);
        }
        final ResourceDto dto = getResourceDto(fullName.concat(DEFAULT_FILE_SUFFIX), RESOURCE_TYPE_FILE);
        if (Objects.isNull(dto)) {
            final OnlineCreateResourceDto createDto = OnlineCreateResourceDto.builder()
                    .type(RESOURCE_TYPE_FILE)
                    .pid(parentResourceDto.getId())
                    .fileName(resourceName)
                    .currentDir(defaultScriptDir)
                    .suffix(RESOURCE_TYPE_FILE_SUFFIX_DEFAULT)
                    .content(content)
                    .build();
            onlineCreateResource(createDto);
            return getResourceDto(fullName.concat(DEFAULT_FILE_SUFFIX), RESOURCE_TYPE_FILE);
        } else {
            updateContent(dto.getId(), content);
            return dto;
        }
    }

    private void updateContent(int id, String content) {
        final Map result = HttpUtils.builder()
                .withUrl(apiPrefix.concat(String.format(UPDATE_CONTENT, id)))
                .withMethod(Connection.Method.PUT)
                .withData(createParamMap(RESOURCE_ID, id, RESOURCE_TYPE_FILE_CONTENT, content))
                .withRequestBody(GSON.toJson(null))
                .withToken(TOKEN, token)
                .execute(Map.class);
        checkResult(result, false);
    }

    private void onlineCreateResource(OnlineCreateResourceDto createDto) {
        final Map<String, Object> map = createDto.toMap();
        final Map result = HttpUtils.builder()
                .withUrl(apiPrefix.concat(ONLINE_CREATE_RESOURCE))
                .withMethod(Connection.Method.POST)
                .withData(translate(map))
                .withRequestBody(GSON.toJson(null))
                .withToken(TOKEN, token)
                .execute(Map.class);
        checkResult(result, false);
    }

    private ResourceDto getResourceDto(String fullName, String fileType) {
        final Map result = HttpUtils.builder()
                .withUrl(apiPrefix.concat(String.format(QUERY_RESOURCE, RESOURCE_ID_DEFAULT)))
                .withMethod(Connection.Method.GET)
                .withData(createParamMap(FULL_NAME, fullName, RESOURCE_TYPE, RESOURCE_TYPE_FILE))
                .withToken(TOKEN, token)
                .execute(Map.class);
        final int code = checkResult(result, true);
        if (code != CODE_SUCCESS) {
            return null;
        }
        final Map<String, Object> map = MapUtils.getMap(result, DATA);
        return ResourceDto.fromMap(map);
    }

    public InstanceLogDto getInstanceLog(long instanceId, int skipNum, int limitNum) {
        final Map result = HttpUtils.builder()
                .withUrl(apiPrefix.concat(LOG_DETAIL))
                .withMethod(Connection.Method.GET)
                .withData(createParamMap(TASK_INSTANCE_ID, instanceId, LOG_SKIP_LINE_NUM, skipNum, LOG_LIMIT_NUM, limitNum))
                .withToken(TOKEN, token)
                .execute(Map.class);
        checkResult(result, false);
        final String logContent = MapUtils.getString(result, DATA);
        return InstanceLogDto.builder()
                .lastSkipNum(skipNum)
                .lastLimitNum(limitNum)
                .instanceId(instanceId)
                .content(logContent)
                .build();
    }

    @Override
    public List<TaskInstanceDto> listTaskInstance(ListProcessInstanceDto dto) {
        final Map result = HttpUtils.builder()
                .withUrl(apiPrefix.concat(String.format(QUERY_TASK_LIST_PAGING, defaultProjectCode)))
                .withMethod(Connection.Method.GET)
                .withData(createParamMap(PROCESS_INSTANCE_NAME, dto.getProcessInstanceName(), PAGE_NO, dto.getPageNo(), PAGE_SIZE, dto.getPageSize()))
                .withToken(TOKEN, token)
                .execute(Map.class);

        checkResult(result, false);
        final List<Map<String, Object>> taskInstanceList = (List<Map<String, Object>>) MapUtils.getMap(result, DATA).get(DATA_TOTAL_LIST);
        if (CollectionUtils.isEmpty(taskInstanceList)) {
            return Collections.emptyList();
        }

        return taskInstanceList.stream().map(TaskInstanceDto::fromMap).collect(Collectors.toList());
    }

    public int checkResult(Map result, boolean ignore) {
        final int intValue = MapUtils.getIntValue(result, CODE, -1);
        if (!ignore && CODE_SUCCESS != intValue) {
            final String msg = MapUtils.getString(result, MSG);
            throw new SeatunnelException(UNEXPECTED_RETURN_CODE, intValue, msg);
        }
        return intValue;
    }

    private Map<String, String> translate(Map<String, Object> origin) {
        final HashMap<String, String> map = Maps.newHashMapWithExpectedSize(origin.size());
        origin.forEach((k, v) -> map.put(k, String.valueOf(v)));
        return map;
    }
}
