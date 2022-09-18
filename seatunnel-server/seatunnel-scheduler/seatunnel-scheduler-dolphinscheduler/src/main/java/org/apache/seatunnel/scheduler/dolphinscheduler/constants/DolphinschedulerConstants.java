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

package org.apache.seatunnel.scheduler.dolphinscheduler.constants;

import java.util.Collections;
import java.util.List;
import java.util.Map;

public class DolphinschedulerConstants {

    /**
     * api url
     */
    public static final String QUERY_PROJECT_LIST_PAGING = "/projects";
    public static final String QUERY_LIST_PAGING = "/projects/%s/process-definition";
    public static final String GEN_TASK_CODE_LIST = "/projects/%s/task-definition/gen-task-codes";
    public static final String VERIFY_RESOURCE_NAME = "/resources/verify-name";
    public static final String QUERY_RESOURCE = "/resources/%s";
    public static final String ONLINE_CREATE_RESOURCE = "/resources/online-create";
    public static final String UPDATE_CONTENT = "/resources/%s/update-content";
    public static final String LOG_DETAIL = "/log/detail";
    public static final String RELEASE = "/projects/%s/process-definition/%s/release";
    public static final String START_PROCESS_INSTANCE = "/projects/%s/executors/start-process-instance";
    public static final String QUERY_PROCESS_DEFINITION_BY_NAME = "/projects/%s/process-definition/query-by-name";
    public static final String QUERY_TASK_LIST_PAGING = "/projects/%s/task-instances";
    public static final String CREATE_PROCESS_DEFINITION = "/projects/%s/process-definition/";
    public static final String CREATE_SCHEDULE = "/projects/%s/schedules/";
    public static final String QUERY_SCHEDULE_LIST_PAGING = "/projects/%s/schedules";
    public static final String SCHEDULE_ONLINE = "/projects/%s/schedules/%s/online";
    public static final String SCHEDULE_OFFLINE = "/projects/%s/schedules/%s/offline";
    public static final String DELETE_PROCESS_DEFINITION = "/projects/%s/process-definition/%s";
    public static final String PROCESS_INSTANCE_LIST = "/projects/%s/process-instances";
    public static final String EXECUTE = "/projects/%s/executors/execute";

    /**
     * request param
     */
    public static final String TOKEN = "token";
    public static final String SEARCH_VAL = "searchVal";
    public static final String PAGE_SIZE = "pageSize";
    public static final int PAGE_SIZE_DEFAULT = 10;
    public static final int PAGE_SIZE_MIN = 1;
    public static final String PAGE_NO = "pageNo";
    public static final int PAGE_NO_DEFAULT = 1;
    public static final String GEN_NUM = "genNum";
    public static final int GEN_NUM_DEFAULT = 1;
    public static final String RESOURCE_SEPARATOR = "/";
    public static final String FULL_NAME = "fullName";
    public static final String RESOURCE_TYPE = "type";
    public static final String RESOURCE_TYPE_FILE = "FILE";
    public static final String RESOURCE_TYPE_FILE_SUFFIX = "suffix";
    public static final String RESOURCE_TYPE_FILE_SUFFIX_DEFAULT = "conf";
    public static final String RESOURCE_TYPE_FILE_CONTENT = "content";
    public static final String RESOURCE_TYPE_UDF = "UDF";
    public static final int RESOURCE_ID_DEFAULT = 0;
    public static final String RESOURCE_ID = "id";
    public static final String TASK_INSTANCE_ID = "taskInstanceId";
    public static final String LOG_SKIP_LINE_NUM = "skipLineNum";
    public static final String LOG_LIMIT_NUM = "limit";
    public static final String PROCESS_DEFINITION_NAME = "name";
    public static final String RELEASE_STATE = "releaseState";
    public static final String RELEASE_STATE_ONLINE = "ONLINE";
    public static final String RELEASE_STATE_OFFLINE = "OFFLINE";
    public static final String FAILURE_STRATEGY = "failureStrategy";
    public static final String FAILURE_STRATEGY_DEFAULT = "CONTINUE";
    public static final String WORKER_GROUP = "workerGroup";
    public static final String WORKER_GROUP_DEFAULT = "default";
    public static final String WARNING_TYPE = "warningType";
    public static final String WARNING_TYPE_DEFAULT = "NONE";
    public static final String WARNING_GROUP_ID = "warningGroupId";
    public static final int WARNING_GROUP_ID_DEFAULT = 0;
    public static final String TASK_DEPEND_TYPE_DEFAULT = "TASK_POST";
    public static final String RUN_MODE_DEFAULT = "RUN_MODE_SERIAL";
    public static final String RUN_MODE_PARALLEL = "RUN_MODE_PARALLEL";
    public static final String PROCESS_INSTANCE_PRIORITY = "processInstancePriority";
    public static final String PROCESS_INSTANCE_PRIORITY_DEFAULT = "MEDIUM";
    public static final int DRY_RUN = 0;
    public static final String PROCESS_DEFINITION = "processDefinition";
    public static final String PROCESS_INSTANCE_NAME = "processInstanceName";
    public static final String LOCATIONS = "locations";
    public static final String LOCATIONS_X = "x";
    public static final int LOCATIONS_X_DEFAULT = 200;
    public static final String LOCATIONS_Y = "y";
    public static final int LOCATIONS_Y_DEFAULT = 200;
    public static final String TASK_CODE = "taskCode";
    public static final String TASK_DEFINITION_JSON = "taskDefinitionJson";
    public static final String TASK_RELATION_JSON = "taskRelationJson";
    public static final String TENANT_CODE = "tenantCode";
    public static final String TASK_RELATION_JSON_NAME = "name";
    public static final String PRE_TASK_CODE = "preTaskCode";
    public static final long PRE_TASK_CODE_DEFAULT = 0;
    public static final String PRE_TASK_VERSION = "preTaskVersion";
    public static final int PRE_TASK_VERSION_DEFAULT = 0;
    public static final String POST_TASK_CODE = "postTaskCode";
    public static final String POST_TASK_VERSION = "postTaskVersion";
    public static final int POST_TASK_VERSION_DEFAULT = 1;
    public static final String CONDITION_TYPE = "conditionType";
    public static final int CONDITION_TYPE_DEFAULT = 0;
    public static final String CONDITION_PARAMS = "conditionParams";
    public static final String TASK_DEFINITION_JSON_CODE = "code";
    public static final String TASK_DEFINITION_JSON_NAME = "name";
    public static final String VERSION = "version";
    public static final int VERSION_DEFAULT = 1;
    public static final String DESCRIPTION = "description";
    public static final String DESCRIPTION_DEFAULT = "";
    public static final String DELAY_TIME = "delayTime";
    public static final int DELAY_TIME_DEFAULT = 0;
    public static final String TASK_TYPE = "taskType";
    public static final String TASK_TYPE_DEFAULT = "SHELL";
    public static final String TASK_PARAMS = "taskParams";
    public static final String RESOURCE_LIST = "resourceList";
    public static final String LOCAL_PARAMS = "localParams";
    public static final String LOCAL_PARAMS_PROP = "prop";
    public static final String LOCAL_PARAMS_DIRECT = "direct";
    public static final String LOCAL_PARAMS_DIRECT_DEFAULT = "IN";
    public static final String LOCAL_PARAMS_TYPE = "type";
    public static final String LOCAL_PARAMS_TYPE_DEFAULT = "VARCHAR";
    public static final String LOCAL_PARAMS_VALUE = "value";
    public static final String RAW_SCRIPT = "rawScript";
    public static final String DEPENDENCE = "dependence";
    public static final Map<String, Object> DEPENDENCE_DEFAULT = Collections.emptyMap();
    public static final String CONDITION_RESULT = "conditionResult";
    public static final String SUCCESS_NODE = "successNode";
    public static final List<Object> SUCCESS_NODE_DEFAULT = Collections.emptyList();
    public static final String FAILED_NODE = "failedNode";
    public static final List<Object> FAILED_NODE_DEFAULT = Collections.emptyList();
    public static final String WAIT_START_TIMEOUT = "waitStartTimeout";
    public static final int WAIT_START_TIMEOUT_DEFAULT = 0;
    public static final String SWITCH_RESULT = "switchResult";
    public static final int SWITCH_RESULT_DEFAULT = 0;
    public static final String FLAG = "flag";
    public static final String FLAG_DEFAULT = "YES";
    public static final String TASK_PRIORITY = "taskPriority";
    public static final String TASK_PRIORITY_DEFAULT = "MEDIUM";
    public static final String FAIL_RETRY_TIMES = "failRetryTimes";
    public static final String FAIL_RETRY_INTERVAL = "failRetryInterval";
    public static final String TIMEOUT_FLAG = "timeoutFlag";
    public static final String TIMEOUT_FLAG_DEFAULT = "CLOSE";
    public static final String TIMEOUT_NOTIFY_STRATEGY = "timeoutNotifyStrategy";
    public static final String TIMEOUT_NOTIFY_STRATEGY_DEFAULT = "WARN";
    public static final String TIMEOUT = "timeout";
    public static final int TIMEOUT_DEFAULT = 0;
    public static final String ENVIRONMENT_CODE = "environmentCode";
    public static final int ENVIRONMENT_CODE_DEFAULT = -1;
    public static final String PROCESS_DEFINITION_CODE = "processDefinitionCode";
    public static final String START_TIME = "startTime";
    public static final String END_TIME = "endTime";
    public static final String CRONTAB = "crontab";
    public static final String TIMEZONE_ID = "timezoneId";
    public static final String TIMEZONE_ID_DEFAULT = "Asia/Shanghai";
    public static final String SCHEDULE = "schedule";
    public static final String SCHEDULE_ID = "id";
    public static final String DEFAULT_FILE_SUFFIX = ".conf";
    public static final String EXEC_TYPE_DEFAULT = "START_PROCESS";
    public static final String EXEC_TYPE_COMPLEMENT = "COMPLEMENT_DATA";
    public static final String DEPENDENT_MODE_DEFAULT = "OFF_MODE";
    public static final String PROCESS_INSTANCE_ID = "processInstanceId";
    public static final String EXECUTE_TYPE = "executeType";
    public static final int LOG_SKIP_LINE_NUM_DEFAULT = 0;
    public static final int LOG_LIMIT_NUM_DEFAULT = Integer.MAX_VALUE;

    /**
     * response param
     */
    public static final String DATA = "data";
    public static final String DATA_TOTAL_LIST = "totalList";
    public static final String DATA_TOTAL = "total";
    public static final String CODE = "code";
    public static final int CODE_SUCCESS = 0;
    public static final String MSG = "msg";
    public static final String LOG_MESSAGE = "message";
}
