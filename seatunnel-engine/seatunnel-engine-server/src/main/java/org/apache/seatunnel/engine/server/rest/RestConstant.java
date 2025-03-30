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

package org.apache.seatunnel.engine.server.rest;

public class RestConstant {

    public static final String JOB_ID = "jobId";

    public static final String JOB_NAME = "jobName";

    public static final String IS_START_WITH_SAVE_POINT = "isStartWithSavePoint";

    public static final String IS_STOP_WITH_SAVE_POINT = "isStopWithSavePoint";

    public static final String CONFIG_FORMAT = "format";

    public static final String JOB_STATUS = "jobStatus";

    public static final String CREATE_TIME = "createTime";

    public static final String FINISH_TIME = "finishTime";

    public static final String ENV_OPTIONS = "envOptions";

    public static final String JOB_DAG = "jobDag";

    public static final String PLUGIN_JARS_URLS = "pluginJarsUrls";

    public static final String JAR_PATH = "jarPath";

    public static final String ERROR_MSG = "errorMsg";

    public static final String METRICS = "metrics";

    public static final String HOCON = "hocon";

    public static final String TABLE_SOURCE_RECEIVED_COUNT = "TableSourceReceivedCount";
    public static final String TABLE_SINK_WRITE_COUNT = "TableSinkWriteCount";
    public static final String TABLE_SOURCE_RECEIVED_QPS = "TableSourceReceivedQPS";
    public static final String TABLE_SINK_WRITE_QPS = "TableSinkWriteQPS";
    public static final String TABLE_SOURCE_RECEIVED_BYTES = "TableSourceReceivedBytes";
    public static final String TABLE_SINK_WRITE_BYTES = "TableSinkWriteBytes";
    public static final String TABLE_SOURCE_RECEIVED_BYTES_PER_SECONDS =
            "TableSourceReceivedBytesPerSeconds";
    public static final String TABLE_SINK_WRITE_BYTES_PER_SECONDS = "TableSinkWriteBytesPerSeconds";

    public static final String CONTEXT_PATH = "/hazelcast/rest/maps";
    public static final String INSTANCE_CONTEXT_PATH = "/hazelcast/rest/instance";

    // api path start
    public static final String REST_URL_OVERVIEW = "/overview";
    public static final String REST_URL_RUNNING_JOBS = "/running-jobs";
    @Deprecated public static final String REST_URL_RUNNING_JOB = "/running-job";
    public static final String REST_URL_JOB_INFO = "/job-info";
    public static final String REST_URL_FINISHED_JOBS = "/finished-jobs";
    public static final String REST_URL_ENCRYPT_CONFIG = "/encrypt-config";
    public static final String REST_URL_THREAD_DUMP = "/thread-dump";
    // only for test use
    public static final String REST_URL_RUNNING_THREADS = "/running-threads";
    public static final String REST_URL_SYSTEM_MONITORING_INFORMATION =
            "/system-monitoring-information";
    public static final String REST_URL_SUBMIT_JOB = "/submit-job";

    public static final String REST_URL_SUBMIT_JOB_BY_UPLOAD_FILE = "/submit-job/upload";

    public static final String REST_URL_SUBMIT_JOBS = "/submit-jobs";
    public static final String REST_URL_STOP_JOB = "/stop-job";
    public static final String REST_URL_STOP_JOBS = "/stop-jobs";
    public static final String REST_URL_UPDATE_TAGS = "/update-tags";
    // Get All Nodes Log
    public static final String REST_URL_LOGS = "/logs";
    // Get Current Node Log
    public static final String REST_URL_LOG = "/log";
    // Code internal Use , Get Node Log Name
    public static final String REST_URL_GET_ALL_LOG_NAME = "/get-all-log-name";
    public static final String REST_URL_METRICS = "/metrics";
    public static final String REST_URL_OPEN_METRICS = "/openmetrics";
    // api path end

}
