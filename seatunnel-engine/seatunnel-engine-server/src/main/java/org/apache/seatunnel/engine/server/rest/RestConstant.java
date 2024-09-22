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

    public static final String JOB_STATUS = "jobStatus";

    public static final String CREATE_TIME = "createTime";

    public static final String FINISH_TIME = "finishTime";

    public static final String ENV_OPTIONS = "envOptions";

    public static final String JOB_DAG = "jobDag";

    public static final String PLUGIN_JARS_URLS = "pluginJarsUrls";

    public static final String JAR_PATH = "jarPath";

    public static final String ERROR_MSG = "errorMsg";

    public static final String METRICS = "metrics";

    public static final String TABLE_SOURCE_RECEIVED_COUNT = "TableSourceReceivedCount";
    public static final String TABLE_SINK_WRITE_COUNT = "TableSinkWriteCount";
    public static final String TABLE_SOURCE_RECEIVED_QPS = "TableSourceReceivedQPS";
    public static final String TABLE_SINK_WRITE_QPS = "TableSinkWriteQPS";
    public static final String TABLE_SOURCE_RECEIVED_BYTES = "TableSourceReceivedBytes";
    public static final String TABLE_SINK_WRITE_BYTES = "TableSinkWriteBytes";
    public static final String TABLE_SOURCE_RECEIVED_BYTES_PER_SECONDS =
            "TableSourceReceivedBytesPerSeconds";
    public static final String TABLE_SINK_WRITE_BYTES_PER_SECONDS = "TableSinkWriteBytesPerSeconds";

    public static final String OVERVIEW = "/hazelcast/rest/maps/overview";
    public static final String RUNNING_JOBS_URL = "/hazelcast/rest/maps/running-jobs";
    @Deprecated public static final String RUNNING_JOB_URL = "/hazelcast/rest/maps/running-job";
    public static final String JOB_INFO_URL = "/hazelcast/rest/maps/job-info";
    public static final String FINISHED_JOBS_INFO = "/hazelcast/rest/maps/finished-jobs";
    public static final String ENCRYPT_CONFIG = "/hazelcast/rest/maps/encrypt-config";
    public static final String THREAD_DUMP = "/hazelcast/rest/maps/thread-dump";

    // only for test use
    public static final String RUNNING_THREADS = "/hazelcast/rest/maps/running-threads";

    public static final String SYSTEM_MONITORING_INFORMATION =
            "/hazelcast/rest/maps/system-monitoring-information";

    public static final String SUBMIT_JOB_URL = "/hazelcast/rest/maps/submit-job";
    public static final String SUBMIT_JOBS_URL = "/hazelcast/rest/maps/submit-jobs";
    public static final String STOP_JOB_URL = "/hazelcast/rest/maps/stop-job";
    public static final String STOP_JOBS_URL = "/hazelcast/rest/maps/stop-jobs";
    public static final String UPDATE_TAGS_URL = "/hazelcast/rest/maps/update-tags";

    // metrics
    public static final String TELEMETRY_METRICS_URL = "/hazelcast/rest/instance/metrics";
    public static final String TELEMETRY_OPEN_METRICS_URL = "/hazelcast/rest/instance/openmetrics";
}
