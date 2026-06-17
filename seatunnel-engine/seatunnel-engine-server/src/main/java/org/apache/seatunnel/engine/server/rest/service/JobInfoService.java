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

package org.apache.seatunnel.engine.server.rest.service;

import org.apache.seatunnel.shade.com.typesafe.config.Config;
import org.apache.seatunnel.shade.com.typesafe.config.ConfigFactory;

import org.apache.seatunnel.api.common.metrics.JobMetrics;
import org.apache.seatunnel.common.utils.DateTimeUtils;
import org.apache.seatunnel.common.utils.JsonUtils;
import org.apache.seatunnel.config.sql.SqlConfigBuilder;
import org.apache.seatunnel.core.starter.utils.ConfigShadeUtils;
import org.apache.seatunnel.engine.common.Constant;
import org.apache.seatunnel.engine.common.job.JobStatus;
import org.apache.seatunnel.engine.core.job.JobDAGInfo;
import org.apache.seatunnel.engine.core.job.JobImmutableInformation;
import org.apache.seatunnel.engine.core.job.JobInfo;
import org.apache.seatunnel.engine.server.SeaTunnelServer;
import org.apache.seatunnel.engine.server.master.JobHistoryService.JobState;
import org.apache.seatunnel.engine.server.operation.GetJobMetricsOperation;
import org.apache.seatunnel.engine.server.operation.GetJobStatusOperation;
import org.apache.seatunnel.engine.server.rest.ConfigFormat;
import org.apache.seatunnel.engine.server.rest.RestConstant;
import org.apache.seatunnel.engine.server.utils.NodeEngineUtil;
import org.apache.seatunnel.engine.server.utils.RestUtil;

import com.hazelcast.internal.json.JsonArray;
import com.hazelcast.internal.json.JsonObject;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.map.IMap;
import com.hazelcast.spi.impl.NodeEngineImpl;
import lombok.extern.slf4j.Slf4j;
import scala.Tuple2;

import java.nio.charset.StandardCharsets;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;

import static org.apache.seatunnel.engine.server.rest.RestConstant.CONFIG_FORMAT;

@Slf4j
public class JobInfoService extends BaseService {

    private volatile IMap<Long, JobInfo> runningJobInfoMap;
    private volatile IMap<Object, Object> runningJobStateMap;
    private final ConcurrentMap<Long, JobBasicInfo> runningJobBasicInfoCache =
            new ConcurrentHashMap<>();

    public JobInfoService(NodeEngineImpl nodeEngine) {
        super(nodeEngine);
    }

    public JsonObject getJobInfoJson(Long jobId) {
        IMap<Object, Object> jobInfoMap =
                nodeEngine.getHazelcastInstance().getMap(Constant.IMAP_RUNNING_JOB_INFO);
        JobInfo jobInfo = (JobInfo) jobInfoMap.get(jobId);

        if (jobInfo != null) {
            return convertToJson(jobInfo, jobId);
        }

        JobState finishedJobState =
                (JobState)
                        nodeEngine
                                .getHazelcastInstance()
                                .getMap(Constant.IMAP_FINISHED_JOB_STATE)
                                .get(jobId);

        if (finishedJobState != null) {
            JobMetrics finishedJobMetrics =
                    (JobMetrics)
                            nodeEngine
                                    .getHazelcastInstance()
                                    .getMap(Constant.IMAP_FINISHED_JOB_METRICS)
                                    .get(jobId);
            if (finishedJobMetrics == null) {
                finishedJobMetrics = JobMetrics.empty();
            }

            JobDAGInfo finishedJobDAGInfo =
                    (JobDAGInfo)
                            nodeEngine
                                    .getHazelcastInstance()
                                    .getMap(Constant.IMAP_FINISHED_JOB_VERTEX_INFO)
                                    .get(jobId);

            return getJobInfoJson(
                    finishedJobState, finishedJobMetrics.toJsonString(), finishedJobDAGInfo);
        } else {
            return new JsonObject().add(RestConstant.JOB_ID, jobId.toString());
        }
    }

    public JsonArray getJobsByStateJson(String state) {
        IMap<Long, JobState> finishedJob =
                nodeEngine.getHazelcastInstance().getMap(Constant.IMAP_FINISHED_JOB_STATE);

        IMap<Long, JobDAGInfo> finishedJobDAGInfo =
                nodeEngine.getHazelcastInstance().getMap(Constant.IMAP_FINISHED_JOB_VERTEX_INFO);

        SeaTunnelServer seaTunnelServer = getSeaTunnelServer(true);

        return finishedJob.values().stream()
                .filter(
                        jobState -> {
                            if (state.isEmpty()) {
                                return true;
                            }
                            return jobState.getJobStatus().name().equals(state.toUpperCase());
                        })
                .sorted(Comparator.comparing(JobState::getFinishTime, Comparator.reverseOrder()))
                .map(
                        jobState -> {
                            Long jobId = jobState.getJobId();
                            String jobMetrics;
                            if (seaTunnelServer == null) {
                                jobMetrics =
                                        (String)
                                                NodeEngineUtil.sendOperationToMasterNode(
                                                                nodeEngine,
                                                                new GetJobMetricsOperation(jobId))
                                                        .join();
                            } else {
                                jobMetrics =
                                        seaTunnelServer
                                                .getCoordinatorService()
                                                .getJobMetrics(jobId)
                                                .toJsonString();
                            }
                            return getJobInfoJson(
                                    jobState, jobMetrics, finishedJobDAGInfo.get(jobId));
                        })
                .collect(JsonArray::new, JsonArray::add, JsonArray::add);
    }

    public JsonArray getRunningJobsJson() {
        IMap<Long, JobInfo> values = getRunningJobInfoMap();
        SeaTunnelServer seaTunnelServer = getSeaTunnelServer(true);
        return values.entrySet().stream()
                .filter(entry -> shouldShowAsRunningJob(seaTunnelServer, entry.getKey()))
                .sorted(
                        Comparator.comparing(
                                entry -> entry.getValue().getInitializationTimestamp(),
                                Comparator.reverseOrder()))
                .map(jobInfoEntry -> convertToJson(jobInfoEntry.getValue(), jobInfoEntry.getKey()))
                .collect(JsonArray::new, JsonArray::add, JsonArray::add);
    }

    /**
     * A lightweight version of {@link #getRunningJobsJson()}, used by UI list page.
     *
     * <p>It avoids rebuilding DAG and fetching full metrics to prevent occasional long latency.
     */
    public JsonArray getRunningJobsSummaryJson() {
        long startNs = System.nanoTime();
        IMap<Long, JobInfo> values = getRunningJobInfoMap();
        IMap<Object, Object> jobStateMap = getRunningJobStateMap();
        SeaTunnelServer seaTunnelServer = getSeaTunnelServer(true);

        long decodeTotalNs = 0L;
        JsonArray out = new JsonArray();

        Set<Long> jobIds = new HashSet<>();
        for (Map.Entry<Long, JobInfo> entry : values.entrySet()) {
            long jobId = entry.getKey();
            JobInfo jobInfo = entry.getValue();
            if (!shouldShowAsRunningJob(seaTunnelServer, jobId)) {
                continue;
            }
            jobIds.add(jobId);

            long decodeStartNs = System.nanoTime();
            JobBasicInfo basicInfo =
                    runningJobBasicInfoCache.computeIfAbsent(
                            jobId, k -> decodeJobBasicInfo(jobInfo));
            decodeTotalNs += (System.nanoTime() - decodeStartNs);

            String jobName = basicInfo == null ? String.valueOf(jobId) : basicInfo.jobName;
            String createTime =
                    basicInfo == null
                            ? null
                            : DateTimeUtils.toString(
                                    basicInfo.createTime,
                                    DateTimeUtils.Formatter.YYYY_MM_DD_HH_MM_SS);

            String jobStatus = JobStatus.UNKNOWABLE.toString();
            try {
                Object state = jobStateMap == null ? null : jobStateMap.get(jobId);
                if (state instanceof JobStatus) {
                    jobStatus = ((JobStatus) state).toString();
                } else if (state != null) {
                    jobStatus = String.valueOf(state);
                }
            } catch (Throwable ignored) {
                // ignore
            }

            out.add(
                    new JsonObject()
                            .add(RestConstant.JOB_ID, String.valueOf(jobId))
                            .add(RestConstant.JOB_NAME, jobName)
                            .add(RestConstant.JOB_STATUS, jobStatus)
                            .add(RestConstant.CREATE_TIME, createTime));
        }

        if (!runningJobBasicInfoCache.isEmpty() && !jobIds.isEmpty()) {
            runningJobBasicInfoCache.keySet().removeIf(id -> !jobIds.contains(id));
        }

        long totalMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startNs);
        if (totalMs > 500) {
            log.warn(
                    "running-jobs summary slow: total={}ms jobs={} decode={}ms",
                    totalMs,
                    values.size(),
                    TimeUnit.NANOSECONDS.toMillis(decodeTotalNs));
            Runtime rt = Runtime.getRuntime();
            long usedBytes = rt.totalMemory() - rt.freeMemory();
            log.warn(
                    "running-jobs summary slow diagnostics: totalMs={} jobs={} decodeMs={} "
                            + "heapUsedMB={} heapTotalMB={} heapMaxMB={}",
                    totalMs,
                    values.size(),
                    TimeUnit.NANOSECONDS.toMillis(decodeTotalNs),
                    usedBytes / 1024 / 1024,
                    rt.totalMemory() / 1024 / 1024,
                    rt.maxMemory() / 1024 / 1024);
        } else {
            log.debug(
                    "running-jobs summary: total={}ms jobs={} decode={}ms",
                    totalMs,
                    values.size(),
                    TimeUnit.NANOSECONDS.toMillis(decodeTotalNs));
        }
        return out;
    }

    private IMap<Long, JobInfo> getRunningJobInfoMap() {
        IMap<Long, JobInfo> local = runningJobInfoMap;
        if (local == null) {
            local = nodeEngine.getHazelcastInstance().getMap(Constant.IMAP_RUNNING_JOB_INFO);
            runningJobInfoMap = local;
        }
        return local;
    }

    private IMap<Object, Object> getRunningJobStateMap() {
        IMap<Object, Object> local = runningJobStateMap;
        if (local == null) {
            local = nodeEngine.getHazelcastInstance().getMap(Constant.IMAP_RUNNING_JOB_STATE);
            runningJobStateMap = local;
        }
        return local;
    }

    private JobBasicInfo decodeJobBasicInfo(JobInfo jobInfo) {
        if (jobInfo == null || jobInfo.getJobImmutableInformation() == null) {
            return null;
        }

        // Fast path: only read jobName/createTime from the beginning of JobImmutableInformation.
        // Avoid deserializing the whole JobImmutableInformation which includes LogicalDag and
        // JobConfig (can be very large).
        try {
            InternalSerializationService serializationService =
                    (InternalSerializationService) nodeEngine.getSerializationService();
            Data data = jobInfo.getJobImmutableInformation();
            com.hazelcast.internal.nio.BufferObjectDataInput in =
                    serializationService.createObjectDataInput(data);

            // IdentifiedDataSerializable header: factoryId + classId.
            in.readInt();
            in.readInt();
            in.readLong(); // jobId
            String jobName = in.readString();
            in.readBoolean(); // isStartWithSavePoint
            long createTime = in.readLong();
            return new JobBasicInfo(jobName, createTime);
        } catch (Throwable t) {
            // Fallback to full deserialization for compatibility.
            try {
                Data data = jobInfo.getJobImmutableInformation();
                JobImmutableInformation obj = nodeEngine.getSerializationService().toObject(data);
                return obj == null ? null : new JobBasicInfo(obj.getJobName(), obj.getCreateTime());
            } catch (Throwable ignored) {
                return null;
            }
        }
    }

    private JobImmutableInformation decodeJobImmutableInformation(JobInfo jobInfo) {
        if (jobInfo == null || jobInfo.getJobImmutableInformation() == null) {
            return null;
        }
        try {
            Data data = jobInfo.getJobImmutableInformation();
            return nodeEngine.getSerializationService().toObject(data);
        } catch (Exception e) {
            log.debug("Decode JobImmutableInformation failed: {}", e.getMessage());
            return null;
        }
    }

    private static final class JobBasicInfo {
        private final String jobName;
        private final long createTime;

        private JobBasicInfo(String jobName, long createTime) {
            this.jobName = jobName;
            this.createTime = createTime;
        }
    }

    private boolean shouldShowAsRunningJob(SeaTunnelServer seaTunnelServer, long jobId) {
        if (seaTunnelServer != null) {
            return seaTunnelServer.getCoordinatorService().shouldShowAsRunningJob(jobId);
        }
        Integer statusOrdinal =
                (Integer)
                        NodeEngineUtil.sendOperationToMasterNode(
                                        nodeEngine, new GetJobStatusOperation(jobId))
                                .join();
        JobStatus status = JobStatus.values()[statusOrdinal];
        return !status.isEndState();
    }

    public JsonObject stopJob(byte[] requestBody) {
        Map<String, Object> map = JsonUtils.toMap(requestHandle(requestBody));
        SeaTunnelServer seaTunnelServer = getSeaTunnelServer(false);
        handleStopJob(map, seaTunnelServer, nodeEngine.getNode());
        return new JsonObject().add(RestConstant.JOB_ID, map.get(RestConstant.JOB_ID).toString());
    }

    public JsonArray stopJobs(byte[] requestBody) {
        JsonArray jsonResponse = new JsonArray();
        List<Map> jobList = JsonUtils.toList(requestHandle(requestBody).toString(), Map.class);

        jobList.forEach(
                job -> {
                    handleStopJob(job, getSeaTunnelServer(false), nodeEngine.getNode());
                    jsonResponse.add(
                            new JsonObject()
                                    .add(RestConstant.JOB_ID, (Long) job.get(RestConstant.JOB_ID)));
                });

        return jsonResponse;
    }

    public JsonObject submitJob(Map<String, String> requestParams, byte[] requestBody) {

        if (requestParams.containsKey(RestConstant.DRY_RUN)
                && requestParams.get(RestConstant.DRY_RUN) != null) {
            throw new IllegalArgumentException("Dry-run is only supported via CLI");
        }
        if (Boolean.parseBoolean(requestParams.get(RestConstant.IS_START_WITH_SAVE_POINT))
                && requestParams.get(RestConstant.JOB_ID) == null) {
            throw new IllegalArgumentException("Please provide jobId when start with save point.");
        }
        Config config;
        ConfigFormat configFormat = ConfigFormat.fromString(requestParams.get(CONFIG_FORMAT));

        switch (configFormat) {
            case HOCON:
                config = ConfigFactory.parseString(new String(requestBody, StandardCharsets.UTF_8));
                break;
            case SQL:
                config = SqlConfigBuilder.of(new String(requestBody, StandardCharsets.UTF_8));
                break;
            case JSON:
            default:
                config = RestUtil.buildConfig(requestHandle(requestBody));
                break;
        }

        config = ConfigShadeUtils.decryptConfig(config);

        SeaTunnelServer seaTunnelServer = getSeaTunnelServer(false);

        return submitJobInternal(config, requestParams, seaTunnelServer, nodeEngine.getNode());
    }

    public JsonObject submitJob(Map<String, String> requestParams, Config config) {
        if (requestParams.containsKey(RestConstant.DRY_RUN)
                && requestParams.get(RestConstant.DRY_RUN) != null) {
            throw new IllegalArgumentException("Dry-run is only supported via CLI");
        }
        if (Boolean.parseBoolean(requestParams.get(RestConstant.IS_START_WITH_SAVE_POINT))
                && requestParams.get(RestConstant.JOB_ID) == null) {
            throw new IllegalArgumentException("Please provide jobId when start with save point.");
        }
        SeaTunnelServer seaTunnelServer = getSeaTunnelServer(false);
        return submitJobInternal(config, requestParams, seaTunnelServer, nodeEngine.getNode());
    }

    public JsonArray submitJobs(byte[] requestBody) {
        List<Tuple2<Map<String, String>, Config>> configTuples =
                RestUtil.buildConfigList(requestHandle(requestBody));

        return configTuples.stream()
                .map(
                        tuple -> {
                            String urlParams = mapToUrlParams(tuple._1);
                            Map<String, String> requestParams = new HashMap<>();
                            RestUtil.buildRequestParams(requestParams, urlParams);
                            if (requestParams.containsKey(RestConstant.DRY_RUN)
                                    && requestParams.get(RestConstant.DRY_RUN) != null) {
                                throw new IllegalArgumentException(
                                        "Dry-run is only supported via CLI");
                            }
                            SeaTunnelServer seaTunnelServer = getSeaTunnelServer(false);
                            Config decryptConfig = ConfigShadeUtils.decryptConfig(tuple._2);
                            return submitJobInternal(
                                    decryptConfig,
                                    requestParams,
                                    seaTunnelServer,
                                    nodeEngine.getNode());
                        })
                .collect(JsonArray::new, JsonArray::add, JsonArray::add);
    }
}
