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
import org.apache.seatunnel.common.utils.JsonUtils;
import org.apache.seatunnel.engine.common.Constant;
import org.apache.seatunnel.engine.core.job.JobDAGInfo;
import org.apache.seatunnel.engine.core.job.JobInfo;
import org.apache.seatunnel.engine.server.SeaTunnelServer;
import org.apache.seatunnel.engine.server.master.JobHistoryService.JobState;
import org.apache.seatunnel.engine.server.operation.GetJobMetricsOperation;
import org.apache.seatunnel.engine.server.rest.RestConstant;
import org.apache.seatunnel.engine.server.utils.NodeEngineUtil;
import org.apache.seatunnel.engine.server.utils.RestUtil;

import com.hazelcast.internal.json.JsonArray;
import com.hazelcast.internal.json.JsonObject;
import com.hazelcast.map.IMap;
import com.hazelcast.spi.impl.NodeEngineImpl;
import scala.Tuple2;

import java.nio.charset.StandardCharsets;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.seatunnel.engine.server.rest.RestConstant.CONFIG_FORMAT;
import static org.apache.seatunnel.engine.server.rest.RestConstant.HOCON;

public class JobInfoService extends BaseService {

    public JobInfoService(NodeEngineImpl nodeEngine) {
        super(nodeEngine);
    }

    public JsonObject getJobInfoJson(Long jobId) {
        IMap<Object, Object> jobInfoMap =
                nodeEngine.getHazelcastInstance().getMap(Constant.IMAP_RUNNING_JOB_INFO);
        JobInfo jobInfo = (JobInfo) jobInfoMap.get(jobId);

        IMap<Object, Object> finishedJobStateMap =
                nodeEngine.getHazelcastInstance().getMap(Constant.IMAP_FINISHED_JOB_STATE);
        JobState finishedJobState = (JobState) finishedJobStateMap.get(jobId);

        if (jobInfo != null) {
            return convertToJson(jobInfo, jobId);
        } else if (finishedJobState != null) {
            JobMetrics finishedJobMetrics =
                    (JobMetrics)
                            nodeEngine
                                    .getHazelcastInstance()
                                    .getMap(Constant.IMAP_FINISHED_JOB_METRICS)
                                    .get(jobId);
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
                .sorted(Comparator.comparing(JobState::getFinishTime))
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
        IMap<Long, JobInfo> values =
                nodeEngine.getHazelcastInstance().getMap(Constant.IMAP_RUNNING_JOB_INFO);
        return values.entrySet().stream()
                .map(jobInfoEntry -> convertToJson(jobInfoEntry.getValue(), jobInfoEntry.getKey()))
                .collect(JsonArray::new, JsonArray::add, JsonArray::add);
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

        if (Boolean.parseBoolean(requestParams.get(RestConstant.IS_START_WITH_SAVE_POINT))
                && requestParams.get(RestConstant.JOB_ID) == null) {
            throw new IllegalArgumentException("Please provide jobId when start with save point.");
        }
        Config config;
        if (HOCON.equalsIgnoreCase(requestParams.get(CONFIG_FORMAT))) {
            String requestBodyStr = new String(requestBody, StandardCharsets.UTF_8);
            config = ConfigFactory.parseString(requestBodyStr);
        } else {
            config = RestUtil.buildConfig(requestHandle(requestBody), false);
        }
        SeaTunnelServer seaTunnelServer = getSeaTunnelServer(false);
        return submitJobInternal(config, requestParams, seaTunnelServer, nodeEngine.getNode());
    }

    public JsonObject submitJob(Map<String, String> requestParams, Config config) {
        if (Boolean.parseBoolean(requestParams.get(RestConstant.IS_START_WITH_SAVE_POINT))
                && requestParams.get(RestConstant.JOB_ID) == null) {
            throw new IllegalArgumentException("Please provide jobId when start with save point.");
        }
        SeaTunnelServer seaTunnelServer = getSeaTunnelServer(false);
        return submitJobInternal(config, requestParams, seaTunnelServer, nodeEngine.getNode());
    }

    public JsonArray submitJobs(byte[] requestBody) {
        List<Tuple2<Map<String, String>, Config>> configTuples =
                RestUtil.buildConfigList(requestHandle(requestBody), false);

        return configTuples.stream()
                .map(
                        tuple -> {
                            String urlParams = mapToUrlParams(tuple._1);
                            Map<String, String> requestParams = new HashMap<>();
                            RestUtil.buildRequestParams(requestParams, urlParams);
                            SeaTunnelServer seaTunnelServer = getSeaTunnelServer(false);
                            return submitJobInternal(
                                    tuple._2, requestParams, seaTunnelServer, nodeEngine.getNode());
                        })
                .collect(JsonArray::new, JsonArray::add, JsonArray::add);
    }
}
