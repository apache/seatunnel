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

package org.apache.seatunnel.engine.server.master;

import org.apache.seatunnel.shade.com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.SerializationFeature;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.node.ObjectNode;

import org.apache.seatunnel.api.common.metrics.JobMetrics;
import org.apache.seatunnel.engine.common.exception.SeaTunnelEngineException;
import org.apache.seatunnel.engine.core.job.JobDAGInfo;
import org.apache.seatunnel.engine.core.job.JobImmutableInformation;
import org.apache.seatunnel.engine.core.job.JobStatus;
import org.apache.seatunnel.engine.core.job.JobStatusData;
import org.apache.seatunnel.engine.core.job.PipelineStatus;
import org.apache.seatunnel.engine.server.dag.physical.PipelineLocation;
import org.apache.seatunnel.engine.server.execution.ExecutionState;
import org.apache.seatunnel.engine.server.execution.PendingSourceState;
import org.apache.seatunnel.engine.server.execution.TaskGroupLocation;
import org.apache.seatunnel.engine.server.operation.CleanLogOperation;
import org.apache.seatunnel.engine.server.utils.NodeEngineUtil;

import com.hazelcast.cluster.Address;
import com.hazelcast.core.EntryEvent;
import com.hazelcast.logging.ILogger;
import com.hazelcast.map.IMap;
import com.hazelcast.map.listener.EntryExpiredListener;
import com.hazelcast.spi.impl.NodeEngine;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.Getter;
import scala.Tuple2;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class JobHistoryService {

    private final NodeEngine nodeEngine;

    /**
     * IMap key is one of jobId {@link
     * org.apache.seatunnel.engine.server.dag.physical.PipelineLocation} and {@link
     * org.apache.seatunnel.engine.server.execution.TaskGroupLocation}
     *
     * <p>The value of IMap is one of {@link JobStatus} {@link PipelineStatus} {@link
     * org.apache.seatunnel.engine.server.execution.ExecutionState}
     *
     * <p>This IMap is used to recovery runningJobStateIMap in JobMaster when a new master node
     * active
     */
    private final IMap<Object, Object> runningJobStateIMap;

    private final ILogger logger;

    /**
     * key: job id; <br>
     * value: job master;
     */
    private final Map<Long, JobMaster> runningJobMasterMap;

    private final Map<Long, JobMaster> finishedJobMasterMap;

    private final Map<Long, Tuple2<PendingSourceState, JobMaster>> pendingJobMasterMap;

    /** finishedJobVertexInfoImap key is jobId and value is JobDAGInfo */
    private final IMap<Long, JobDAGInfo> finishedJobDAGInfoImap;

    /**
     * finishedJobStateImap key is jobId and value is jobState(json) JobStateData Indicates the
     * status of the job, pipeline, and task
     */
    @Getter private final IMap<Long, JobState> finishedJobStateImap;

    private final IMap<Long, JobMetrics> finishedJobMetricsImap;

    private final ObjectMapper objectMapper;

    private final int finishedJobExpireTime;

    public JobHistoryService(
            NodeEngine nodeEngine,
            IMap<Object, Object> runningJobStateIMap,
            ILogger logger,
            Map<Long, Tuple2<PendingSourceState, JobMaster>> pendingJobMasterMap,
            Map<Long, JobMaster> runningJobMasterMap,
            Map<Long, JobMaster> finishedJobMasterMap,
            IMap<Long, JobState> finishedJobStateImap,
            IMap<Long, JobMetrics> finishedJobMetricsImap,
            IMap<Long, JobDAGInfo> finishedJobVertexInfoImap,
            int finishedJobExpireTime) {
        this.nodeEngine = nodeEngine;
        this.runningJobStateIMap = runningJobStateIMap;
        this.logger = logger;
        this.pendingJobMasterMap = pendingJobMasterMap;
        this.runningJobMasterMap = runningJobMasterMap;
        this.finishedJobStateImap = finishedJobStateImap;
        this.finishedJobMasterMap = finishedJobMasterMap;
        this.finishedJobMetricsImap = finishedJobMetricsImap;
        this.finishedJobDAGInfoImap = finishedJobVertexInfoImap;
        this.finishedJobDAGInfoImap.addEntryListener(new JobInfoExpiredListener(), true);
        this.objectMapper = new ObjectMapper();
        this.objectMapper.configure(SerializationFeature.FAIL_ON_EMPTY_BEANS, false);
        this.finishedJobExpireTime = finishedJobExpireTime;
    }

    // Gets the status of a running and completed job.
    public String listAllJob() {
        List<JobStatusData> status = getJobStatusData();
        try {
            return objectMapper.writeValueAsString(status);
        } catch (JsonProcessingException e) {
            logger.severe("Failed to list all job", e);
            throw new SeaTunnelEngineException(e);
        }
    }

    public List<JobStatusData> getJobStatusData() {
        List<JobStatusData> status = new ArrayList<>();
        final List<JobState> runningJobStateList =
                runningJobMasterMap.values().stream()
                        .map(master -> toJobStateMapper(master, true))
                        .collect(Collectors.toList());
        Set<Long> runningJonIds =
                runningJobStateList.stream().map(JobState::getJobId).collect(Collectors.toSet());
        Stream.concat(
                        runningJobStateList.stream(),
                        finishedJobStateImap.values().stream()
                                .filter(jobState -> !runningJonIds.contains(jobState.getJobId())))
                .forEach(
                        jobState -> {
                            JobStatusData jobStatusData =
                                    new JobStatusData(
                                            jobState.getJobId(),
                                            jobState.getJobName(),
                                            jobState.getJobStatus(),
                                            jobState.getSubmitTime(),
                                            jobState.getFinishTime());
                            status.add(jobStatusData);
                        });
        return status;
    }

    // Get detailed status of a single job
    public JobState getJobDetailState(Long jobId) {
        if (pendingJobMasterMap.containsKey(jobId)) {
            // return pending job state
            JobImmutableInformation jobImmutableInformation =
                    pendingJobMasterMap.get(jobId)._2.getJobImmutableInformation();
            return new JobState(
                    jobId,
                    jobImmutableInformation.getJobName(),
                    JobStatus.PENDING,
                    jobImmutableInformation.getCreateTime(),
                    null,
                    null,
                    null);
        }
        return runningJobMasterMap.containsKey(jobId)
                ? toJobStateMapper(runningJobMasterMap.get(jobId), false)
                : finishedJobStateImap.getOrDefault(jobId, null);
    }

    public JobMetrics getJobMetrics(Long jobId) {
        return finishedJobMetricsImap.getOrDefault(jobId, null);
    }

    public JobDAGInfo getJobDAGInfo(Long jobId) {
        return finishedJobDAGInfoImap.getOrDefault(jobId, null);
    }

    // Get detailed status of a single job as json
    public String getJobDetailStateAsString(Long jobId) {
        JobState jobStatus = getJobDetailState(jobId);
        if (null != jobStatus) {
            try {
                return objectMapper.writeValueAsString(jobStatus);
            } catch (JsonProcessingException e) {
                logger.severe("serialize jobStateMapper err", e);
                ObjectNode objectNode = objectMapper.createObjectNode();
                objectNode.put("err", "serialize jobStateMapper err");
                return objectNode.toString();
            }
        }
        ObjectNode objectNode = objectMapper.createObjectNode();
        objectNode.put("err", String.format("jobId : %s not found", jobId));
        return objectNode.toString();
    }

    public void storeFinishedJobState(JobMaster jobMaster) {
        JobState jobState = toJobStateMapper(jobMaster, false);
        jobState.setFinishTime(System.currentTimeMillis());
        jobState.setErrorMessage(jobMaster.getErrorMessage());
        finishedJobStateImap.put(jobState.jobId, jobState, finishedJobExpireTime, TimeUnit.MINUTES);
    }

    public void storeFinishedPipelineMetrics(long jobId, JobMetrics metrics) {
        finishedJobMetricsImap.computeIfAbsent(jobId, key -> JobMetrics.of(new HashMap<>()));
        JobMetrics newMetrics = finishedJobMetricsImap.get(jobId).merge(metrics);
        finishedJobMetricsImap.put(jobId, newMetrics, finishedJobExpireTime, TimeUnit.MINUTES);
    }

    private JobState toJobStateMapper(JobMaster jobMaster, boolean simple) {

        Long jobId = jobMaster.getJobImmutableInformation().getJobId();
        Map<PipelineLocation, PipelineStateData> pipelineStateMapperMap = new HashMap<>();
        if (!simple) {
            try {
                jobMaster
                        .getPhysicalPlan()
                        .getPipelineList()
                        .forEach(
                                pipeline -> {
                                    PipelineLocation pipelineLocation =
                                            pipeline.getPipelineLocation();
                                    PipelineStatus pipelineState =
                                            (PipelineStatus)
                                                    runningJobStateIMap.get(pipelineLocation);
                                    Map<TaskGroupLocation, ExecutionState> taskStateMap =
                                            new HashMap<>();
                                    pipeline.getCoordinatorVertexList()
                                            .forEach(
                                                    coordinator -> {
                                                        TaskGroupLocation taskGroupLocation =
                                                                coordinator.getTaskGroupLocation();
                                                        taskStateMap.put(
                                                                taskGroupLocation,
                                                                (ExecutionState)
                                                                        runningJobStateIMap.get(
                                                                                taskGroupLocation));
                                                    });
                                    pipeline.getPhysicalVertexList()
                                            .forEach(
                                                    task -> {
                                                        TaskGroupLocation taskGroupLocation =
                                                                task.getTaskGroupLocation();
                                                        taskStateMap.put(
                                                                taskGroupLocation,
                                                                (ExecutionState)
                                                                        runningJobStateIMap.get(
                                                                                taskGroupLocation));
                                                    });

                                    PipelineStateData pipelineStateData =
                                            new PipelineStateData(pipelineState, taskStateMap);
                                    pipelineStateMapperMap.put(pipelineLocation, pipelineStateData);
                                });
            } catch (Exception e) {
                logger.warning("get job pipeline state err", e);
            }
        }
        JobStatus jobStatus = (JobStatus) runningJobStateIMap.get(jobId);
        String jobName = jobMaster.getJobImmutableInformation().getJobName();
        long submitTime = jobMaster.getJobImmutableInformation().getCreateTime();
        return new JobState(
                jobId, jobName, jobStatus, submitTime, null, pipelineStateMapperMap, null);
    }

    public void storeJobInfo(long jobId, JobDAGInfo jobInfo) {
        finishedJobDAGInfoImap.put(jobId, jobInfo, finishedJobExpireTime, TimeUnit.MINUTES);
    }

    @AllArgsConstructor
    @Data
    public static final class JobState implements Serializable {
        private static final long serialVersionUID = -1176348098833918960L;
        private Long jobId;
        private String jobName;
        private JobStatus jobStatus;
        private long submitTime;
        private Long finishTime;
        private Map<PipelineLocation, PipelineStateData> pipelineStateMapperMap;
        private String errorMessage;
    }

    @AllArgsConstructor
    @Data
    public static final class PipelineStateData implements Serializable {
        private static final long serialVersionUID = -7875004875757861958L;
        private PipelineStatus pipelineStatus;
        private Map<TaskGroupLocation, ExecutionState> executionStateMap;
    }

    private class JobInfoExpiredListener implements EntryExpiredListener<Long, JobDAGInfo> {
        @Override
        public void entryExpired(EntryEvent<Long, JobDAGInfo> event) {
            Long jobId = event.getKey();
            try {
                JobMaster jobMaster = finishedJobMasterMap.get(jobId);
                Set<Address> historyExecutionPlan = jobMaster.getHistoryExecutionPlan();

                historyExecutionPlan.forEach(
                        address -> {
                            NodeEngineUtil.sendOperationToMemberNode(
                                    nodeEngine, new CleanLogOperation(jobId), address);
                        });
            } finally {
                finishedJobMasterMap.remove(jobId);
            }
        }
    }
}
