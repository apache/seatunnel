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

import org.apache.seatunnel.api.common.metrics.JobMetrics;
import org.apache.seatunnel.api.common.metrics.RawJobMetrics;
import org.apache.seatunnel.engine.core.job.JobStatus;
import org.apache.seatunnel.engine.core.job.PipelineStatus;
import org.apache.seatunnel.engine.server.dag.physical.PipelineLocation;
import org.apache.seatunnel.engine.server.execution.ExecutionState;
import org.apache.seatunnel.engine.server.execution.TaskGroupLocation;
import org.apache.seatunnel.engine.server.metrics.JobMetricsUtil;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.hazelcast.logging.ILogger;
import com.hazelcast.map.IMap;
import lombok.AllArgsConstructor;
import lombok.Data;

import java.io.Serializable;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

public class JobHistoryService {
    /**
     * IMap key is one of jobId {@link org.apache.seatunnel.engine.server.dag.physical.PipelineLocation} and
     * {@link org.apache.seatunnel.engine.server.execution.TaskGroupLocation}
     * <p>
     * The value of IMap is one of {@link JobStatus} {@link PipelineStatus}
     * {@link org.apache.seatunnel.engine.server.execution.ExecutionState}
     * <p>
     * This IMap is used to recovery runningJobStateIMap in JobMaster when a new master node active
     */
    private final IMap<Object, Object> runningJobStateIMap;

    private final ILogger logger;

    /**
     * key: job id;
     * <br> value: job master;
     */
    private final Map<Long, JobMaster> runningJobMasterMap;

    /**
     * finishedJobStateImap key is jobId and value is jobState(json)
     * JobStateData Indicates the status of the job, pipeline, and task
     */
    //TODO need to limit the amount of storage
    private final IMap<Long, JobStateData> finishedJobStateImap;

    private final IMap<Long, JobMetrics> finishedJobMetricsImap;

    private final ObjectMapper objectMapper;

    public JobHistoryService(
        IMap<Object, Object> runningJobStateIMap,
        ILogger logger,
        Map<Long, JobMaster> runningJobMasterMap,
        IMap<Long, JobStateData> finishedJobStateImap,
        IMap<Long, JobMetrics> finishedJobMetricsImap
    ) {
        this.runningJobStateIMap = runningJobStateIMap;
        this.logger = logger;
        this.runningJobMasterMap = runningJobMasterMap;
        this.finishedJobStateImap = finishedJobStateImap;
        this.finishedJobMetricsImap = finishedJobMetricsImap;
        this.objectMapper = new ObjectMapper();
        this.objectMapper.configure(SerializationFeature.FAIL_ON_EMPTY_BEANS, false);
    }

    // Gets the status of a running and completed job
    public String listAllJob() {
        ObjectNode objectNode = objectMapper.createObjectNode();
        ArrayNode jobs = objectNode.putArray("jobs");

        Stream.concat(runningJobMasterMap.values().stream().map(this::toJobStateMapper),
                finishedJobStateImap.values().stream())
            .forEach(jobStateData -> {
                JobStatusData jobStatusData = new JobStatusData(jobStateData.jobId, jobStateData.jobStatus);
                JsonNode jsonNode = objectMapper.valueToTree(jobStatusData);
                jobs.add(jsonNode);
            });
        return jobs.toString();
    }

    // Get detailed status of a single job
    public JobStateData getJobStatus(Long jobId) {
        return runningJobMasterMap.containsKey(jobId) ? toJobStateMapper(runningJobMasterMap.get(jobId)) :
            finishedJobStateImap.getOrDefault(jobId, null);
    }

    public JobMetrics getJobMetrics(Long jobId){
        return finishedJobMetricsImap.getOrDefault(jobId, null);
    }

    // Get detailed status of a single job as json
    public String getJobStatusAsString(Long jobId) {
        JobStateData jobStatus = getJobStatus(jobId);
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

    @SuppressWarnings("checkstyle:MagicNumber")
    public void storeFinishedJobState(JobMaster jobMaster) {
        JobStateData jobStateData = toJobStateMapper(jobMaster);
        finishedJobStateImap.put(jobStateData.jobId, jobStateData, 14, TimeUnit.DAYS);
    }

    @SuppressWarnings("checkstyle:MagicNumber")
    public void storeFinishedJobMetrics(JobMaster jobMaster) {
        List<RawJobMetrics> currJobMetrics = jobMaster.getCurrJobMetrics();
        JobMetrics jobMetrics = JobMetricsUtil.toJobMetrics(currJobMetrics);
        Long jobId = jobMaster.getJobImmutableInformation().getJobId();
        finishedJobMetricsImap.put(jobId, jobMetrics, 14, TimeUnit.DAYS);
        //Clean TaskGroupContext for TaskExecutionServer
        jobMaster.cleanTaskGroupContext();
    }

    private JobStateData toJobStateMapper(JobMaster jobMaster) {

        Long jobId = jobMaster.getJobImmutableInformation().getJobId();
        Map<PipelineLocation, PipelineStateData> pipelineStateMapperMap = new HashMap<>();

        jobMaster.getPhysicalPlan().getPipelineList().forEach(pipeline -> {
            PipelineLocation pipelineLocation = pipeline.getPipelineLocation();
            PipelineStatus pipelineState = (PipelineStatus) runningJobStateIMap.get(pipelineLocation);
            Map<TaskGroupLocation, ExecutionState> taskStateMap = new HashMap<>();
            pipeline.getCoordinatorVertexList().forEach(coordinator -> {
                TaskGroupLocation taskGroupLocation = coordinator.getTaskGroupLocation();
                taskStateMap.put(taskGroupLocation, (ExecutionState) runningJobStateIMap.get(taskGroupLocation));
            });
            pipeline.getPhysicalVertexList().forEach(task -> {
                TaskGroupLocation taskGroupLocation = task.getTaskGroupLocation();
                taskStateMap.put(taskGroupLocation, (ExecutionState) runningJobStateIMap.get(taskGroupLocation));
            });

            PipelineStateData pipelineStateData = new PipelineStateData(pipelineState, taskStateMap);
            pipelineStateMapperMap.put(pipelineLocation, pipelineStateData);
        });
        JobStatus jobStatus = (JobStatus) runningJobStateIMap.get(jobId);

        return new JobStateData(jobId, jobStatus, pipelineStateMapperMap);
    }

    @AllArgsConstructor
    @Data
    public static final class JobStatusData implements Serializable {
        Long jobId;
        JobStatus jobStatus;
    }

    @AllArgsConstructor
    @Data
    public static final class JobStateData implements Serializable{
        Long jobId;
        JobStatus jobStatus;
        Map<PipelineLocation, PipelineStateData> pipelineStateMapperMap;
    }

    @AllArgsConstructor
    @Data
    public static final class PipelineStateData implements Serializable{
        PipelineStatus pipelineStatus;
        Map<TaskGroupLocation, ExecutionState> executionStateMap;
    }
}
