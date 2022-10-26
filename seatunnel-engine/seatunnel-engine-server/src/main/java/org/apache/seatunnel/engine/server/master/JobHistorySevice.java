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

import org.apache.seatunnel.engine.core.job.JobStatus;
import org.apache.seatunnel.engine.core.job.PipelineStatus;
import org.apache.seatunnel.engine.server.dag.physical.PipelineLocation;
import org.apache.seatunnel.engine.server.execution.ExecutionState;
import org.apache.seatunnel.engine.server.execution.TaskGroupLocation;

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

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

public class JobHistorySevice {
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
     * JobStateMapper Indicates the status of the job, pipeline, and task
     */
    //TODO need to limit the amount of storage
    private final IMap<Long, JobStateMapper> finishedJobStateImap;

    public JobHistorySevice(
        IMap<Object, Object> runningJobStateIMap,
        ILogger logger,
        Map<Long, JobMaster> runningJobMasterMap,
        IMap<Long, JobStateMapper> finishedJobStateImap
    ) {
        this.runningJobStateIMap = runningJobStateIMap;
        this.logger = logger;
        this.runningJobMasterMap = runningJobMasterMap;
        this.finishedJobStateImap = finishedJobStateImap;
    }

    // Gets the status of a running and completed job
    public String listAllJob() {
        ObjectMapper objectMapper = new ObjectMapper();
        ObjectNode objectNode = objectMapper.createObjectNode();
        objectMapper.configure(SerializationFeature.FAIL_ON_EMPTY_BEANS, false);
        ArrayNode jobs = objectNode.putArray("jobs");

        Stream.concat(runningJobMasterMap.values().stream().map(this::toJobStateMapper),
                finishedJobStateImap.values().stream())
            .forEach(jobStateMapper -> {
                JobStatusMapper jobStatusMapper = new JobStatusMapper(jobStateMapper.jobId, jobStateMapper.jobStatus);
                JsonNode jsonNode = objectMapper.valueToTree(jobStatusMapper);
                jobs.add(jsonNode);
            });
        return jobs.toString();
    }

    // Get detailed status of a single job
    public JobStateMapper getJobStatus(Long jobId) {
        return runningJobMasterMap.containsKey(jobId) ? toJobStateMapper(runningJobMasterMap.get(jobId)) :
            finishedJobStateImap.getOrDefault(jobId, null);
    }

    // Get detailed status of a single job as json
    public String getJobStatusAsString(Long jobId) {
        ObjectMapper objectMapper = new ObjectMapper();
        JobStateMapper jobStatus = getJobStatus(jobId);
        if (null != jobStatus) {
            try {
                return objectMapper.writeValueAsString(jobStatus);
            } catch (JsonProcessingException e) {
                logger.severe("serialize jobStateMapper err", e);
                ObjectNode objectNode = objectMapper.createObjectNode();
                objectNode.put("err", "serialize jobStateMapper err");
                return objectNode.toString();
            }
        } else {
            ObjectNode objectNode = objectMapper.createObjectNode();
            objectNode.put("err", String.format("jobId : %s not found", jobId));
            return objectNode.toString();
        }
    }

    @SuppressWarnings("checkstyle:MagicNumber")
    public void storeFinishedJobState(JobMaster jobMaster) {
        JobStateMapper jobStateMapper = toJobStateMapper(jobMaster);
        finishedJobStateImap.put(jobStateMapper.jobId, jobStateMapper, 90, TimeUnit.DAYS);
    }

    private JobStateMapper toJobStateMapper(JobMaster jobMaster) {

        Long jobId = jobMaster.getJobImmutableInformation().getJobId();
        Map<PipelineLocation, PipelineStateMapper> pipelineStateMapperMap = new HashMap<>();

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

            PipelineStateMapper pipelineStateMapper = new PipelineStateMapper(pipelineState, taskStateMap);
            pipelineStateMapperMap.put(pipelineLocation, pipelineStateMapper);
        });
        JobStatus jobStatus = (JobStatus) runningJobStateIMap.get(jobId);

        return new JobStateMapper(jobId, jobStatus, pipelineStateMapperMap);
    }

    @AllArgsConstructor
    @Data
    public static final class JobStatusMapper {
        Long jobId;
        JobStatus jobStatus;
    }

    @AllArgsConstructor
    @Data
    public static final class JobStateMapper {
        Long jobId;
        JobStatus jobStatus;
        Map<PipelineLocation, PipelineStateMapper> pipelineStateMapperMap;
    }

    @AllArgsConstructor
    @Data
    public static final class PipelineStateMapper {
        PipelineStatus pipelineStatus;
        Map<TaskGroupLocation, ExecutionState> executionStateMap;
    }
}
