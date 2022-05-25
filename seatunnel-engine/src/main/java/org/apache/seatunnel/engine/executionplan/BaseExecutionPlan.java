/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.seatunnel.engine.executionplan;

import org.apache.seatunnel.engine.api.common.JobStatus;
import org.apache.seatunnel.engine.cache.CacheConfig;
import org.apache.seatunnel.engine.cache.CachePartition;
import org.apache.seatunnel.engine.cache.CacheSink;
import org.apache.seatunnel.engine.cache.CacheSource;
import org.apache.seatunnel.engine.cache.DataStreamCache;
import org.apache.seatunnel.engine.cache.DataStreamCachePartitionBuilder;
import org.apache.seatunnel.engine.config.Configuration;
import org.apache.seatunnel.engine.logicalplan.LogicalTask;
import org.apache.seatunnel.engine.task.TaskExecutionState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;

import static com.google.common.base.Preconditions.checkNotNull;

public class BaseExecutionPlan implements ExecutionPlan {

    static final Logger LOG = LoggerFactory.getLogger(BaseExecutionPlan.class);

    private int sourceParallelism;

    private Configuration configuration;

    private JobInformation jobInformation;

    private List<ExecutionTask> tasks;

    /**
     * The currently executed tasks, for callbacks.
     */
    private final Map<ExecutionId, Execution> currentExecutionMap;

    /**
     * Timestamps (in milliseconds as returned by {@code System.currentTimeMillis()} when the
     * execution graph transitioned into a certain state. The index into this array is the ordinal
     * of the enum value, i.e. the timestamp when the graph went into state "RUNNING" is at {@code
     * stateTimestamps[RUNNING.ordinal()]}.
     */
    private final long[] stateTimestamps;

    private int numFinishedExecutionTask;

    /**
     * The executor which is used to execute futures.
     */
    private final ScheduledExecutorService executorService;

    private JobStatus jobStatus = JobStatus.CREATED;

    /**
     * when job status turn to end, complete this future.
     */
    private final CompletableFuture<JobStatus> jobEndFuture = new CompletableFuture<>();

    public BaseExecutionPlan(ScheduledExecutorService executor, Configuration configuration, JobInformation jobInformation, Long initializationTimestamp, Integer sourceParallelism) {
        checkNotNull(executor, "executor can not be null");
        checkNotNull(configuration, "configuration can not be null");
        checkNotNull(jobInformation, "jobInformation can not be null");
        checkNotNull(initializationTimestamp, "initializationTimestamp can not be null");
        checkNotNull(sourceParallelism, "sourceParallelism can not be null");

        this.executorService = executor;
        this.configuration = configuration;
        this.jobInformation = jobInformation;

        currentExecutionMap = new HashMap<>();
        stateTimestamps = new long[JobStatus.values().length];
        this.stateTimestamps[JobStatus.INITIALIZING.ordinal()] = initializationTimestamp;
        this.stateTimestamps[JobStatus.CREATED.ordinal()] = System.currentTimeMillis();

        this.sourceParallelism = sourceParallelism;
    }

    public Configuration getConfiguration() {
        return configuration;
    }

    public JobInformation getJobInformation() {
        return jobInformation;
    }

    @Override
    public void initExecutionTasks(List<LogicalTask> logicalTasks) {
        this.tasks = new ArrayList<>(logicalTasks.size());

        if (logicalTasks.size() == 2) {
            // init cache partitionSelector
            LogicalTask beforeCacheLogicalTask = logicalTasks.get(0);
            CacheConfig cacheConfig = beforeCacheLogicalTask.getCacheConfig();
            DataStreamCache dataStreamCache = beforeCacheLogicalTask.getLogicalPlan().getDataStreamCache();
            DataStreamCachePartitionBuilder cachePartitionBuilder = dataStreamCache.createCachePartitionBuilder(beforeCacheLogicalTask.getCacheConfig());
            CachePartition[] cachePartitions = cachePartitionBuilder.createCachePartitions(cacheConfig, sourceParallelism);

            CacheSink cacheSink = (CacheSink) beforeCacheLogicalTask.getSink();
            cacheSink.initPartitionSelector(cachePartitionBuilder.createCacheSinkPartitionSelector(cacheConfig, cachePartitions));

            LogicalTask afterCacheLogicalTask = logicalTasks.get(1);
            CacheSource cacheSource = (CacheSource) afterCacheLogicalTask.getSource();
            cacheSource.initPartitionSelector(cachePartitionBuilder.createCacheSourcePartitionSelector(cacheConfig, cachePartitions));
        }

        createExecutionTasksFromLogicalTasks(logicalTasks);
    }

    @Override
    public void turnToRunning() {
        if (!updateJobState(JobStatus.CREATED, JobStatus.RUNNING)) {
            throw new IllegalStateException(
                    "Job may only be scheduled from state " + JobStatus.CREATED);
        }
    }

    @Override
    public boolean updateJobState(JobStatus current, JobStatus targetState) {
        // consistency check
        if (current.isEndState()) {
            String message = "Job is trying to leave terminal state " + current;
            LOG.error(message);
            throw new IllegalStateException(message);
        }

        // now do the actual state transition
        if (jobStatus == current) {
            jobStatus = targetState;
            LOG.info(
                    "Job {} ({}) turn from state {} to {}.",
                    jobInformation.getJobName(),
                    jobInformation.getJobId(),
                    current,
                    targetState);

            stateTimestamps[targetState.ordinal()] = System.currentTimeMillis();
            return true;
        } else {
            return false;
        }
    }

    @Override
    public boolean updateExecutionState(TaskExecutionState state) {
        ExecutionId executionID = state.getExecutionID();
        Execution execution = currentExecutionMap.get(executionID);
        if (execution == null) {
            return false;
        }

        return execution.turnState(state.getExecutionState());
    }

    @Override
    public CompletableFuture<JobStatus> getJobEndCompletableFuture() {
        return this.jobEndFuture;
    }

    private void createExecutionTasksFromLogicalTasks(List<LogicalTask> logicalTasks) {
        for (LogicalTask logicalTask : logicalTasks) {
            ExecutionTask executionTask = new ExecutionTask(
                    "st_task",
                    sourceParallelism,
                    logicalTask.getSource(),
                    logicalTask.getTransformations(),
                    logicalTask.getSink(),
                    this);

            executionTask.init();
            tasks.add(executionTask);
        }
    }

    public Map<ExecutionId, Execution> getCurrentExecutionMap() {
        return currentExecutionMap;
    }

    public ScheduledExecutorService getExecutorService() {
        return executorService;
    }

    @Override
    public List<ExecutionTask> getExecutionTasks() {
        return tasks;
    }

    @Override
    public void removeExecution(Execution execExecution) {
        Execution execution = currentExecutionMap.remove(execExecution.getExecutionID());

        if (execution != null && execution != execExecution) {
            throw new RuntimeException(new Exception(
                    "remove execution "
                            + execExecution
                            + " failed. Found for same ID execution "
                            + execution));
        }
    }

    @Override
    public void addExecution(Execution execution) {
        Execution preExecution = currentExecutionMap.putIfAbsent(execution.getExecutionID(), execution);
        if (preExecution != null) {
            throw new RuntimeException("add execution to execution plan error, {} exists" + execution.getExecutionID());
        }
    }

    @Override
    public void executionTaskFinish() {
        final int currFinishedNum = ++numFinishedExecutionTask;
        if (currFinishedNum == tasks.size()) {

            // check whether we are still in "RUNNING" and trigger the final cleanup
            if (jobStatus == JobStatus.RUNNING) {

                if (updateJobState(JobStatus.RUNNING, JobStatus.FINISHED)) {
                    LOG.info("Job {}({}) finished", jobInformation.getJobName(), jobInformation.getJobId());
                    jobEndFuture.complete(jobStatus);
                }
            }
        }
    }
}
