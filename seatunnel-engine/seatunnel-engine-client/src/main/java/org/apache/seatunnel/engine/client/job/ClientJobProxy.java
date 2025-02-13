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

package org.apache.seatunnel.engine.client.job;

import org.apache.seatunnel.common.utils.ExceptionUtils;
import org.apache.seatunnel.common.utils.RetryUtils;
import org.apache.seatunnel.engine.client.SeaTunnelHazelcastClient;
import org.apache.seatunnel.engine.common.Constant;
import org.apache.seatunnel.engine.common.exception.SeaTunnelEngineException;
import org.apache.seatunnel.engine.common.utils.ExceptionUtil;
import org.apache.seatunnel.engine.common.utils.PassiveCompletableFuture;
import org.apache.seatunnel.engine.core.job.Job;
import org.apache.seatunnel.engine.core.job.JobImmutableInformation;
import org.apache.seatunnel.engine.core.job.JobResult;
import org.apache.seatunnel.engine.core.job.JobStatus;
import org.apache.seatunnel.engine.core.protocol.codec.SeaTunnelCancelJobCodec;
import org.apache.seatunnel.engine.core.protocol.codec.SeaTunnelGetJobStatusCodec;
import org.apache.seatunnel.engine.core.protocol.codec.SeaTunnelSubmitJobCodec;
import org.apache.seatunnel.engine.core.protocol.codec.SeaTunnelWaitForJobCompleteCodec;

import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import lombok.NonNull;

public class ClientJobProxy implements Job {
    private static final ILogger LOGGER = Logger.getLogger(ClientJobProxy.class);
    private final SeaTunnelHazelcastClient seaTunnelHazelcastClient;
    private final Long jobId;
    private JobResult jobResult;

    public ClientJobProxy(
            @NonNull SeaTunnelHazelcastClient seaTunnelHazelcastClient,
            @NonNull JobImmutableInformation jobImmutableInformation) {
        this.seaTunnelHazelcastClient = seaTunnelHazelcastClient;
        this.jobId = jobImmutableInformation.getJobId();
        submitJob(jobImmutableInformation);
    }

    public ClientJobProxy(@NonNull SeaTunnelHazelcastClient seaTunnelHazelcastClient, Long jobId) {
        this.seaTunnelHazelcastClient = seaTunnelHazelcastClient;
        this.jobId = jobId;
    }

    @Override
    public long getJobId() {
        return jobId;
    }

    private void submitJob(JobImmutableInformation jobImmutableInformation) {
        LOGGER.info(
                String.format(
                        "Start submit job, job id: %s, with plugin jar %s",
                        jobImmutableInformation.getJobId(),
                        jobImmutableInformation.getPluginJarsUrls()));
        ClientMessage request =
                SeaTunnelSubmitJobCodec.encodeRequest(
                        jobImmutableInformation.getJobId(),
                        seaTunnelHazelcastClient
                                .getSerializationService()
                                .toData(jobImmutableInformation),
                        jobImmutableInformation.isStartWithSavePoint());
        PassiveCompletableFuture<Void> submitJobFuture =
                seaTunnelHazelcastClient.requestOnMasterAndGetCompletableFuture(request);
        submitJobFuture.join();
        LOGGER.info(
                String.format(
                        "Submit job finished, job id: %s, job name: %s",
                        jobImmutableInformation.getJobId(), jobImmutableInformation.getJobName()));
    }

    /**
     * This method will block even the Job turn to a EndState
     *
     * @return The job final status
     */
    @Override
    public JobResult waitForJobCompleteV2() {
        try {
            jobResult =
                    RetryUtils.retryWithException(
                            () -> {
                                PassiveCompletableFuture<JobResult> jobFuture =
                                        doWaitForJobComplete();
                                return jobFuture.get();
                            },
                            new RetryUtils.RetryMaterial(
                                    100000,
                                    true,
                                    ExceptionUtil::isOperationNeedRetryException,
                                    Constant.OPERATION_RETRY_SLEEP));
            if (jobResult == null) {
                throw new SeaTunnelEngineException("failed to fetch job result");
            }
        } catch (Exception e) {
            LOGGER.severe(
                    String.format(
                            "Job (%s) end with unknown state, and throw Exception: %s",
                            jobId, ExceptionUtils.getMessage(e)));
            throw new RuntimeException(e);
        }
        LOGGER.info(String.format("Job (%s) end with state %s", jobId, jobResult.getStatus()));
        return jobResult;
    }

    public JobResult getJobResultCache() {
        return jobResult;
    }

    @Override
    public PassiveCompletableFuture<JobResult> doWaitForJobComplete() {
        return new PassiveCompletableFuture<>(
                seaTunnelHazelcastClient
                        .requestOnMasterAndGetCompletableFuture(
                                SeaTunnelWaitForJobCompleteCodec.encodeRequest(jobId),
                                SeaTunnelWaitForJobCompleteCodec::decodeResponse)
                        .thenApply(
                                jobResult ->
                                        seaTunnelHazelcastClient
                                                .getSerializationService()
                                                .toObject(jobResult)));
    }

    @Override
    public void cancelJob() {
        PassiveCompletableFuture<Void> cancelFuture =
                seaTunnelHazelcastClient.requestOnMasterAndGetCompletableFuture(
                        SeaTunnelCancelJobCodec.encodeRequest(jobId));

        cancelFuture.join();
    }

    @Override
    public JobStatus getJobStatus() {
        int jobStatusOrdinal =
                seaTunnelHazelcastClient.requestOnMasterAndDecodeResponse(
                        SeaTunnelGetJobStatusCodec.encodeRequest(jobId),
                        SeaTunnelGetJobStatusCodec::decodeResponse);
        return JobStatus.values()[jobStatusOrdinal];
    }
}
