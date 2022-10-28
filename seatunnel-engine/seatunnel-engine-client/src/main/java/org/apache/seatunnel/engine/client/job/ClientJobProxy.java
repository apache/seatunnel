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
import org.apache.seatunnel.engine.common.utils.PassiveCompletableFuture;
import org.apache.seatunnel.engine.core.job.Job;
import org.apache.seatunnel.engine.core.job.JobImmutableInformation;
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
    private final JobImmutableInformation jobImmutableInformation;

    public ClientJobProxy(@NonNull SeaTunnelHazelcastClient seaTunnelHazelcastClient,
                          @NonNull JobImmutableInformation jobImmutableInformation) {
        this.seaTunnelHazelcastClient = seaTunnelHazelcastClient;
        this.jobImmutableInformation = jobImmutableInformation;
        submitJob();
    }

    @Override
    public long getJobId() {
        return jobImmutableInformation.getJobId();
    }

    private void submitJob() {
        LOGGER.info(String.format("start submit job, job id: %s, with plugin jar %s", jobImmutableInformation.getJobId(), jobImmutableInformation.getPluginJarsUrls()));
        ClientMessage request = SeaTunnelSubmitJobCodec.encodeRequest(jobImmutableInformation.getJobId(),
            seaTunnelHazelcastClient.getSerializationService().toData(jobImmutableInformation));
        PassiveCompletableFuture<Void> submitJobFuture =
            seaTunnelHazelcastClient.requestOnMasterAndGetCompletableFuture(request);
        submitJobFuture.join();
    }

    /**
     * This method will block even the Job turn to a EndState
     *
     * @return The job final status
     */
    public JobStatus waitForJobComplete() {
        JobStatus jobStatus;
        try {
            jobStatus = RetryUtils.retryWithException(() -> {
                PassiveCompletableFuture<JobStatus> jobFuture = doWaitForJobComplete();
                return jobFuture.get();
            }, new RetryUtils.RetryMaterial(Constant.OPERATION_RETRY_TIME, true,
                exception -> exception instanceof RuntimeException, Constant.OPERATION_RETRY_SLEEP));
        } catch (Exception e) {
            LOGGER.info(String.format("Job %s (%s) end with unknown state, and throw Exception: %s",
                jobImmutableInformation.getJobId(),
                jobImmutableInformation.getJobConfig().getName(),
                ExceptionUtils.getMessage(e)));
            throw new RuntimeException(e);
        }
        LOGGER.info(String.format("Job %s (%s) end with state %s",
            jobImmutableInformation.getJobConfig().getName(),
            jobImmutableInformation.getJobId(),
            jobStatus));
        return jobStatus;
    }

    @Override
    public PassiveCompletableFuture<JobStatus> doWaitForJobComplete() {
        return seaTunnelHazelcastClient.requestOnMasterAndGetCompletableFuture(
            SeaTunnelWaitForJobCompleteCodec.encodeRequest(jobImmutableInformation.getJobId()),
            response -> JobStatus.values()[SeaTunnelWaitForJobCompleteCodec.decodeResponse(response)]);
    }

    @Override
    public void cancelJob() {
        PassiveCompletableFuture<Void> cancelFuture = seaTunnelHazelcastClient.requestOnMasterAndGetCompletableFuture(
            SeaTunnelCancelJobCodec.encodeRequest(jobImmutableInformation.getJobId()));

        cancelFuture.join();
    }

    @Override
    public JobStatus getJobStatus() {
        int jobStatusOrdinal = seaTunnelHazelcastClient.requestOnMasterAndDecodeResponse(
            SeaTunnelGetJobStatusCodec.encodeRequest(jobImmutableInformation.getJobId()),
            SeaTunnelGetJobStatusCodec::decodeResponse);
        return JobStatus.values()[jobStatusOrdinal];
    }

}
