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

package org.apache.seatunnel.engine.client;

import org.apache.seatunnel.engine.client.job.JobClient;
import org.apache.seatunnel.engine.client.job.JobExecutionEnvironment;
import org.apache.seatunnel.engine.common.config.JobConfig;
import org.apache.seatunnel.engine.common.utils.PassiveCompletableFuture;
import org.apache.seatunnel.engine.core.job.JobDAGInfo;
import org.apache.seatunnel.engine.core.job.JobStatus;
import org.apache.seatunnel.engine.core.protocol.codec.SeaTunnelCancelJobCodec;
import org.apache.seatunnel.engine.core.protocol.codec.SeaTunnelGetJobDetailStatusCodec;
import org.apache.seatunnel.engine.core.protocol.codec.SeaTunnelGetJobInfoCodec;
import org.apache.seatunnel.engine.core.protocol.codec.SeaTunnelGetJobMetricsCodec;
import org.apache.seatunnel.engine.core.protocol.codec.SeaTunnelGetJobStatusCodec;
import org.apache.seatunnel.engine.core.protocol.codec.SeaTunnelListJobStatusCodec;
import org.apache.seatunnel.engine.core.protocol.codec.SeaTunnelPrintMessageCodec;

import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.logging.ILogger;
import lombok.NonNull;

public class SeaTunnelClient implements SeaTunnelClientInstance {
    private final SeaTunnelHazelcastClient hazelcastClient;

    public SeaTunnelClient(@NonNull ClientConfig clientConfig) {
        this.hazelcastClient = new SeaTunnelHazelcastClient(clientConfig);
    }

    @Override
    public JobExecutionEnvironment createExecutionContext(@NonNull String filePath, @NonNull JobConfig jobConfig) {
        return new JobExecutionEnvironment(jobConfig, filePath, hazelcastClient);
    }

    @Override
    public JobClient createJobClient() {
        return new JobClient(hazelcastClient);
    }

    @Override
    public void close() {
        hazelcastClient.getHazelcastInstance().shutdown();
    }

    public ILogger getLogger() {
        return hazelcastClient.getLogger(getClass());
    }

    public String printMessageToMaster(@NonNull String msg) {
        return hazelcastClient.requestOnMasterAndDecodeResponse(
            SeaTunnelPrintMessageCodec.encodeRequest(msg),
            SeaTunnelPrintMessageCodec::decodeResponse
        );
    }

    public void shutdown() {
        if (hazelcastClient != null) {
            hazelcastClient.shutdown();
        }
    }

    /**
     * get job status and the tasks status
     *
     * @param jobId jobId
     */
    public String getJobDetailStatus(Long jobId) {
        return hazelcastClient.requestOnMasterAndDecodeResponse(
            SeaTunnelGetJobDetailStatusCodec.encodeRequest(jobId),
            SeaTunnelGetJobDetailStatusCodec::decodeResponse
        );
    }

    /**
     * list all jobId and job status
     */
    public String listJobStatus() {
        return hazelcastClient.requestOnMasterAndDecodeResponse(
            SeaTunnelListJobStatusCodec.encodeRequest(),
            SeaTunnelListJobStatusCodec::decodeResponse
        );
    }

    /**
     * get one job status
     * @param jobId jobId
     */
    public String getJobStatus(Long jobId) {
        int jobStatusOrdinal = hazelcastClient.requestOnMasterAndDecodeResponse(
            SeaTunnelGetJobStatusCodec.encodeRequest(jobId),
            SeaTunnelGetJobStatusCodec::decodeResponse);
        return JobStatus.values()[jobStatusOrdinal].toString();
    }

    public String getJobMetrics(Long jobId) {
        return hazelcastClient.requestOnMasterAndDecodeResponse(
            SeaTunnelGetJobMetricsCodec.encodeRequest(jobId),
            SeaTunnelGetJobMetricsCodec::decodeResponse
        );
    }

    public void cancelJob(Long jobId) {
        PassiveCompletableFuture<Void> cancelFuture = hazelcastClient.requestOnMasterAndGetCompletableFuture(
            SeaTunnelCancelJobCodec.encodeRequest(jobId));

        cancelFuture.join();
    }

    public JobDAGInfo getJobInfo(Long jobId) {
        return hazelcastClient.getSerializationService().toObject(hazelcastClient.requestOnMasterAndDecodeResponse(
            SeaTunnelGetJobInfoCodec.encodeRequest(jobId),
            SeaTunnelGetJobInfoCodec::decodeResponse
        ));
    }
}
