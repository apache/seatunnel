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

import org.apache.seatunnel.shade.com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.seatunnel.shade.com.fasterxml.jackson.core.type.TypeReference;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.JsonNode;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.ObjectMapper;

import org.apache.seatunnel.engine.client.SeaTunnelHazelcastClient;
import org.apache.seatunnel.engine.client.util.ContentFormatUtil;
import org.apache.seatunnel.engine.common.Constant;
import org.apache.seatunnel.engine.common.utils.PassiveCompletableFuture;
import org.apache.seatunnel.engine.core.job.JobDAGInfo;
import org.apache.seatunnel.engine.core.job.JobImmutableInformation;
import org.apache.seatunnel.engine.core.job.JobStatus;
import org.apache.seatunnel.engine.core.job.JobStatusData;
import org.apache.seatunnel.engine.core.protocol.codec.SeaTunnelCancelJobCodec;
import org.apache.seatunnel.engine.core.protocol.codec.SeaTunnelGetJobDetailStatusCodec;
import org.apache.seatunnel.engine.core.protocol.codec.SeaTunnelGetJobInfoCodec;
import org.apache.seatunnel.engine.core.protocol.codec.SeaTunnelGetJobMetricsCodec;
import org.apache.seatunnel.engine.core.protocol.codec.SeaTunnelGetJobStatusCodec;
import org.apache.seatunnel.engine.core.protocol.codec.SeaTunnelGetRunningJobMetricsCodec;
import org.apache.seatunnel.engine.core.protocol.codec.SeaTunnelListJobStatusCodec;
import org.apache.seatunnel.engine.core.protocol.codec.SeaTunnelSavePointJobCodec;

import lombok.NonNull;

import java.util.List;

public class JobClient {
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    private final SeaTunnelHazelcastClient hazelcastClient;

    public JobClient(@NonNull SeaTunnelHazelcastClient hazelcastClient) {
        this.hazelcastClient = hazelcastClient;
    }

    public long getNewJobId() {
        return hazelcastClient
                .getHazelcastInstance()
                .getFlakeIdGenerator(Constant.SEATUNNEL_ID_GENERATOR_NAME)
                .newId();
    }

    public ClientJobProxy createJobProxy(@NonNull JobImmutableInformation jobImmutableInformation) {
        return new ClientJobProxy(hazelcastClient, jobImmutableInformation);
    }

    public ClientJobProxy getJobProxy(@NonNull Long jobId) {
        return new ClientJobProxy(hazelcastClient, jobId);
    }

    public String getJobDetailStatus(Long jobId) {
        return hazelcastClient.requestOnMasterAndDecodeResponse(
                SeaTunnelGetJobDetailStatusCodec.encodeRequest(jobId),
                SeaTunnelGetJobDetailStatusCodec::decodeResponse);
    }

    /** list all jobId and job status */
    public String listJobStatus(boolean format) {
        String jobStatusStr =
                hazelcastClient.requestOnMasterAndDecodeResponse(
                        SeaTunnelListJobStatusCodec.encodeRequest(),
                        SeaTunnelListJobStatusCodec::decodeResponse);
        if (!format) {
            return jobStatusStr;
        } else {
            try {
                List<JobStatusData> statusDataList =
                        OBJECT_MAPPER.readValue(
                                jobStatusStr, new TypeReference<List<JobStatusData>>() {});
                statusDataList.sort(
                        (s1, s2) -> {
                            if (s1.getSubmitTime() == s2.getSubmitTime()) {
                                return 0;
                            }
                            return s1.getSubmitTime() > s2.getSubmitTime() ? -1 : 1;
                        });
                return ContentFormatUtil.format(statusDataList);
            } catch (JsonProcessingException e) {
                throw new RuntimeException(e);
            }
        }
    }

    /**
     * get one job status
     *
     * @param jobId jobId
     */
    public String getJobStatus(Long jobId) {
        int jobStatusOrdinal =
                hazelcastClient.requestOnMasterAndDecodeResponse(
                        SeaTunnelGetJobStatusCodec.encodeRequest(jobId),
                        SeaTunnelGetJobStatusCodec::decodeResponse);
        return JobStatus.values()[jobStatusOrdinal].toString();
    }

    public String getJobMetrics(Long jobId) {
        return hazelcastClient.requestOnMasterAndDecodeResponse(
                SeaTunnelGetJobMetricsCodec.encodeRequest(jobId),
                SeaTunnelGetJobMetricsCodec::decodeResponse);
    }

    public String getRunningJobMetrics() {
        return hazelcastClient.requestOnMasterAndDecodeResponse(
                SeaTunnelGetRunningJobMetricsCodec.encodeRequest(),
                SeaTunnelGetRunningJobMetricsCodec::decodeResponse);
    }

    public void savePointJob(Long jobId) {
        PassiveCompletableFuture<Void> cancelFuture =
                hazelcastClient.requestOnMasterAndGetCompletableFuture(
                        SeaTunnelSavePointJobCodec.encodeRequest(jobId));

        cancelFuture.join();
    }

    public void cancelJob(Long jobId) {
        PassiveCompletableFuture<Void> cancelFuture =
                hazelcastClient.requestOnMasterAndGetCompletableFuture(
                        SeaTunnelCancelJobCodec.encodeRequest(jobId));

        cancelFuture.join();
    }

    public JobDAGInfo getJobInfo(Long jobId) {
        return hazelcastClient
                .getSerializationService()
                .toObject(
                        hazelcastClient.requestOnMasterAndDecodeResponse(
                                SeaTunnelGetJobInfoCodec.encodeRequest(jobId),
                                SeaTunnelGetJobInfoCodec::decodeResponse));
    }

    public JobMetricsRunner.JobMetricsSummary getJobMetricsSummary(Long jobId) {
        long sourceReadCount = 0L;
        long sinkWriteCount = 0L;
        String jobMetrics = getJobMetrics(jobId);
        try {
            JsonNode jsonNode = OBJECT_MAPPER.readTree(jobMetrics);
            JsonNode sourceReaders = jsonNode.get("SourceReceivedCount");
            JsonNode sinkWriters = jsonNode.get("SinkWriteCount");
            for (int i = 0; i < sourceReaders.size(); i++) {
                JsonNode sourceReader = sourceReaders.get(i);
                JsonNode sinkWriter = sinkWriters.get(i);
                sourceReadCount += sourceReader.get("value").asLong();
                sinkWriteCount += sinkWriter.get("value").asLong();
            }
            return new JobMetricsRunner.JobMetricsSummary(sourceReadCount, sinkWriteCount);
            // Add NullPointerException because of metrics information can be empty like {}
        } catch (JsonProcessingException | NullPointerException e) {
            return new JobMetricsRunner.JobMetricsSummary(sourceReadCount, sinkWriteCount);
        }
    }
}
