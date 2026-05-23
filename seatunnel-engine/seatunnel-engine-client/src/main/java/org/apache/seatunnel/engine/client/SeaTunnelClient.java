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

import org.apache.seatunnel.api.metadata.MetadataProviderManager;
import org.apache.seatunnel.common.utils.JsonUtils;
import org.apache.seatunnel.engine.client.job.ClientJobExecutionEnvironment;
import org.apache.seatunnel.engine.client.job.JobClient;
import org.apache.seatunnel.engine.client.job.JobMetricsRunner.JobMetricsSummary;
import org.apache.seatunnel.engine.common.config.JobConfig;
import org.apache.seatunnel.engine.common.config.SeaTunnelConfig;
import org.apache.seatunnel.engine.core.job.JobDAGInfo;
import org.apache.seatunnel.engine.core.protocol.codec.SeaTunnelGetClusterHealthMetricsCodec;
import org.apache.seatunnel.engine.core.protocol.codec.SeaTunnelPrintMessageCodec;

import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.cluster.Member;
import com.hazelcast.logging.ILogger;
import lombok.Getter;
import lombok.NonNull;

import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

public class SeaTunnelClient implements SeaTunnelClientInstance, AutoCloseable {
    private static final int INVALID_METRICS_LOG_INTERVAL_MS = 60_000;
    private static final int INVALID_METRICS_LOG_PREFIX_MAX_LEN = 512;
    private static final int INVALID_METRICS_LOG_TOKEN_MAX_LEN = 128;
    private static final int INVALID_METRICS_LOG_TOKEN_MAX_COUNT = 5;
    private static final AtomicLong LAST_INVALID_METRICS_LOG_TIME_MS = new AtomicLong(0L);
    private final SeaTunnelHazelcastClient hazelcastClient;
    @Getter private final JobClient jobClient;

    public SeaTunnelClient(@NonNull ClientConfig clientConfig) {
        this.hazelcastClient = new SeaTunnelHazelcastClient(clientConfig);
        this.jobClient = new JobClient(this.hazelcastClient);
    }

    @Override
    public ClientJobExecutionEnvironment createExecutionContext(
            @NonNull String filePath,
            @NonNull JobConfig jobConfig,
            @NonNull SeaTunnelConfig seaTunnelConfig) {
        return createExecutionContext(filePath, null, jobConfig, seaTunnelConfig);
    }

    @Override
    public ClientJobExecutionEnvironment createExecutionContext(
            @NonNull String filePath,
            List<String> variables,
            @NonNull JobConfig jobConfig,
            @NonNull SeaTunnelConfig seaTunnelConfig) {
        return new ClientJobExecutionEnvironment(
                jobConfig, filePath, variables, hazelcastClient, seaTunnelConfig, null);
    }

    @Override
    public ClientJobExecutionEnvironment createExecutionContext(
            @NonNull String filePath,
            List<String> variables,
            @NonNull JobConfig jobConfig,
            @NonNull SeaTunnelConfig seaTunnelConfig,
            Long jobId) {
        return new ClientJobExecutionEnvironment(
                jobConfig, filePath, variables, hazelcastClient, seaTunnelConfig, jobId);
    }

    @Override
    public ClientJobExecutionEnvironment restoreExecutionContext(
            @NonNull String filePath,
            @NonNull JobConfig jobConfig,
            @NonNull SeaTunnelConfig seaTunnelConfig,
            @NonNull Long jobId) {
        return restoreExecutionContext(filePath, null, jobConfig, seaTunnelConfig, jobId);
    }

    @Override
    public ClientJobExecutionEnvironment restoreExecutionContext(
            @NonNull String filePath,
            List<String> variables,
            @NonNull JobConfig jobConfig,
            @NonNull SeaTunnelConfig seaTunnelConfig,
            @NonNull Long jobId) {
        return new ClientJobExecutionEnvironment(
                jobConfig, filePath, variables, hazelcastClient, seaTunnelConfig, true, jobId);
    }

    @Override
    public JobClient createJobClient() {
        return new JobClient(hazelcastClient);
    }

    @Override
    public void close() {
        hazelcastClient.getHazelcastInstance().shutdown();
        MetadataProviderManager.closeProviders();
    }

    public ILogger getLogger() {
        return hazelcastClient.getLogger(getClass());
    }

    public String printMessageToMaster(@NonNull String msg) {
        return hazelcastClient.requestOnMasterAndDecodeResponse(
                SeaTunnelPrintMessageCodec.encodeRequest(msg),
                SeaTunnelPrintMessageCodec::decodeResponse);
    }

    /**
     * get job status and the tasks status
     *
     * @param jobId jobId
     */
    @Deprecated
    public String getJobDetailStatus(Long jobId) {
        return jobClient.getJobDetailStatus(jobId);
    }

    /** list all jobId and job status */
    @Deprecated
    public String listJobStatus() {
        return jobClient.listJobStatus(false);
    }

    /**
     * get one job status
     *
     * @param jobId jobId
     */
    @Deprecated
    public String getJobStatus(Long jobId) {
        return jobClient.getJobStatus(jobId);
    }

    @Deprecated
    public String getJobMetrics(Long jobId) {
        return jobClient.getJobMetrics(jobId);
    }

    @Deprecated
    public void savePointJob(Long jobId) {
        jobClient.savePointJob(jobId);
    }

    @Deprecated
    public void cancelJob(Long jobId) {
        jobClient.cancelJob(jobId);
    }

    public JobDAGInfo getJobInfo(Long jobId) {
        return jobClient.getJobInfo(jobId);
    }

    public JobMetricsSummary getJobMetricsSummary(Long jobId) {
        return jobClient.getJobMetricsSummary(jobId);
    }

    public Map<String, String> getClusterHealthMetrics() {
        Set<Member> members = hazelcastClient.getHazelcastInstance().getCluster().getMembers();
        Map<String, String> healthMetricsMap = new HashMap<>();
        members.forEach(
                member -> {
                    healthMetricsMap.put(
                            member.getAddress().toString(), getMetricsByMember(member));
                });
        return healthMetricsMap;
    }

    public String getHealthMetrics(String address) {
        Set<Member> members = hazelcastClient.getHazelcastInstance().getCluster().getMembers();
        Member member =
                members.stream()
                        .filter(m -> m.getAddress().toString().equals(address))
                        .findFirst()
                        .orElseThrow(
                                () ->
                                        new IllegalArgumentException(
                                                "Member with address "
                                                        + address
                                                        + " not found in the cluster."));
        return getMetricsByMember(member);
    }

    private String getMetricsByMember(Member member) {
        String metrics =
                hazelcastClient.requestAndDecodeResponse(
                        member.getUuid(),
                        SeaTunnelGetClusterHealthMetricsCodec.encodeRequest(),
                        SeaTunnelGetClusterHealthMetricsCodec::decodeResponse);
        return JsonUtils.toJsonString(
                parseHealthMetricsString(metrics, member.getAddress().toString(), getLogger()));
    }

    static Map<String, String> parseHealthMetricsString(String metrics) {
        return parseHealthMetricsString(metrics, null, null);
    }

    static Map<String, String> parseHealthMetricsString(
            String metrics, String memberAddress, ILogger logger) {
        Map<String, String> kvMap = new LinkedHashMap<>();
        if (metrics == null || metrics.isEmpty()) {
            return kvMap;
        }

        // SeaTunnelHealthMonitor#render uses ", " as pair separators. Splitting on ",\\s+" avoids
        // breaking values which may contain commas (e.g. decimal separator under some locales).
        String[] pairs = metrics.split(",\\s+");
        List<String> invalidTokens =
                Arrays.stream(pairs)
                        .map(kv -> kv == null ? "" : kv.trim())
                        .filter(trimmed -> !trimmed.isEmpty())
                        .filter(trimmed -> trimmed.indexOf('=') <= 0)
                        .limit(INVALID_METRICS_LOG_TOKEN_MAX_COUNT)
                        .map(token -> truncateForLog(token, INVALID_METRICS_LOG_TOKEN_MAX_LEN))
                        .collect(Collectors.toList());

        Arrays.stream(pairs)
                .forEach(
                        kv -> {
                            if (kv == null) {
                                return;
                            }
                            String trimmed = kv.trim();
                            if (trimmed.isEmpty()) {
                                return;
                            }
                            int eqIndex = trimmed.indexOf('=');
                            if (eqIndex <= 0) {
                                return;
                            }
                            String key = trimmed.substring(0, eqIndex).trim();
                            String value =
                                    eqIndex == trimmed.length() - 1
                                            ? ""
                                            : trimmed.substring(eqIndex + 1).trim();
                            if (!key.isEmpty()) {
                                kvMap.put(key, value);
                            }
                        });

        logInvalidHealthMetricsIfNeeded(metrics, memberAddress, invalidTokens, logger);
        return kvMap;
    }

    private static void logInvalidHealthMetricsIfNeeded(
            String metrics, String memberAddress, List<String> invalidTokens, ILogger logger) {
        if (logger == null || invalidTokens == null || invalidTokens.isEmpty()) {
            return;
        }
        if (!logger.isWarningEnabled()) {
            return;
        }
        long now = System.currentTimeMillis();
        long last = LAST_INVALID_METRICS_LOG_TIME_MS.get();
        if (now - last < INVALID_METRICS_LOG_INTERVAL_MS) {
            return;
        }
        if (!LAST_INVALID_METRICS_LOG_TIME_MS.compareAndSet(last, now)) {
            return;
        }

        String address = memberAddress == null ? "unknown" : memberAddress;
        String prefix =
                truncateForLog(metrics == null ? "" : metrics, INVALID_METRICS_LOG_PREFIX_MAX_LEN);
        logger.warning(
                "Invalid zeta health metrics token(s) from member "
                        + address
                        + ", tokens="
                        + invalidTokens
                        + ", rawPrefix="
                        + prefix);
    }

    private static String truncateForLog(String s, int maxLen) {
        if (s == null) {
            return "";
        }
        String normalized = s.replace('\n', ' ').replace('\r', ' ');
        if (maxLen <= 0) {
            return "";
        }
        return normalized.length() <= maxLen ? normalized : normalized.substring(0, maxLen) + "...";
    }
}
