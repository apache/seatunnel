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

package org.apache.seatunnel.engine.e2e;

import org.apache.seatunnel.engine.client.SeaTunnelClient;
import org.apache.seatunnel.engine.client.job.ClientJobExecutionEnvironment;
import org.apache.seatunnel.engine.client.job.ClientJobProxy;
import org.apache.seatunnel.engine.common.config.ConfigProvider;
import org.apache.seatunnel.engine.common.config.JobConfig;
import org.apache.seatunnel.engine.common.config.SeaTunnelConfig;
import org.apache.seatunnel.engine.common.job.JobStatus;
import org.apache.seatunnel.engine.server.SeaTunnelServerStarter;
import org.apache.seatunnel.engine.server.rest.RestConstant;

import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.instance.impl.HazelcastInstanceImpl;
import io.restassured.common.mapper.TypeRef;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.net.ServerSocket;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static io.restassured.RestAssured.given;

/**
 * Scale/stress E2E coverage for sustained backpressure between a fast source and a deliberately
 * slow sink (Tier 3, item L1 of the Zeta extreme-case gap analysis).
 *
 * <p>The job pairs a {@code FakeSource} that produces as fast as the JVM allows
 * (split.read-interval = 0, a very large row.num) with an {@code InMemory} sink throttled to a
 * fixed ~500 rows/sec via {@code write_delay_ms}. Because the sink's writer thread is also the
 * thread that drains Zeta's bounded source-to-sink intermediate queue (an {@code
 * ArrayBlockingQueue} of fixed capacity, see {@code
 * TaskGroupWithIntermediateBlockingQueue#QUEUE_SIZE}), a slow writer keeps that queue saturated for
 * as long as the job runs, which in turn blocks the source's own enqueue calls. This is exactly the
 * "fast source outruns slow sink" condition the rest of this E2E suite has no coverage for.
 *
 * <p>What is asserted, and why each assertion is meaningful:
 *
 * <ul>
 *   <li>The job stays {@code RUNNING} for the whole sustained window - no crash, no OOM, no stuck
 *       state, despite the queue being kept full for tens of seconds.
 *   <li>The checkpoint-overview REST API's completed-checkpoint counter for the job's single
 *       pipeline never regresses and grows by a healthy minimum across the window - i.e.
 *       checkpoints keep completing on their configured schedule even while backpressure is
 *       continuously in effect, not just once before backpressure kicks in.
 *   <li>(Stronger, supplementary signal) The realtime-observability edges API reports a stable,
 *       positive queue capacity throughout - confirming the intermediate queue is a genuinely
 *       bounded structure, not something that silently grows - and a positive {@code emitBlockedNs}
 *       - confirming the fast source was actually measured blocking on that full queue, i.e. real
 *       backpressure was enforced end-to-end rather than merely "the job didn't crash".
 * </ul>
 */
@Slf4j
public class BackpressureSlowSinkIT {

    private static final String HOST = "http://localhost:";

    private static final String CONF_FILE =
            "stream_fast_fakesource_to_slow_inmemory_backpressure.conf";

    /** How long to sustain the slow-sink backpressure condition before asserting and stopping. */
    private static final long BACKPRESSURE_WINDOW_MS = TimeUnit.SECONDS.toMillis(90);

    /** Sampling cadence for checkpoint/queue observations during the sustained window. */
    private static final long POLL_INTERVAL_MS = TimeUnit.SECONDS.toMillis(10);

    /**
     * Window requested from the realtime-metrics edges API on each poll; wide enough to always
     * contain the freshest completed metrics bucket (bucket_ms defaults to 5000).
     */
    private static final long REALTIME_METRICS_WINDOW_MS = TimeUnit.SECONDS.toMillis(20);

    /**
     * Conservative lower bound for how many additional checkpoints must complete during {@link
     * #BACKPRESSURE_WINDOW_MS}. This is well below the nominal count implied by {@code
     * checkpoint.interval = 15000} in {@link #CONF_FILE} (~6 over 90s) to absorb job startup
     * ramp-up and CI scheduling slack while still proving checkpoints keep completing repeatedly,
     * not just once.
     */
    private static final long MIN_NEW_COMPLETED_CHECKPOINTS = 3;

    private HazelcastInstanceImpl node;
    private SeaTunnelClient engineClient;
    private SeaTunnelConfig config;
    private ClientJobProxy jobProxy;

    @BeforeEach
    void beforeEach() throws Exception {
        String testClusterName = TestUtils.getClusterName("BackpressureSlowSinkIT");
        config = ConfigProvider.locateAndGetSeaTunnelConfig();
        config.getEngineConfig().getHttpConfig().setPort(getAvailablePort());
        config.getEngineConfig().getHttpConfig().setEnabled(true);
        config.getHazelcastConfig().setClusterName(testClusterName);
        config.getEngineConfig().getSlotServiceConfig().setDynamicSlot(false);
        config.getEngineConfig().getSlotServiceConfig().setSlotNum(20);
        node = SeaTunnelServerStarter.createHazelcastInstance(config);

        String filePath = TestUtils.getResource(CONF_FILE);
        JobConfig jobConfig = new JobConfig();
        jobConfig.setName("backpressure_slow_sink_it");

        ClientConfig clientConfig = ConfigProvider.locateAndGetClientConfig();
        clientConfig.setClusterName(testClusterName);
        engineClient = new SeaTunnelClient(clientConfig);
        ClientJobExecutionEnvironment jobExecutionEnv =
                engineClient.createExecutionContext(filePath, jobConfig, config);

        jobProxy = jobExecutionEnv.execute();
        Awaitility.await()
                .atMost(2, TimeUnit.MINUTES)
                .untilAsserted(
                        () -> Assertions.assertEquals(JobStatus.RUNNING, jobProxy.getJobStatus()));
    }

    @AfterEach
    void afterEach() {
        try {
            if (jobProxy != null) {
                JobStatus status = jobProxy.getJobStatus();
                if (status != null && !status.isEndState()) {
                    jobProxy.cancelJob();
                }
            }
        } catch (Exception e) {
            log.warn("Failed to cancel the streaming job during teardown", e);
        }
        if (engineClient != null) {
            engineClient.close();
        }
        if (node != null) {
            node.shutdown();
        }
    }

    /**
     * Runs the fast-source/slow-sink job for a sustained window and verifies the job stays healthy
     * while its checkpoints keep completing on schedule, then supplements that with a
     * realtime-metrics-based proof that the intermediate queue is genuinely bounded and that real
     * backpressure (not silent unbounded buffering) is what is happening.
     *
     * @throws Exception if polling the cluster's REST endpoints fails unexpectedly
     */
    @Test
    public void testCheckpointsKeepCompletingUnderSustainedBackpressure() throws Exception {
        String checkpointBaseUrl =
                HOST
                        + config.getEngineConfig().getHttpConfig().getPort()
                        + config.getEngineConfig().getHttpConfig().getContextPath();
        String metricsBaseUrl = HOST + config.getEngineConfig().getHttpConfig().getPort();
        long jobId = jobProxy.getJobId();

        // Wait for the first checkpoint so the sampling loop below always starts from a
        // well-defined baseline instead of racing the job's own startup.
        Awaitility.await()
                .atMost(2, TimeUnit.MINUTES)
                .pollInterval(2, TimeUnit.SECONDS)
                .untilAsserted(
                        () -> {
                            Assertions.assertEquals(JobStatus.RUNNING, jobProxy.getJobStatus());
                            Map<String, Object> overview =
                                    getCheckpointOverview(jobId, checkpointBaseUrl);
                            List<Map<String, Object>> pipelines =
                                    castList(overview.get("pipelines"));
                            Assertions.assertFalse(pipelines.isEmpty());
                            Map<String, Object> counts = castMap(pipelines.get(0).get("counts"));
                            Assertions.assertTrue(getLong(counts, "completed") >= 1L);
                        });

        List<Long> completedSamples = new ArrayList<>();
        List<Long> queueCapacitySamples = new ArrayList<>();
        List<Long> blockedNsSamples = new ArrayList<>();

        long deadline = System.currentTimeMillis() + BACKPRESSURE_WINDOW_MS;
        while (System.currentTimeMillis() < deadline) {
            // The job must stay healthy throughout - no crash, no failure, no stuck state - while
            // the slow sink keeps the intermediate queue saturated.
            Assertions.assertEquals(
                    JobStatus.RUNNING,
                    jobProxy.getJobStatus(),
                    "job must remain RUNNING throughout the sustained backpressure window");

            Map<String, Object> overview = getCheckpointOverview(jobId, checkpointBaseUrl);
            List<Map<String, Object>> pipelines = castList(overview.get("pipelines"));
            Assertions.assertFalse(pipelines.isEmpty(), "checkpoint overview has no pipelines");
            Map<String, Object> counts = castMap(pipelines.get(0).get("counts"));
            Assertions.assertEquals(
                    0L,
                    getLong(counts, "failed"),
                    "no checkpoint should fail while the slow sink is applying backpressure");
            completedSamples.add(getLong(counts, "completed"));

            Map<String, Object> edgesResp = getEdgesMetrics(jobId, metricsBaseUrl);
            List<Map<String, Object>> edges = castList(edgesResp.get("edges"));
            if (!edges.isEmpty()) {
                List<Map<String, Object>> points = castList(edges.get(0).get("points"));
                if (!points.isEmpty()) {
                    Map<String, Object> latestPoint = castMap(points.get(points.size() - 1));
                    queueCapacitySamples.add(getLong(latestPoint, "queueCapacity"));
                    blockedNsSamples.add(getLong(latestPoint, "emitBlockedNs"));
                }
            }

            Thread.sleep(POLL_INTERVAL_MS);
        }

        // 1. Checkpoints kept completing throughout the window, not just once at the very start:
        // the completed count must never regress, and must grow by a healthy minimum over the
        // whole sustained-backpressure window.
        Assertions.assertTrue(
                completedSamples.size() >= 3,
                "expected multiple checkpoint-overview polls, got " + completedSamples.size());
        for (int i = 1; i < completedSamples.size(); i++) {
            Assertions.assertTrue(
                    completedSamples.get(i) >= completedSamples.get(i - 1),
                    "completed checkpoint count must never regress across polls: "
                            + completedSamples);
        }
        long newlyCompleted =
                completedSamples.get(completedSamples.size() - 1) - completedSamples.get(0);
        Assertions.assertTrue(
                newlyCompleted >= MIN_NEW_COMPLETED_CHECKPOINTS,
                String.format(
                        "expected at least %d additional checkpoints to complete during the %ds "
                                + "sustained backpressure window, only observed %d (samples=%s)",
                        MIN_NEW_COMPLETED_CHECKPOINTS,
                        BACKPRESSURE_WINDOW_MS / 1000,
                        newlyCompleted,
                        completedSamples));

        // 2. The source->sink intermediate queue is a genuinely bounded queue (backed by a
        // fixed-capacity ArrayBlockingQueue in the engine), not an unbounded buffer: its reported
        // capacity is a stable, positive number throughout the run.
        Assertions.assertFalse(
                queueCapacitySamples.isEmpty(),
                "no queue-capacity samples observed from the realtime-metrics edges API; "
                        + "is engine.observability.enabled wired correctly?");
        for (long capacity : queueCapacitySamples) {
            Assertions.assertTrue(
                    capacity > 0,
                    "intermediate queue capacity must be a positive, finite bound: "
                            + queueCapacitySamples);
        }

        // 3. The fast source was actually blocked trying to enqueue into that bounded, full
        // queue - i.e. real backpressure was enforced end-to-end, not silently absorbed into
        // unbounded memory.
        long maxBlockedNs = blockedNsSamples.stream().mapToLong(Long::longValue).max().orElse(0L);
        Assertions.assertTrue(
                maxBlockedNs > 0,
                "expected the fast source to be measurably blocked enqueuing into the bounded "
                        + "intermediate queue while the sink was throttled (emitBlockedNs should "
                        + "be > 0 for at least one sample); got "
                        + blockedNsSamples);

        // 4. Final health check after sustaining backpressure for the whole window.
        Assertions.assertEquals(JobStatus.RUNNING, jobProxy.getJobStatus());
    }

    private Map<String, Object> getCheckpointOverview(long jobId, String baseUrl) {
        return given().get(baseUrl + RestConstant.REST_URL_CHECKPOINT_OVERVIEW + "/" + jobId)
                .then()
                .statusCode(200)
                .extract()
                .as(new TypeRef<Map<String, Object>>() {});
    }

    private Map<String, Object> getEdgesMetrics(long jobId, String baseUrl) {
        return given().get(
                        baseUrl
                                + "/metrics/realtime/jobs/"
                                + jobId
                                + "/edges?windowMs="
                                + REALTIME_METRICS_WINDOW_MS)
                .then()
                .statusCode(200)
                .extract()
                .as(new TypeRef<Map<String, Object>>() {});
    }

    @SuppressWarnings("unchecked")
    private <T> List<T> castList(Object value) {
        if (value == null) {
            return Collections.emptyList();
        }
        return (List<T>) value;
    }

    @SuppressWarnings("unchecked")
    private Map<String, Object> castMap(Object value) {
        if (value == null) {
            return Collections.emptyMap();
        }
        return (Map<String, Object>) value;
    }

    private long getLong(Map<String, Object> source, String key) {
        Object value = source.get(key);
        return value instanceof Number ? ((Number) value).longValue() : 0L;
    }

    private static int getAvailablePort() {
        try (ServerSocket socket = new ServerSocket(0)) {
            return socket.getLocalPort();
        } catch (IOException e) {
            throw new IllegalStateException("No available port for BackpressureSlowSinkIT", e);
        }
    }
}
