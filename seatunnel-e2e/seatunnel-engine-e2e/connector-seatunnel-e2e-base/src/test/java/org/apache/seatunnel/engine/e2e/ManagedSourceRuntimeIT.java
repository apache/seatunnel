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

import org.apache.seatunnel.common.utils.JsonUtils;
import org.apache.seatunnel.e2e.common.util.JobIdGenerator;

import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;

import org.awaitility.Awaitility;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.Container;

import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static org.apache.seatunnel.api.common.metrics.MetricNames.SOURCE_RECEIVED_COUNT;
import static org.apache.seatunnel.e2e.common.util.ContainerUtil.PROJECT_ROOT_PATH;

/**
 * End-to-end coverage for the engine-managed Source runtime introduced by STIP-31.
 *
 * <p>The cluster boots with {@code managed-source-runtime} enabled, and FakeSource explicitly
 * declares the managed capability. These tests therefore run a real job through the managed Reader
 * and coordinator event loops while unsupported connectors remain on the legacy lane.
 *
 * <p>Each test asserts the lane that was actually selected. Without that assertion a silent
 * fallback to the legacy lane would still pass, which would make this suite worse than no coverage
 * at all.
 */
@Slf4j
public class ManagedSourceRuntimeIT extends SeaTunnelEngineContainer {

    private static final String BATCH_CONF =
            "/managed-source-runtime/batch_fake_to_inmemory_managed.conf";
    private static final String STREAM_CONF =
            "/managed-source-runtime/stream_fake_to_inmemory_managed.conf";

    /** Logged once per Source by PhysicalPlanGenerator when a non-legacy lane is selected. */
    private static final String MANAGED_LANE_LOG =
            "selected the managed Source runtime lane MANAGED_READER_AND_COORDINATOR";

    @Override
    @BeforeAll
    public void startUp() throws Exception {
        this.server =
                createSeaTunnelContainerWithFakeSourceAndInMemorySink(
                        PROJECT_ROOT_PATH
                                + "/seatunnel-e2e/seatunnel-engine-e2e/connector-seatunnel-e2e-base/src/test/resources/seatunnel_managed_source_runtime.yaml");
    }

    /**
     * Runs a bounded job whose Source is on the managed lane, covering split assignment,
     * no-more-splits, reader completion and the barrier path that carries checkpoint state.
     */
    @Test
    public void testBatchJobCompletesOnManagedLane() throws Exception {
        Container.ExecResult execResult = executeJob(BATCH_CONF);
        Assertions.assertEquals(0, execResult.getExitCode(), execResult.getStderr());
        Assertions.assertTrue(
                server.getLogs().contains(MANAGED_LANE_LOG),
                "FakeSource declares the managed capability, so it must run on the managed lane");
    }

    /**
     * Savepoints and restores a managed-lane job.
     *
     * <p>This is the highest risk path in the feature: it exercises managed Reader checkpoint state
     * serialization, the persisted lane selection, capability digest validation on restore, and the
     * coordinator ownership proof replay. A restore that silently changed lane, or that rejected
     * its own state, would never reach RUNNING again.
     */
    @Test
    public void testManagedLaneJobRestoresFromSavepoint() throws Exception {
        long jobId = JobIdGenerator.newJobId();
        CompletableFuture<Container.ExecResult> jobFuture =
                CompletableFuture.supplyAsync(
                        () -> {
                            try {
                                return executeJob(STREAM_CONF, String.valueOf(jobId));
                            } catch (Exception e) {
                                throw new RuntimeException(e);
                            }
                        });

        awaitJobStatus(jobId, "RUNNING");
        Assertions.assertTrue(
                server.getLogs().contains(MANAGED_LANE_LOG),
                "Streaming FakeSource must also enter the managed lane before the savepoint");
        awaitCompletedCheckpoint(jobId);
        Awaitility.await()
                .atMost(2, TimeUnit.MINUTES)
                .untilAsserted(() -> Assertions.assertTrue(receivedRows(jobId) > 0L));

        Container.ExecResult savepointResult = savepointJob(String.valueOf(jobId));
        Assertions.assertEquals(0, savepointResult.getExitCode(), savepointResult.getStderr());
        awaitJobStatus(jobId, "SAVEPOINT_DONE");
        Assertions.assertEquals(0, jobFuture.get().getExitCode());

        CompletableFuture<Container.ExecResult> restoreFuture =
                CompletableFuture.supplyAsync(
                        () -> {
                            try {
                                return restoreJob(STREAM_CONF, String.valueOf(jobId));
                            } catch (Exception e) {
                                throw new RuntimeException(e);
                            }
                        });

        awaitJobStatus(jobId, "RUNNING");
        // A reader restored as DRAINING can report RUNNING without ever polling again. Require
        // fresh source output and a later checkpoint from the restored execution attempt.
        long restoredRows = receivedRows(jobId);
        long restoredCheckpoints = getCompletedCheckpointCount(String.valueOf(jobId));
        Awaitility.await()
                .atMost(2, TimeUnit.MINUTES)
                .untilAsserted(
                        () -> {
                            Assertions.assertTrue(receivedRows(jobId) > Math.max(0L, restoredRows));
                            Assertions.assertTrue(
                                    getCompletedCheckpointCount(String.valueOf(jobId))
                                            > restoredCheckpoints);
                        });
        // Restoring managed state into the legacy lane, or vice versa, fails closed with these
        // messages rather than silently changing execution semantics.
        Assertions.assertFalse(
                server.getLogs().contains("Cannot restore a managed Source Reader from legacy"),
                "Managed Reader restore must accept its own checkpoint state");
        Assertions.assertFalse(
                server.getLogs().contains("Cannot silently restore managed Source"),
                "Restore must not fall back to the legacy lane");

        stopJob(String.valueOf(jobId));
        awaitJobStatus(jobId, "CANCELED");
        Container.ExecResult restoreResult = restoreFuture.get();
        Assertions.assertEquals(0, restoreResult.getExitCode(), restoreResult.getStderr());
    }

    private void awaitJobStatus(long jobId, String expectedStatus) {
        Awaitility.await()
                .atMost(2, TimeUnit.MINUTES)
                .untilAsserted(
                        () ->
                                Assertions.assertEquals(
                                        expectedStatus, getJobStatus(String.valueOf(jobId))));
    }

    private void awaitCompletedCheckpoint(long jobId) {
        Awaitility.await()
                .atMost(2, TimeUnit.MINUTES)
                .untilAsserted(
                        () ->
                                Assertions.assertTrue(
                                        getCompletedCheckpointCount(String.valueOf(jobId)) > 0));
    }

    /**
     * Reads the engine's source counter with bounded HTTP calls, rather than inferring progress
     * from job status or the presence of checkpoint metadata.
     */
    private long receivedRows(long jobId) throws Exception {
        HttpGet request =
                new HttpGet(
                        String.format(
                                "http://%s:%d/job-info/%s",
                                server.getHost(), server.getMappedPort(8080), jobId));
        request.setConfig(
                RequestConfig.custom()
                        .setConnectTimeout(5000)
                        .setSocketTimeout(5000)
                        .setConnectionRequestTimeout(5000)
                        .build());
        try (CloseableHttpClient client = HttpClients.createDefault();
                CloseableHttpResponse response = client.execute(request)) {
            Assertions.assertEquals(200, response.getStatusLine().getStatusCode());
            return JsonUtils.stringToJsonNode(EntityUtils.toString(response.getEntity()))
                    .path("metrics")
                    .path(SOURCE_RECEIVED_COUNT)
                    .asLong(-1L);
        }
    }
}
