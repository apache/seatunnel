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

import org.apache.seatunnel.e2e.common.util.JobIdGenerator;
import org.apache.seatunnel.engine.server.rest.RestConstant;

import org.awaitility.Awaitility;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.Container;

import io.restassured.common.mapper.TypeRef;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

import static io.restassured.RestAssured.given;

public class CheckpointRestoreWithStopIT extends SeaTunnelEngineContainer {

    private static final String HOST = "http://localhost:";
    private static final String CONF_FILE =
            "/checkpoint-restore-with-stop/stream_fakesource_to_localfile_resume_with_checkpoint.conf";

    @Test
    public void testRestoreFromCheckpointAfterStop()
            throws IOException, InterruptedException, java.util.concurrent.ExecutionException {
        long sourceJobId = JobIdGenerator.newJobId();
        CompletableFuture<Container.ExecResult> sourceJobFuture =
                CompletableFuture.supplyAsync(
                        () -> {
                            try {
                                return executeJob(CONF_FILE, String.valueOf(sourceJobId));
                            } catch (Exception e) {
                                throw new RuntimeException(e);
                            }
                        });

        awaitJobStatus(sourceJobId, "RUNNING");
        awaitCompletedCheckpoint(sourceJobId);

        stopJob(String.valueOf(sourceJobId));
        awaitJobStatus(sourceJobId, "CANCELED");
        Assertions.assertEquals(0, sourceJobFuture.get().getExitCode());
        Assertions.assertTrue(
                getRunningJobIds().isEmpty(), "Expected no running jobs before restore");

        CompletableFuture<Container.ExecResult> restoreFuture =
                CompletableFuture.supplyAsync(
                        () -> {
                            try {
                                return restoreJobWithCheckpoint(
                                        CONF_FILE, String.valueOf(sourceJobId));
                            } catch (Exception e) {
                                throw new RuntimeException(e);
                            }
                        });

        AtomicLong restoredJobId = new AtomicLong(-1L);
        Awaitility.await()
                .atMost(2, TimeUnit.MINUTES)
                .untilAsserted(
                        () -> {
                            List<Long> runningJobIds = getRunningJobIds();
                            Assertions.assertFalse(runningJobIds.isEmpty());
                            restoredJobId.set(
                                    runningJobIds.stream()
                                            .filter(jobId -> jobId != sourceJobId)
                                            .findFirst()
                                            .orElseThrow(IllegalStateException::new));
                        });

        Assertions.assertNotEquals(sourceJobId, restoredJobId.get());
        awaitJobStatus(restoredJobId.get(), "RUNNING");

        stopJob(String.valueOf(restoredJobId.get()));
        awaitJobStatus(restoredJobId.get(), "CANCELED");
        Container.ExecResult restoreResult = restoreFuture.get();
        Assertions.assertEquals(0, restoreResult.getExitCode(), restoreResult.getStderr());
        Assertions.assertTrue(
                restoreResult.getStderr().contains("Start with CHECKPOINT"),
                restoreResult.getStderr());
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
                .untilAsserted(() -> Assertions.assertTrue(getCompletedCheckpointCount(jobId) > 0));
    }

    private long getCompletedCheckpointCount(long jobId) {
        return getPipelineCounter(jobId, "completed");
    }

    private long getPipelineCounter(long jobId, String counterKey) {
        Map<String, Object> overview =
                given().get(
                                getRestBaseUrl()
                                        + RestConstant.REST_URL_CHECKPOINT_OVERVIEW
                                        + "/"
                                        + jobId)
                        .then()
                        .statusCode(200)
                        .extract()
                        .as(new TypeRef<Map<String, Object>>() {});
        List<Map<String, Object>> pipelines = castList(overview.get("pipelines"));
        if (pipelines == null || pipelines.isEmpty()) {
            return 0L;
        }
        Map<String, Object> counts = castMap(pipelines.get(0).get("counts"));
        if (counts == null) {
            return 0L;
        }
        Object counter = counts.get(counterKey);
        return counter instanceof Number ? ((Number) counter).longValue() : 0L;
    }

    private List<Long> getRunningJobIds() {
        List<Map<String, Object>> jobs =
                given().get(getRestBaseUrl() + RestConstant.REST_URL_RUNNING_JOBS)
                        .then()
                        .statusCode(200)
                        .extract()
                        .as(new TypeRef<List<Map<String, Object>>>() {});
        return jobs.stream()
                .map(job -> castMap(job.get("jobDag")).get("jobId"))
                .map(String::valueOf)
                .map(Long::parseLong)
                .collect(Collectors.toList());
    }

    @SuppressWarnings("unchecked")
    private List<Map<String, Object>> castList(Object value) {
        return (List<Map<String, Object>>) value;
    }

    @SuppressWarnings("unchecked")
    private Map<String, Object> castMap(Object value) {
        return (Map<String, Object>) value;
    }

    private String getRestBaseUrl() {
        return HOST + server.getMappedPort(8080);
    }
}
