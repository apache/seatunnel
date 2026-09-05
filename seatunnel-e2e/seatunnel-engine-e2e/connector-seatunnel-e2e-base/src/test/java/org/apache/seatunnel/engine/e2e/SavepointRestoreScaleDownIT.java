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

import org.apache.seatunnel.common.utils.FileUtils;
import org.apache.seatunnel.e2e.common.util.JobIdGenerator;
import org.apache.seatunnel.engine.server.rest.RestConstant;

import org.awaitility.Awaitility;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.Container;
import org.testcontainers.utility.MountableFile;

import io.restassured.common.mapper.TypeRef;

import java.io.IOException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
import java.util.function.LongPredicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static io.restassured.RestAssured.given;
import static org.apache.seatunnel.e2e.common.util.ContainerUtil.PROJECT_ROOT_PATH;

/**
 * Verifies that a savepoint taken at one parallelism can be restored at a LOWER parallelism
 * (scale-down), exercising the modulo-based per-task state remap in {@code
 * CheckpointCoordinator#restoreTaskState} with a genuinely different old/new parallelism pair.
 *
 * <p>Companion of {@link SavepointRestoreScaleUpIT}: that test covers scale-up (2 -> 4), this one
 * covers scale-down (4 -> 2). The remap formula is asymmetric between the two directions - scaling
 * down merges multiple old subtask indices' state onto every new instance (every new reader
 * inherits {@code oldParallelism / newParallelism} old readers' state when evenly divisible),
 * whereas scaling up leaves new instances whose index is {@code >=} the old parallelism with no
 * inherited subtask state at all. Both directions must independently be proven safe (no data loss,
 * no duplication), which is why this is a separate test rather than a parameterized variant.
 *
 * <p>Rigor mirrors {@link SavepointRestoreIT}: exact offset reconciliation (no loss, no
 * duplication) across the savepoint boundary. In addition, this test verifies the RESTORED job's
 * actual physical task count via the {@code /trace/task-mapping} REST endpoint (backed by the live
 * {@code JobMaster#getPhysicalPlan()}, the same white-box source of truth used elsewhere in this
 * test family), rather than trusting that the configured parallelism was applied.
 */
public class SavepointRestoreScaleDownIT extends SeaTunnelEngineContainer {

    private static final String HOST = "http://localhost:";
    private static final String ORIGINAL_CONF_FILE =
            "/savepoint-restore-rescale/stream_p4_to_localfile_scaledown.conf";
    private static final String RESTORE_CONF_FILE =
            "/savepoint-restore-rescale/stream_p2_to_localfile_scaledown.conf";
    private static final String SINK_OUTPUT_DIR =
            HOST_VOLUME_MOUNT_PATH + "/savepoint-restore-rescale/scale-down/sinkfile";

    // Must match env.parallelism in stream_p4_to_localfile_scaledown.conf.
    private static final int ORIGINAL_PARALLELISM = 4;
    // Must match env.parallelism in stream_p2_to_localfile_scaledown.conf.
    private static final int RESTORED_PARALLELISM = 2;
    // The source and the sink are the only two parallelism-scaled actions in this pipeline (no
    // transform stage), so every regular (non-coordinator) physical vertex list shrinks by
    // exactly one task per action per unit of parallelism decrease. Coordinator-type vertices (the
    // source split enumerator, the sink aggregated committer) are singletons that never scale with
    // parallelism, so they cancel out of this delta regardless of their exact count.
    private static final long EXPECTED_TASK_COUNT_DELTA =
            2L * (RESTORED_PARALLELISM - ORIGINAL_PARALLELISM);

    @Override
    @BeforeAll
    public void startUp() throws Exception {
        super.startUp();
        copyRescaleTestPluginsToContainer();
    }

    @Test
    public void testRestoreFromSavepointWithLowerParallelism()
            throws IOException, InterruptedException, java.util.concurrent.ExecutionException {
        FileUtils.createNewDir(SINK_OUTPUT_DIR);
        try {
            long jobId = JobIdGenerator.newJobId();
            CompletableFuture<Container.ExecResult> sourceJobFuture =
                    CompletableFuture.supplyAsync(
                            () -> {
                                try {
                                    return executeJob(ORIGINAL_CONF_FILE, String.valueOf(jobId));
                                } catch (Exception e) {
                                    throw new RuntimeException(e);
                                }
                            });

            awaitJobStatus(jobId, "RUNNING");
            awaitCompletedCheckpoint(jobId);
            // A completed checkpoint requires every subtask of every parallel instance to have
            // acknowledged the barrier, so it is safe to treat the task count observed here as
            // the fully-deployed baseline for the original parallelism.
            long taskCountBeforeRestore =
                    awaitTaskItemCount(
                            jobId,
                            count -> count > 0,
                            "Expected at least one deployed task before restore");

            Container.ExecResult savepointResult = savepointJob(String.valueOf(jobId));
            Assertions.assertEquals(0, savepointResult.getExitCode(), savepointResult.getStderr());
            awaitJobStatus(jobId, "SAVEPOINT_DONE");

            List<Long> offsetsBeforeRestore = readObservedOffsets();
            long maxOffsetBeforeRestore = getMaxOffset(offsetsBeforeRestore);
            Assertions.assertFalse(
                    offsetsBeforeRestore.isEmpty(), "Expected committed offsets before restore");
            Assertions.assertEquals(0, sourceJobFuture.get().getExitCode());

            CompletableFuture<Container.ExecResult> restoreFuture =
                    CompletableFuture.supplyAsync(
                            () -> {
                                try {
                                    return restoreJob(RESTORE_CONF_FILE, String.valueOf(jobId));
                                } catch (Exception e) {
                                    throw new RuntimeException(e);
                                }
                            });

            awaitJobStatus(jobId, "RUNNING");
            long expectedTaskCountAfterRestore = taskCountBeforeRestore + EXPECTED_TASK_COUNT_DELTA;
            awaitTaskItemCount(
                    jobId,
                    count -> count == expectedTaskCountAfterRestore,
                    "Expected restored job's physical task count ("
                            + taskCountBeforeRestore
                            + " -> "
                            + expectedTaskCountAfterRestore
                            + ") to reflect the new parallelism ("
                            + ORIGINAL_PARALLELISM
                            + " -> "
                            + RESTORED_PARALLELISM
                            + ")");
            assertRestoreContinuesAfterBoundary(offsetsBeforeRestore, maxOffsetBeforeRestore);
            assertNoOffsetDuplicates();

            stopJob(String.valueOf(jobId));
            awaitJobStatus(jobId, "CANCELED");
            Container.ExecResult restoreResult = restoreFuture.get();
            Assertions.assertEquals(0, restoreResult.getExitCode(), restoreResult.getStderr());
        } finally {
            FileUtils.deleteFile(SINK_OUTPUT_DIR);
        }
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

    private void copyRescaleTestPluginsToContainer() throws IOException {
        URL url =
                FileUtils.searchJarFiles(
                                Paths.get(
                                        PROJECT_ROOT_PATH,
                                        "seatunnel-e2e",
                                        "seatunnel-e2e-common",
                                        "target"))
                        .stream()
                        .filter(jar -> jar.toString().endsWith("-tests.jar"))
                        .findFirst()
                        .orElseThrow(
                                () ->
                                        new IllegalStateException(
                                                "Could not locate seatunnel-e2e-common test jar"));
        server.copyFileToContainer(
                MountableFile.forHostPath(Paths.get(url.getFile())),
                Paths.get(
                                SEATUNNEL_HOME,
                                "connectors",
                                Paths.get(url.getFile()).getFileName().toString())
                        .toString());
        // Reuse the checkpoint-restore-with-stop plugin mapping: it resolves the same test-only
        // CheckpointableSequenceSource used here, and SavepointRestoreIT already establishes the
        // precedent of sharing this file rather than duplicating it per directory.
        server.copyFileToContainer(
                MountableFile.forHostPath(
                        Paths.get(
                                PROJECT_ROOT_PATH,
                                "seatunnel-e2e",
                                "seatunnel-engine-e2e",
                                "connector-seatunnel-e2e-base",
                                "src",
                                "test",
                                "resources",
                                "checkpoint-restore-with-stop",
                                "plugin-mapping.properties")),
                Paths.get(SEATUNNEL_HOME, "connectors", "plugin-mapping.properties").toString());
    }

    private void assertRestoreContinuesAfterBoundary(
            List<Long> offsetsBeforeRestore, long maxOffsetBeforeRestore) {
        Awaitility.await()
                .atMost(2, TimeUnit.MINUTES)
                .untilAsserted(
                        () -> {
                            List<Long> offsetsAfterRestore = readObservedOffsets();
                            List<Long> restoredOffsets = new ArrayList<>();
                            for (Long offset : offsetsAfterRestore) {
                                if (offset > maxOffsetBeforeRestore) {
                                    restoredOffsets.add(offset);
                                }
                            }

                            Assertions.assertTrue(
                                    !restoredOffsets.isEmpty(),
                                    "Expected restored run to continue from savepoint boundary "
                                            + maxOffsetBeforeRestore);

                            Assertions.assertEquals(
                                    offsetsBeforeRestore.size() + restoredOffsets.size(),
                                    offsetsAfterRestore.size(),
                                    "Expected restore to append only new offsets after savepoint boundary "
                                            + maxOffsetBeforeRestore);
                        });
    }

    private void assertNoOffsetDuplicates() {
        Map<Long, Long> counts =
                readObservedOffsets().stream()
                        .collect(Collectors.groupingBy(Function.identity(), Collectors.counting()));
        List<Long> duplicates =
                counts.entrySet().stream()
                        .filter(entry -> entry.getValue() > 1)
                        .map(Map.Entry::getKey)
                        .collect(Collectors.toList());
        Assertions.assertTrue(
                duplicates.isEmpty(),
                "Found duplicate offsets (exactly-once violated): " + duplicates);
    }

    private long getMaxOffset(List<Long> offsets) {
        return offsets.stream().mapToLong(Long::longValue).max().orElse(-1L);
    }

    private List<Long> readObservedOffsets() {
        Path outputDir = Paths.get(SINK_OUTPUT_DIR);
        if (!Files.exists(outputDir)) {
            return Collections.emptyList();
        }
        try (Stream<Path> paths = Files.walk(outputDir)) {
            return paths.filter(Files::isRegularFile)
                    .flatMap(
                            path -> {
                                try {
                                    return Files.readAllLines(path).stream();
                                } catch (IOException e) {
                                    throw new RuntimeException(e);
                                }
                            })
                    .map(String::trim)
                    .filter(line -> !line.isEmpty())
                    .map(Long::parseLong)
                    .collect(Collectors.toList());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
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

    /**
     * Polls the live physical task mapping for {@code jobId} until {@code condition} holds,
     * returning the last observed count.
     *
     * <p>Awaitility's {@code untilAsserted} only retries on {@link AssertionError}, so the actual
     * REST call is isolated in {@link #getTaskItemCount(long)} which translates any transport
     * failure into an assertion failure instead of letting a checked/unchecked transport exception
     * escape and abort the poll on the first attempt.
     */
    private long awaitTaskItemCount(long jobId, LongPredicate condition, String description) {
        AtomicLong result = new AtomicLong();
        Awaitility.await()
                .atMost(2, TimeUnit.MINUTES)
                .untilAsserted(
                        () -> {
                            long count = getTaskItemCount(jobId);
                            Assertions.assertTrue(
                                    condition.test(count),
                                    description + " (actual physical task count=" + count + ")");
                            result.set(count);
                        });
        return result.get();
    }

    /**
     * Reads the live {@code /trace/task-mapping} REST endpoint (backed by {@code
     * JobMaster#getPhysicalPlan()} on the active master) and counts how many physical tasks are
     * currently deployed for {@code jobId}, across every pipeline, regular and coordinator vertex
     * alike. This is a white-box count of the actually-running plan, not the submitted config.
     *
     * <p>Uses RestAssured's {@code JsonPath} extraction (rather than {@code .as(TypeRef)}) because
     * this endpoint is served by the Hazelcast member's own text-command processor on port 5801 - a
     * different REST stack than the port 8080 job-info/checkpoint-overview endpoints used elsewhere
     * in this class - and {@code JsonPath} parses the body as JSON directly instead of relying on a
     * possibly-absent/unrecognized Content-Type header to select a deserializer.
     */
    private long getTaskItemCount(long jobId) {
        try {
            List<?> items =
                    given().get(
                                    "http://localhost:"
                                            + server.getMappedPort(5801)
                                            + "/hazelcast/rest/maps/trace/task-mapping/"
                                            + jobId)
                            .then()
                            .statusCode(200)
                            .extract()
                            .response()
                            .jsonPath()
                            .getList("items");
            return items == null ? 0L : items.size();
        } catch (RuntimeException e) {
            // Awaitility's untilAsserted only retries on AssertionError, so translate a
            // transient REST-call failure (e.g. endpoint not ready yet) into a JUnit assertion
            // failure instead of letting it escape unretried and abort the poll immediately.
            throw new AssertionError("Failed to fetch task mapping for job " + jobId, e);
        }
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
