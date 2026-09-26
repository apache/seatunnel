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
 * Shared machinery for savepoint-restore ITs that change parallelism across the restore boundary,
 * extracted from the near-identical bodies of {@link SavepointRestoreScaleUpIT} (2 -&gt; 4) and
 * {@link SavepointRestoreScaleDownIT} (4 -&gt; 2) so the REST-polling helpers,
 * offset-reconciliation assertions, and container plugin setup are defined exactly once instead of
 * drifting between two hand-maintained copies.
 *
 * <p>Both directions exercise the modulo-based per-task state remap in {@code
 * CheckpointCoordinator#restoreTaskState} with a genuinely different old/new parallelism pair -
 * every other checkpoint/savepoint restore IT in this package (e.g. {@link
 * CheckpointRestoreWithStopIT}, {@link SavepointRestoreIT}) restores at the SAME parallelism the
 * checkpoint was taken at, so that remap's {@code currentParallelism !=
 * actionState.getParallelism()} branch is never exercised end-to-end anywhere else.
 *
 * <p>Rigor mirrors {@link SavepointRestoreIT}: exact offset reconciliation (no loss, no
 * duplication) across the savepoint boundary. In addition, this verifies the RESTORED job's actual
 * physical task count via the {@code /trace/task-mapping} REST endpoint (backed by the live {@code
 * JobMaster#getPhysicalPlan()}, the same white-box source of truth used elsewhere in this test
 * family), rather than trusting that the configured parallelism was applied.
 *
 * <p>Subclasses supply the conf file pair, the original/restored parallelism, and a distinct sink
 * output directory via the abstract hook methods below, and invoke {@link
 * #verifySavepointRestoreWithRescale()} from their own {@code @Test} method so each retains its own
 * JUnit test name.
 */
public abstract class AbstractSavepointRescaleIT extends SeaTunnelEngineContainer {

    private static final String HOST = "http://localhost:";

    /**
     * Relative test-resource path to the conf submitted at {@link #originalParallelism()}, before
     * the savepoint is taken.
     */
    protected abstract String originalConfFile();

    /**
     * Relative test-resource path to the conf used to restore the job at {@link
     * #restoredParallelism()}, after the savepoint is taken.
     */
    protected abstract String restoreConfFile();

    /**
     * Host-mounted sink output directory for this test, scoped uniquely per subclass so the two
     * rescale directions never read or clean up each other's output files.
     */
    protected abstract String sinkOutputDir();

    /**
     * Parallelism the original run is submitted and savepointed at. Must match {@code
     * env.parallelism} in {@link #originalConfFile()}.
     */
    protected abstract int originalParallelism();

    /**
     * Parallelism the restore run is submitted at. Must match {@code env.parallelism} in {@link
     * #restoreConfFile()}.
     */
    protected abstract int restoredParallelism();

    /**
     * Expected signed delta in physical task count between the original and restored runs.
     *
     * <p>The source and the sink are the only two parallelism-scaled actions in this pipeline (no
     * transform stage), so every regular (non-coordinator) physical vertex list changes by exactly
     * one task per action per unit of parallelism change - hence the factor of 2. The sign follows
     * {@link #restoredParallelism()} minus {@link #originalParallelism()}, so this is positive for
     * scale-up and negative for scale-down. Coordinator-type vertices (the source split enumerator,
     * the sink aggregated committer) are singletons that never scale with parallelism, so they
     * cancel out of this delta regardless of their exact count.
     */
    protected long expectedTaskCountDelta() {
        return 2L * (restoredParallelism() - originalParallelism());
    }

    @Override
    @BeforeAll
    public void startUp() throws Exception {
        super.startUp();
        copyRescaleTestPluginsToContainer();
    }

    /**
     * Runs the full savepoint-then-rescale-restore verification flow shared by {@link
     * SavepointRestoreScaleUpIT} and {@link SavepointRestoreScaleDownIT}: submits a job at {@link
     * #originalParallelism()}, waits for a completed checkpoint, takes a savepoint, restores the
     * job at {@link #restoredParallelism()}, then asserts that (a) the restored job's live physical
     * task count reflects the new parallelism, (b) execution resumes strictly after the savepoint
     * boundary with no gap, and (c) no offset is observed more than once end-to-end (exactly-once).
     * Concrete subclasses call this from their own {@code @Test} method so each keeps its own
     * distinct JUnit test name.
     */
    protected void verifySavepointRestoreWithRescale()
            throws IOException, InterruptedException, java.util.concurrent.ExecutionException {
        FileUtils.createNewDir(sinkOutputDir());
        try {
            long jobId = JobIdGenerator.newJobId();
            CompletableFuture<Container.ExecResult> sourceJobFuture =
                    CompletableFuture.supplyAsync(
                            () -> {
                                try {
                                    return executeJob(originalConfFile(), String.valueOf(jobId));
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
                                    return restoreJob(restoreConfFile(), String.valueOf(jobId));
                                } catch (Exception e) {
                                    throw new RuntimeException(e);
                                }
                            });

            awaitJobStatus(jobId, "RUNNING");
            long expectedTaskCountAfterRestore = taskCountBeforeRestore + expectedTaskCountDelta();
            awaitTaskItemCount(
                    jobId,
                    count -> count == expectedTaskCountAfterRestore,
                    "Expected restored job's physical task count ("
                            + taskCountBeforeRestore
                            + " -> "
                            + expectedTaskCountAfterRestore
                            + ") to reflect the new parallelism ("
                            + originalParallelism()
                            + " -> "
                            + restoredParallelism()
                            + ")");
            assertRestoreContinuesAfterBoundary(offsetsBeforeRestore, maxOffsetBeforeRestore);
            assertNoOffsetDuplicates();

            stopJob(String.valueOf(jobId));
            awaitJobStatus(jobId, "CANCELED");
            Container.ExecResult restoreResult = restoreFuture.get();
            Assertions.assertEquals(0, restoreResult.getExitCode(), restoreResult.getStderr());
        } finally {
            FileUtils.deleteFile(sinkOutputDir());
        }
    }

    /**
     * Polls the job's REST-reported status until it equals {@code expectedStatus}, failing the test
     * if the status has not converged within the timeout.
     */
    private void awaitJobStatus(long jobId, String expectedStatus) {
        Awaitility.await()
                .atMost(2, TimeUnit.MINUTES)
                .untilAsserted(
                        () ->
                                Assertions.assertEquals(
                                        expectedStatus, getJobStatus(String.valueOf(jobId))));
    }

    /**
     * Blocks until the job has completed at least one checkpoint, so callers can rely on a
     * fully-acknowledged baseline (e.g. before reading the pre-restore physical task count).
     */
    private void awaitCompletedCheckpoint(long jobId) {
        Awaitility.await()
                .atMost(2, TimeUnit.MINUTES)
                .untilAsserted(() -> Assertions.assertTrue(getCompletedCheckpointCount(jobId) > 0));
    }

    /**
     * Copies the test-only {@code CheckpointableSequenceSource} plugin jar and its plugin-mapping
     * descriptor into the container so the rescale conf files can resolve it, reusing the same
     * checkpoint-restore-with-stop mapping file that {@link SavepointRestoreIT} already relies on
     * instead of duplicating it per test directory.
     */
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

    /**
     * Waits until offsets are observed past {@code maxOffsetBeforeRestore}, then asserts the
     * post-restore offset count equals the pre-restore count plus exactly the newly-observed
     * offsets past that boundary. This only confirms the restored run continues without a gap in
     * count; it does not by itself rule out replayed (duplicate) offsets - {@link
     * #assertNoOffsetDuplicates()} is the complementary check that directly enforces exactly-once.
     */
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

    /**
     * Asserts that every offset written to the sink output directory (across the pre- and
     * post-restore runs combined) was observed exactly once. A non-empty duplicate set indicates
     * the restored run replayed data the original run had already committed, i.e. an exactly-once
     * violation of the rescale-restore path.
     */
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

    /**
     * Reads every line written under {@link #sinkOutputDir()} across all sink output files and
     * parses each as a {@code long} offset. Returns an empty list if the output directory does not
     * exist yet (e.g. before the job has flushed its first record).
     */
    private List<Long> readObservedOffsets() {
        Path outputDir = Paths.get(sinkOutputDir());
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

    /**
     * Reads a single named counter (e.g. {@code "completed"}) from the first pipeline's {@code
     * counts} object in the {@code /jobs/checkpoints} REST overview for {@code jobId}, returning 0
     * if the job, its pipelines, or the requested counter are not yet present in the response.
     */
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
                                            + RestConstant.CONTEXT_PATH
                                            + RestConstant.REST_URL_TRACE_TASK_MAPPING
                                            + "/"
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
