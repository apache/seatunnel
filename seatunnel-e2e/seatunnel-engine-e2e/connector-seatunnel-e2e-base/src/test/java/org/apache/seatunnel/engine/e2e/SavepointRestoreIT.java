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
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static io.restassured.RestAssured.given;
import static org.apache.seatunnel.e2e.common.util.ContainerUtil.PROJECT_ROOT_PATH;

@Slf4j
public class SavepointRestoreIT extends SeaTunnelEngineContainer {

    private static final String HOST = "http://localhost:";
    private static final String CONF_FILE =
            "/savepoint-restore/stream_checkpointable_sequence_to_localfile.conf";
    private static final String CONF_FILE_PARALLELISM5 =
            "/savepoint-restore/stream_checkpointable_sequence_to_localfile_parallelism5.conf";
    private static final String SINK_OUTPUT_DIR =
            HOST_VOLUME_MOUNT_PATH + "/savepoint-restore/sinkfile";
    private static final String SINK_OUTPUT_DIR_PARALLELISM5 =
            HOST_VOLUME_MOUNT_PATH + "/savepoint-restore-queue-metrics/sinkfile";

    @Override
    @BeforeAll
    public void startUp() throws Exception {
        super.startUp();
        copySavepointRestoreTestPluginsToContainer();
    }

    @Test
    public void testRestoreFromSavepoint()
            throws IOException, InterruptedException, java.util.concurrent.ExecutionException {
        FileUtils.createNewDir(SINK_OUTPUT_DIR);
        try {
            long jobId = JobIdGenerator.newJobId();
            CompletableFuture<Container.ExecResult> sourceJobFuture =
                    CompletableFuture.supplyAsync(
                            () -> {
                                try {
                                    return executeJob(CONF_FILE, String.valueOf(jobId));
                                } catch (Exception e) {
                                    throw new RuntimeException(e);
                                }
                            });

            awaitJobStatus(jobId, "RUNNING");
            awaitCompletedCheckpoint(jobId);

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
                                    return restoreJob(CONF_FILE, String.valueOf(jobId));
                                } catch (Exception e) {
                                    throw new RuntimeException(e);
                                }
                            });

            awaitJobStatus(jobId, "RUNNING");
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

    @Test
    public void testQueueMetricsStableAcrossRepeatedSavepointRestores()
            throws IOException, InterruptedException, ExecutionException, TimeoutException {
        FileUtils.createNewDir(SINK_OUTPUT_DIR_PARALLELISM5);
        long jobId = JobIdGenerator.newJobId();
        CompletableFuture<Container.ExecResult> activeCliProcessFuture = null;
        try {
            activeCliProcessFuture =
                    CompletableFuture.supplyAsync(
                            () -> {
                                try {
                                    return executeJob(
                                            CONF_FILE_PARALLELISM5, String.valueOf(jobId));
                                } catch (Exception e) {
                                    throw new RuntimeException(e);
                                }
                            });

            awaitJobStatus(jobId, "RUNNING");
            awaitCompletedCheckpoint(jobId);

            Map<Long, Long> baselineSubtaskCountByQueueId = new HashMap<>();
            Map<Long, Long> baselineCapacityByQueueId = new HashMap<>();

            AtomicInteger consecutiveMatches = new AtomicInteger(0);
            Map<Long, Long> candidateSubtaskCount = new HashMap<>();
            Map<Long, Long> candidateCapacity = new HashMap<>();
            AtomicLong lastObservedBucketTs = new AtomicLong(0L);

            Awaitility.await()
                    .atMost(2, TimeUnit.MINUTES)
                    .pollInterval(1200, TimeUnit.MILLISECONDS)
                    .untilAsserted(
                            () -> {
                                Map<Long, Map<String, Object>> latestPoints =
                                        getLatestEdgePoints(jobId);
                                Assertions.assertEquals(
                                        1,
                                        latestPoints.size(),
                                        "Expected exactly one intermediate queue, found "
                                                + latestPoints.keySet());
                                Map<Long, Long> currentSubtask = new HashMap<>();
                                Map<Long, Long> currentCapacity = new HashMap<>();
                                long maxTs = 0L;
                                for (Map.Entry<Long, Map<String, Object>> entry :
                                        latestPoints.entrySet()) {
                                    long queueId = entry.getKey();
                                    Map<String, Object> point = entry.getValue();
                                    Assertions.assertNotNull(
                                            point,
                                            "Edge metric point must not be null for queue "
                                                    + queueId);
                                    long subtaskCount = toLong(point.get("subtaskCount"));
                                    long queueCapacity = toLong(point.get("queueCapacity"));
                                    long pointTs = toLong(point.get("ts"));

                                    Assertions.assertEquals(
                                            5L,
                                            subtaskCount,
                                            "Expected exactly 5 subtask owners for queue "
                                                    + queueId);
                                    Assertions.assertEquals(
                                            10240L,
                                            queueCapacity,
                                            "Expected aggregate capacity 10240 (5 * 2048) for queue "
                                                    + queueId);
                                    currentSubtask.put(queueId, subtaskCount);
                                    currentCapacity.put(queueId, queueCapacity);
                                    maxTs = Math.max(maxTs, pointTs);
                                }

                                if (currentSubtask.equals(candidateSubtaskCount)
                                        && currentCapacity.equals(candidateCapacity)) {
                                    consecutiveMatches.incrementAndGet();
                                } else {
                                    candidateSubtaskCount.clear();
                                    candidateSubtaskCount.putAll(currentSubtask);
                                    candidateCapacity.clear();
                                    candidateCapacity.putAll(currentCapacity);
                                    consecutiveMatches.set(1);
                                }

                                Assertions.assertTrue(
                                        consecutiveMatches.get() >= 3,
                                        "Waiting for complete baseline topology to stabilize across 3 consecutive polls (current: "
                                                + consecutiveMatches.get()
                                                + ")");
                                baselineSubtaskCountByQueueId.putAll(candidateSubtaskCount);
                                baselineCapacityByQueueId.putAll(candidateCapacity);
                                lastObservedBucketTs.set(maxTs);
                            });

            Assertions.assertEquals(
                    1, baselineSubtaskCountByQueueId.size(), "Unexpected initial queue topology");

            for (Map.Entry<Long, Long> entry : baselineSubtaskCountByQueueId.entrySet()) {
                long queueId = entry.getKey();
                log.info(
                        "Verified complete initial baseline for queueId {}: subtaskCount={}, queueCapacity={}",
                        queueId,
                        entry.getValue(),
                        baselineCapacityByQueueId.get(queueId));
            }

            AtomicLong lastReceivedCount = new AtomicLong(0L);
            Awaitility.await()
                    .atMost(2, TimeUnit.MINUTES)
                    .pollInterval(1, TimeUnit.SECONDS)
                    .untilAsserted(
                            () -> {
                                Long count = getSourceReceivedCountOrNull(jobId);
                                Assertions.assertNotNull(
                                        count,
                                        "SourceReceivedCount metric not found for jobId " + jobId);
                                Assertions.assertTrue(
                                        count > 0,
                                        "SourceReceivedCount should be > 0 once source produces records");
                                lastReceivedCount.set(count);
                            });

            for (int cycle = 1; cycle <= 3; cycle++) {
                final int currentCycle = cycle;

                Container.ExecResult savepointResult = savepointJob(String.valueOf(jobId));
                Assertions.assertEquals(
                        0,
                        savepointResult.getExitCode(),
                        "Savepoint command failed: " + savepointResult.getStderr());
                awaitJobStatus(jobId, "SAVEPOINT_DONE");

                Assertions.assertNotNull(
                        activeCliProcessFuture,
                        "Active CLI process future must not be null in cycle " + currentCycle);
                Container.ExecResult prevCliResult =
                        activeCliProcessFuture.get(1, TimeUnit.MINUTES);
                Assertions.assertEquals(
                        0,
                        prevCliResult.getExitCode(),
                        "Previous CLI process finished with non-zero exit code: "
                                + prevCliResult.getStderr());

                long endedAttemptTs = 0L;
                try {
                    endedAttemptTs = maxBucketTs(getLatestEdgePoints(jobId));
                } catch (Exception | AssertionError e) {
                    log.warn("Could not read edges after savepoint, using last observed bucket", e);
                }
                final long staleBucketFloorTs =
                        Math.max(lastObservedBucketTs.get(), endedAttemptTs);

                // Capture the committed output boundary after savepoint completion.
                long maxOffsetBeforeRestore =
                        getMaxOffset(readObservedOffsets(SINK_OUTPUT_DIR_PARALLELISM5));

                activeCliProcessFuture =
                        CompletableFuture.supplyAsync(
                                () -> {
                                    try {
                                        return restoreJob(
                                                CONF_FILE_PARALLELISM5, String.valueOf(jobId));
                                    } catch (Exception e) {
                                        throw new RuntimeException(e);
                                    }
                                });

                awaitJobStatus(jobId, "RUNNING");

                final long prevBucketTs = staleBucketFloorTs;
                final Map<Long, List<Map<String, Object>>> freshByQueue = new HashMap<>();
                final AtomicLong barrierTsRef = new AtomicLong(Long.MAX_VALUE);

                // Phase 1: completeness barrier. Wait until every queue has a bucket where all
                // expected
                // owners have reported, then until >= 3 buckets exist at or after that point. A
                // bucket
                // emitted while task groups are still publishing can legitimately show fewer
                // owners.
                Awaitility.await()
                        .atMost(2, TimeUnit.MINUTES)
                        .pollInterval(1, TimeUnit.SECONDS)
                        .untilAsserted(
                                () -> {
                                    Map<Long, List<Map<String, Object>>> all =
                                            getEdgePointsAfter(jobId, prevBucketTs);
                                    Assertions.assertEquals(
                                            baselineSubtaskCountByQueueId.keySet(),
                                            all.keySet(),
                                            "Queue topology differs after restore in cycle "
                                                    + currentCycle);

                                    long barrierTs = Long.MIN_VALUE;
                                    for (Map.Entry<Long, List<Map<String, Object>>> e :
                                            all.entrySet()) {
                                        long queueId = e.getKey();
                                        long firstComplete =
                                                firstCompleteBucketTs(
                                                        e.getValue(),
                                                        baselineSubtaskCountByQueueId.get(queueId),
                                                        baselineCapacityByQueueId.get(queueId));
                                        Assertions.assertTrue(
                                                firstComplete >= 0,
                                                String.format(
                                                        "No complete sample yet for queue %d in cycle %d "
                                                                + "(expected subtaskCount %d, capacity %d)",
                                                        queueId,
                                                        currentCycle,
                                                        baselineSubtaskCountByQueueId.get(queueId),
                                                        baselineCapacityByQueueId.get(queueId)));
                                        barrierTs = Math.max(barrierTs, firstComplete);
                                    }

                                    final long barrier = barrierTs;
                                    for (Map.Entry<Long, List<Map<String, Object>>> e :
                                            all.entrySet()) {
                                        long steadyCount =
                                                e.getValue().stream()
                                                        .filter(p -> toLong(p.get("ts")) >= barrier)
                                                        .count();
                                        Assertions.assertTrue(
                                                steadyCount >= 3,
                                                "Waiting for 3 complete buckets after barrier in cycle "
                                                        + currentCycle);
                                    }
                                    freshByQueue.clear();
                                    freshByQueue.putAll(all);
                                    barrierTsRef.set(barrier);
                                });

                // Phase 2: assert once, no retry.
                //  - before the barrier: owners may be missing but must never exceed the baseline
                //    (stale-owner regression check)
                //  - at/after the barrier: counts must equal the baseline exactly
                final long steadyFromTs = barrierTsRef.get();
                for (Map.Entry<Long, List<Map<String, Object>>> entry : freshByQueue.entrySet()) {
                    long queueId = entry.getKey();
                    long expectedSubtasks = baselineSubtaskCountByQueueId.get(queueId);
                    long expectedCapacity = baselineCapacityByQueueId.get(queueId);
                    for (Map<String, Object> point : entry.getValue()) {
                        long subtaskCount = toLong(point.get("subtaskCount"));
                        long queueCapacity = toLong(point.get("queueCapacity"));
                        long queueSize = toLong(point.get("queueSize"));
                        long pointTs = toLong(point.get("ts"));

                        Assertions.assertTrue(
                                subtaskCount <= expectedSubtasks,
                                String.format(
                                        "Stale owner: subtaskCount %d > %d for queue %d in cycle %d (bucket ts %d)",
                                        subtaskCount,
                                        expectedSubtasks,
                                        queueId,
                                        currentCycle,
                                        pointTs));
                        Assertions.assertTrue(
                                queueCapacity <= expectedCapacity,
                                String.format(
                                        "Stale owner: queueCapacity %d > %d for queue %d in cycle %d (bucket ts %d)",
                                        queueCapacity,
                                        expectedCapacity,
                                        queueId,
                                        currentCycle,
                                        pointTs));

                        if (pointTs >= steadyFromTs) {
                            Assertions.assertEquals(
                                    expectedSubtasks,
                                    subtaskCount,
                                    String.format(
                                            "subtaskCount drifted for queue %d in cycle %d (bucket ts %d)",
                                            queueId, currentCycle, pointTs));
                            Assertions.assertEquals(
                                    expectedCapacity,
                                    queueCapacity,
                                    String.format(
                                            "queueCapacity drifted for queue %d in cycle %d (bucket ts %d)",
                                            queueId, currentCycle, pointTs));
                            Assertions.assertTrue(
                                    queueSize >= 0 && queueSize <= queueCapacity,
                                    String.format(
                                            "Invalid queue size %d (capacity %d) for queue %d in cycle %d",
                                            queueSize, queueCapacity, queueId, currentCycle));
                        }
                        lastObservedBucketTs.set(Math.max(lastObservedBucketTs.get(), pointTs));
                    }
                }

                Awaitility.await()
                        .atMost(2, TimeUnit.MINUTES)
                        .pollInterval(1, TimeUnit.SECONDS)
                        .untilAsserted(
                                () -> {
                                    Long sourceReceivedAfterRestore =
                                            getSourceReceivedCountOrNull(jobId);
                                    Assertions.assertNotNull(
                                            sourceReceivedAfterRestore,
                                            String.format(
                                                    "SourceReceivedCount metric not present after restore in cycle %d",
                                                    currentCycle));
                                    Assertions.assertTrue(
                                            sourceReceivedAfterRestore >= lastReceivedCount.get(),
                                            String.format(
                                                    "SourceReceivedCount decreased after restore in cycle %d: previous=%d, current=%d",
                                                    currentCycle,
                                                    lastReceivedCount.get(),
                                                    sourceReceivedAfterRestore));
                                    lastReceivedCount.set(sourceReceivedAfterRestore);
                                });

                Awaitility.await()
                        .atMost(2, TimeUnit.MINUTES)
                        .pollInterval(1, TimeUnit.SECONDS)
                        .untilAsserted(
                                () -> {
                                    long maxOffsetAfterRestore =
                                            getMaxOffset(
                                                    readObservedOffsets(
                                                            SINK_OUTPUT_DIR_PARALLELISM5));
                                    Assertions.assertTrue(
                                            maxOffsetAfterRestore > maxOffsetBeforeRestore,
                                            String.format(
                                                    "Expected new data offsets after restore in cycle %d (boundary: %d, current max: %d)",
                                                    currentCycle,
                                                    maxOffsetBeforeRestore,
                                                    maxOffsetAfterRestore));
                                });

                assertNoOffsetDuplicates(SINK_OUTPUT_DIR_PARALLELISM5);
            }

            stopJob(String.valueOf(jobId));
            awaitJobStatus(jobId, "CANCELED");

            if (activeCliProcessFuture != null) {
                Container.ExecResult finalCliResult =
                        activeCliProcessFuture.get(1, TimeUnit.MINUTES);
                Assertions.assertEquals(
                        0,
                        finalCliResult.getExitCode(),
                        "Final CLI process finished with non-zero exit code: "
                                + finalCliResult.getStderr());
            }

            assertNoOffsetDuplicates(SINK_OUTPUT_DIR_PARALLELISM5);
        } finally {
            Throwable cleanupError = null;
            try {
                stopJob(String.valueOf(jobId));
            } catch (Exception e) {
                cleanupError = e;
            }

            if (activeCliProcessFuture != null) {
                try {
                    activeCliProcessFuture.get(30, TimeUnit.SECONDS);
                } catch (Exception e) {
                    if (cleanupError == null) {
                        cleanupError = e;
                    } else {
                        cleanupError.addSuppressed(e);
                    }
                }
            }

            try {
                FileUtils.deleteFile(SINK_OUTPUT_DIR_PARALLELISM5);
            } catch (Exception e) {
                if (cleanupError != null) {
                    cleanupError.addSuppressed(e);
                } else {
                    cleanupError = e;
                }
            }

            if (cleanupError != null) {
                log.warn(
                        "Encountered exception during test cleanup for job {}: {}",
                        jobId,
                        cleanupError.getMessage(),
                        cleanupError);
            }
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

    private void copySavepointRestoreTestPluginsToContainer() throws Exception {
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
        assertNoOffsetDuplicates(SINK_OUTPUT_DIR);
    }

    private void assertNoOffsetDuplicates(String outputDirPath) {
        Map<Long, Long> counts =
                readObservedOffsets(outputDirPath).stream()
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
        return readObservedOffsets(SINK_OUTPUT_DIR);
    }

    private List<Long> readObservedOffsets(String outputDirPath) {
        Path outputDir = Paths.get(outputDirPath);
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

    private Map<Long, Map<String, Object>> getLatestEdgePoints(long jobId) {
        Map<String, Object> resp =
                given().get(
                                getRestBaseUrl()
                                        + RestConstant.REST_URL_REALTIME_METRICS
                                        + "/jobs/"
                                        + jobId
                                        + "/edges?windowMs=600000")
                        .then()
                        .statusCode(200)
                        .extract()
                        .as(new TypeRef<Map<String, Object>>() {});
        List<Map<String, Object>> edges = castList(resp.get("edges"));
        if (edges == null || edges.isEmpty()) {
            return Collections.emptyMap();
        }
        Map<Long, Map<String, Object>> latestPointsByQueueId = new HashMap<>();
        for (Map<String, Object> edge : edges) {
            long queueId = toLong(edge.get("queueId"));
            List<Map<String, Object>> points = castList(edge.get("points"));
            if (points != null && !points.isEmpty()) {
                Map<String, Object> latestPoint =
                        points.stream()
                                .max(Comparator.comparingLong(p -> toLong(p.get("ts"))))
                                .orElse(null);
                if (latestPoint != null) {
                    latestPointsByQueueId.put(queueId, latestPoint);
                }
            }
        }
        return latestPointsByQueueId;
    }

    private long maxBucketTs(Map<Long, Map<String, Object>> latestPoints) {
        return latestPoints.values().stream().mapToLong(p -> toLong(p.get("ts"))).max().orElse(0L);
    }

    private Map<Long, List<Map<String, Object>>> getEdgePointsAfter(long jobId, long floorTs) {
        Map<String, Object> resp =
                given().get(
                                getRestBaseUrl()
                                        + RestConstant.REST_URL_REALTIME_METRICS
                                        + "/jobs/"
                                        + jobId
                                        + "/edges?windowMs=600000")
                        .then()
                        .statusCode(200)
                        .extract()
                        .as(new TypeRef<Map<String, Object>>() {});
        List<Map<String, Object>> edges = castList(resp.get("edges"));
        if (edges == null) {
            return Collections.emptyMap();
        }
        Map<Long, List<Map<String, Object>>> result = new HashMap<>();
        for (Map<String, Object> edge : edges) {
            long queueId = toLong(edge.get("queueId"));
            List<Map<String, Object>> points = castList(edge.get("points"));
            if (points == null) {
                continue;
            }
            List<Map<String, Object>> fresh =
                    points.stream()
                            .filter(p -> toLong(p.get("ts")) > floorTs)
                            .sorted(Comparator.comparingLong(p -> toLong(p.get("ts"))))
                            .collect(Collectors.toList());
            if (!fresh.isEmpty()) {
                result.put(queueId, fresh);
            }
        }
        return result;
    }

    private Long getSourceReceivedCountOrNull(long jobId) {
        Map<String, Object> jobInfo =
                given().get(getRestBaseUrl() + RestConstant.REST_URL_JOB_INFO + "/" + jobId)
                        .then()
                        .statusCode(200)
                        .extract()
                        .as(new TypeRef<Map<String, Object>>() {});
        Map<String, Object> metrics = castMap(jobInfo.get("metrics"));
        if (metrics == null || !metrics.containsKey("SourceReceivedCount")) {
            return null;
        }
        Object count = metrics.get("SourceReceivedCount");
        if (count == null) {
            return null;
        }
        return toLong(count);
    }

    private static long toLong(Object value) {
        Assertions.assertNotNull(value, "Expected a numeric metric value but got null");
        if (value instanceof Number) {
            return ((Number) value).longValue();
        }
        return Long.parseLong(String.valueOf(value));
    }

    /** Earliest bucket ts where all expected owners reported, or -1 if none yet. */
    private long firstCompleteBucketTs(
            List<Map<String, Object>> points, long expectedSubtasks, long expectedCapacity) {
        long first = -1;
        for (Map<String, Object> p : points) {
            if (toLong(p.get("subtaskCount")) == expectedSubtasks
                    && toLong(p.get("queueCapacity")) == expectedCapacity) {
                long ts = toLong(p.get("ts"));
                if (first < 0 || ts < first) {
                    first = ts;
                }
            }
        }
        return first;
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
