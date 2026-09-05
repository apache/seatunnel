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
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static io.restassured.RestAssured.given;
import static org.apache.seatunnel.e2e.common.util.ContainerUtil.PROJECT_ROOT_PATH;

/**
 * Verifies that the public REST APIs stay coherent across savepoint restore and checkpoint restore
 * lifecycles for deterministic streaming jobs.
 */
public class RecoveryLifecycleRestApiIT extends SeaTunnelEngineContainer {

    private static final String HOST = "http://localhost:";
    private static final String SAVEPOINT_CONF_FILE =
            "/savepoint-restore/stream_checkpointable_sequence_to_localfile.conf";
    private static final String CHECKPOINT_CONF_FILE =
            "/checkpoint-restore-with-stop/stream_checkpointable_sequence_to_localfile.conf";
    private static final String SAVEPOINT_SINK_OUTPUT_DIR =
            HOST_VOLUME_MOUNT_PATH + "/savepoint-restore/sinkfile";
    private static final String CHECKPOINT_SINK_OUTPUT_DIR =
            HOST_VOLUME_MOUNT_PATH + "/checkpoint-restore-with-stop/sinkfile";

    @Override
    @BeforeAll
    public void startUp() throws Exception {
        super.startUp();
        copyCheckpointRestoreTestPluginsToContainer();
    }

    @Test
    public void testSavepointRestoreRestLifecycle() throws Exception {
        FileUtils.createNewDir(SAVEPOINT_SINK_OUTPUT_DIR);
        try {
            long jobId = JobIdGenerator.newJobId();
            CompletableFuture<Container.ExecResult> sourceJobFuture =
                    CompletableFuture.supplyAsync(
                            () -> {
                                try {
                                    return executeJob(SAVEPOINT_CONF_FILE, String.valueOf(jobId));
                                } catch (Exception e) {
                                    throw new RuntimeException(e);
                                }
                            });

            awaitJobStatus(jobId, "RUNNING");
            awaitRunningJobVisible(jobId);
            awaitJobInfoStatus(jobId, "RUNNING");
            awaitCompletedCheckpoint(jobId);
            long completedBeforeSavepoint = getCompletedCheckpointCount(jobId);

            Container.ExecResult savepointResult = savepointJob(String.valueOf(jobId));
            Assertions.assertEquals(0, savepointResult.getExitCode(), savepointResult.getStderr());
            awaitJobStatus(jobId, "SAVEPOINT_DONE");
            awaitJobInfoStatus(jobId, "SAVEPOINT_DONE");
            awaitFinishedJobVisible(jobId, "SAVEPOINT_DONE");
            awaitRunningJobInvisible(jobId);

            List<Long> offsetsBeforeRestore = readObservedOffsets(SAVEPOINT_SINK_OUTPUT_DIR);
            long maxOffsetBeforeRestore = getMaxOffset(offsetsBeforeRestore);
            Assertions.assertFalse(
                    offsetsBeforeRestore.isEmpty(), "Expected committed offsets before savepoint");
            Assertions.assertEquals(0, sourceJobFuture.get().getExitCode());

            CompletableFuture<Container.ExecResult> restoreFuture =
                    CompletableFuture.supplyAsync(
                            () -> {
                                try {
                                    return restoreJob(SAVEPOINT_CONF_FILE, String.valueOf(jobId));
                                } catch (Exception e) {
                                    throw new RuntimeException(e);
                                }
                            });

            awaitJobStatus(jobId, "RUNNING");
            awaitRunningJobVisible(jobId);
            awaitJobInfoStatus(jobId, "RUNNING");
            awaitCompletedCheckpointBeyond(jobId, completedBeforeSavepoint);
            assertRestoreContinuesAfterBoundary(
                    SAVEPOINT_SINK_OUTPUT_DIR, offsetsBeforeRestore, maxOffsetBeforeRestore);
            assertNoOffsetDuplicates(SAVEPOINT_SINK_OUTPUT_DIR);

            stopJob(String.valueOf(jobId));
            awaitJobStatus(jobId, "CANCELED");
            awaitJobInfoStatus(jobId, "CANCELED");
            awaitFinishedJobVisible(jobId, "CANCELED");
            Container.ExecResult restoreResult = restoreFuture.get();
            Assertions.assertEquals(0, restoreResult.getExitCode(), restoreResult.getStderr());
        } finally {
            FileUtils.deleteFile(SAVEPOINT_SINK_OUTPUT_DIR);
        }
    }

    @Test
    public void testCheckpointRestoreRestLifecycle() throws Exception {
        FileUtils.createNewDir(CHECKPOINT_SINK_OUTPUT_DIR);
        try {
            long sourceJobId = JobIdGenerator.newJobId();
            CompletableFuture<Container.ExecResult> sourceJobFuture =
                    CompletableFuture.supplyAsync(
                            () -> {
                                try {
                                    return executeJob(
                                            CHECKPOINT_CONF_FILE, String.valueOf(sourceJobId));
                                } catch (Exception e) {
                                    throw new RuntimeException(e);
                                }
                            });

            awaitJobStatus(sourceJobId, "RUNNING");
            awaitRunningJobVisible(sourceJobId);
            awaitJobInfoStatus(sourceJobId, "RUNNING");
            awaitCompletedCheckpoint(sourceJobId);

            stopJob(String.valueOf(sourceJobId));
            awaitJobStatus(sourceJobId, "CANCELED");
            awaitJobInfoStatus(sourceJobId, "CANCELED");
            awaitFinishedJobVisible(sourceJobId, "CANCELED");
            awaitRunningJobInvisible(sourceJobId);

            List<Long> offsetsBeforeStop = readObservedOffsets(CHECKPOINT_SINK_OUTPUT_DIR);
            long maxOffsetBeforeStop = getMaxOffset(offsetsBeforeStop);
            long restoreJobId = JobIdGenerator.newJobId();
            Assertions.assertFalse(
                    offsetsBeforeStop.isEmpty(), "Expected committed offsets before stop");
            Assertions.assertEquals(0, sourceJobFuture.get().getExitCode());

            CompletableFuture<Container.ExecResult> restoreFuture =
                    CompletableFuture.supplyAsync(
                            () -> {
                                try {
                                    return restoreJobWithCheckpoint(
                                            CHECKPOINT_CONF_FILE,
                                            String.valueOf(sourceJobId),
                                            String.valueOf(restoreJobId));
                                } catch (Exception e) {
                                    throw new RuntimeException(e);
                                }
                            });

            awaitJobStatus(restoreJobId, "RUNNING");
            awaitRunningJobVisible(restoreJobId);
            awaitJobInfoStatus(restoreJobId, "RUNNING");
            awaitCompletedCheckpoint(restoreJobId);
            assertRestoreContinuesAfterBoundary(
                    CHECKPOINT_SINK_OUTPUT_DIR, offsetsBeforeStop, maxOffsetBeforeStop);
            assertNoOffsetDuplicates(CHECKPOINT_SINK_OUTPUT_DIR);

            stopJob(String.valueOf(restoreJobId));
            awaitJobStatus(restoreJobId, "CANCELED");
            awaitJobInfoStatus(restoreJobId, "CANCELED");
            awaitFinishedJobVisible(restoreJobId, "CANCELED");
            Container.ExecResult restoreResult = restoreFuture.get();
            Assertions.assertEquals(0, restoreResult.getExitCode(), restoreResult.getStderr());
        } finally {
            FileUtils.deleteFile(CHECKPOINT_SINK_OUTPUT_DIR);
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

    private void awaitRunningJobVisible(long jobId) {
        Awaitility.await()
                .atMost(2, TimeUnit.MINUTES)
                .untilAsserted(
                        () -> {
                            List<Map<String, Object>> runningJobs = getRunningJobs();
                            Assertions.assertTrue(
                                    runningJobs.stream()
                                            .anyMatch(
                                                    job ->
                                                            Long.toString(jobId)
                                                                            .equals(
                                                                                    String.valueOf(
                                                                                            castMap(
                                                                                                            job
                                                                                                                    .get(
                                                                                                                            "jobDag"))
                                                                                                    .get(
                                                                                                            "jobId")))
                                                                    && "RUNNING"
                                                                            .equals(
                                                                                    job.get(
                                                                                            RestConstant
                                                                                                    .JOB_STATUS))),
                                    "Running jobs should contain " + jobId);
                        });
    }

    private void awaitRunningJobInvisible(long jobId) {
        Awaitility.await()
                .atMost(2, TimeUnit.MINUTES)
                .untilAsserted(
                        () -> {
                            List<Map<String, Object>> runningJobs = getRunningJobs();
                            Assertions.assertTrue(
                                    runningJobs.stream()
                                            .noneMatch(
                                                    job ->
                                                            Long.toString(jobId)
                                                                    .equals(
                                                                            String.valueOf(
                                                                                    castMap(
                                                                                                    job
                                                                                                            .get(
                                                                                                                    "jobDag"))
                                                                                            .get(
                                                                                                    "jobId")))),
                                    "Running jobs should not contain " + jobId);
                        });
    }

    private void awaitFinishedJobVisible(long jobId, String finishedStatus) {
        Awaitility.await()
                .atMost(2, TimeUnit.MINUTES)
                .untilAsserted(
                        () -> {
                            List<Map<String, Object>> finishedJobs =
                                    getFinishedJobs(finishedStatus);
                            Assertions.assertTrue(
                                    finishedJobs.stream()
                                            .anyMatch(
                                                    job ->
                                                            Long.toString(jobId)
                                                                            .equals(
                                                                                    String.valueOf(
                                                                                            job.get(
                                                                                                    RestConstant
                                                                                                            .JOB_ID)))
                                                                    && finishedStatus.equals(
                                                                            job.get(
                                                                                    RestConstant
                                                                                            .JOB_STATUS))),
                                    "Finished jobs should contain "
                                            + jobId
                                            + " with status "
                                            + finishedStatus);
                        });
    }

    private void awaitJobInfoStatus(long jobId, String expectedStatus) {
        Awaitility.await()
                .atMost(2, TimeUnit.MINUTES)
                .untilAsserted(
                        () -> {
                            Map<String, Object> jobInfo = getJobInfo(jobId);
                            Assertions.assertEquals(
                                    expectedStatus, jobInfo.get(RestConstant.JOB_STATUS));
                            Assertions.assertEquals(
                                    Long.toString(jobId),
                                    String.valueOf(castMap(jobInfo.get("jobDag")).get("jobId")));
                        });
    }

    private void awaitCompletedCheckpoint(long jobId) {
        Awaitility.await()
                .atMost(2, TimeUnit.MINUTES)
                .untilAsserted(() -> Assertions.assertTrue(getCompletedCheckpointCount(jobId) > 0));
    }

    private void awaitCompletedCheckpointBeyond(long jobId, long baseline) {
        Awaitility.await()
                .atMost(2, TimeUnit.MINUTES)
                .untilAsserted(
                        () ->
                                Assertions.assertTrue(
                                        getCompletedCheckpointCount(jobId) > baseline,
                                        "Expected completed checkpoints to grow after restore"));
    }

    private void copyCheckpointRestoreTestPluginsToContainer() throws IOException {
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
            String outputDir, List<Long> offsetsBeforeRestore, long maxOffsetBeforeRestore) {
        Awaitility.await()
                .atMost(2, TimeUnit.MINUTES)
                .untilAsserted(
                        () -> {
                            List<Long> offsetsAfterRestore = readObservedOffsets(outputDir);
                            List<Long> restoredOffsets = new ArrayList<>();
                            for (Long offset : offsetsAfterRestore) {
                                if (offset > maxOffsetBeforeRestore) {
                                    restoredOffsets.add(offset);
                                }
                            }

                            Assertions.assertTrue(
                                    !restoredOffsets.isEmpty(),
                                    "Expected restored run to continue from boundary "
                                            + maxOffsetBeforeRestore);
                            Assertions.assertEquals(
                                    offsetsBeforeRestore.size() + restoredOffsets.size(),
                                    offsetsAfterRestore.size(),
                                    "Expected restore to append only new offsets after boundary "
                                            + maxOffsetBeforeRestore);
                        });
    }

    private void assertNoOffsetDuplicates(String outputDir) {
        Map<Long, Long> counts =
                readObservedOffsets(outputDir).stream()
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

    private long getCompletedCheckpointCount(long jobId) {
        Map<String, Object> overview = getCheckpointOverview(jobId);
        List<Map<String, Object>> pipelines = castList(overview.get("pipelines"));
        if (pipelines.isEmpty()) {
            return 0L;
        }
        Map<String, Object> counts = castMap(pipelines.get(0).get("counts"));
        Object counter = counts.get("completed");
        return counter instanceof Number ? ((Number) counter).longValue() : 0L;
    }

    private long getMaxOffset(List<Long> offsets) {
        return offsets.stream().mapToLong(Long::longValue).max().orElse(-1L);
    }

    private List<Long> readObservedOffsets(String outputDir) {
        Path path = Paths.get(outputDir);
        if (!Files.exists(path)) {
            return Collections.emptyList();
        }
        try (Stream<Path> paths = Files.walk(path)) {
            return paths.filter(Files::isRegularFile)
                    .flatMap(
                            file -> {
                                try {
                                    return Files.readAllLines(file).stream();
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

    private List<Map<String, Object>> getRunningJobs() {
        return given().get(getRestBaseUrl() + RestConstant.REST_URL_RUNNING_JOBS)
                .then()
                .statusCode(200)
                .extract()
                .as(new TypeRef<List<Map<String, Object>>>() {});
    }

    private List<Map<String, Object>> getFinishedJobs(String status) {
        return given().get(getRestBaseUrl() + RestConstant.REST_URL_FINISHED_JOBS + "/" + status)
                .then()
                .statusCode(200)
                .extract()
                .as(new TypeRef<List<Map<String, Object>>>() {});
    }

    private Map<String, Object> getJobInfo(long jobId) {
        return given().get(getRestBaseUrl() + RestConstant.REST_URL_JOB_INFO + "/" + jobId)
                .then()
                .statusCode(200)
                .extract()
                .as(new TypeRef<Map<String, Object>>() {});
    }

    private Map<String, Object> getCheckpointOverview(long jobId) {
        return given().get(
                        getRestBaseUrl() + RestConstant.REST_URL_CHECKPOINT_OVERVIEW + "/" + jobId)
                .then()
                .statusCode(200)
                .extract()
                .as(new TypeRef<Map<String, Object>>() {});
    }

    @SuppressWarnings("unchecked")
    private List<Map<String, Object>> castList(Object value) {
        if (value == null) {
            return Collections.emptyList();
        }
        return (List<Map<String, Object>>) value;
    }

    @SuppressWarnings("unchecked")
    private Map<String, Object> castMap(Object value) {
        if (value == null) {
            return Collections.emptyMap();
        }
        return (Map<String, Object>) value;
    }

    private String getRestBaseUrl() {
        return HOST + server.getMappedPort(8080);
    }
}
