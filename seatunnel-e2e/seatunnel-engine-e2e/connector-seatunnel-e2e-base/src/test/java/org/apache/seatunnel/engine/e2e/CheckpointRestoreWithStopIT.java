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
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static io.restassured.RestAssured.given;
import static org.apache.seatunnel.e2e.common.util.ContainerUtil.PROJECT_ROOT_PATH;

public class CheckpointRestoreWithStopIT extends SeaTunnelEngineContainer {

    private static final String HOST = "http://localhost:";
    private static final String CONF_FILE =
            "/checkpoint-restore-with-stop/stream_checkpointable_sequence_to_localfile.conf";
    private static final String SINK_OUTPUT_DIR =
            HOST_VOLUME_MOUNT_PATH + "/checkpoint-restore-with-stop/sinkfile";

    /**
     * Checkpoint storage root actually used by this test module's cluster config (see this module's
     * {@code src/test/resources/seatunnel.yaml}, {@code
     * seatunnel.engine.checkpoint.storage.plugin-config.namespace}). This intentionally overrides
     * {@code LocalFileStorage}'s own OS-default namespace ({@code /tmp/seatunnel/checkpoint/}), so
     * tests must read the real configured value instead of assuming the storage plugin's
     * out-of-the-box default.
     */
    private static final String CHECKPOINT_STORAGE_ROOT = "/tmp/seatunnel/checkpoint_snapshot/";

    @Override
    @BeforeAll
    public void startUp() throws Exception {
        super.startUp();
        copyCheckpointRestoreTestPluginsToContainer();
    }

    @Test
    public void testRestoreFromCheckpointAfterStop()
            throws IOException, InterruptedException, java.util.concurrent.ExecutionException {
        FileUtils.createNewDir(SINK_OUTPUT_DIR);
        try {
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
            List<Long> offsetsBeforeStop = readObservedOffsets();
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
                                            CONF_FILE,
                                            String.valueOf(sourceJobId),
                                            String.valueOf(restoreJobId));
                                } catch (Exception e) {
                                    throw new RuntimeException(e);
                                }
                            });

            awaitJobStatus(restoreJobId, "RUNNING");
            assertRestoreContinuesAfterCheckpoint(offsetsBeforeStop, maxOffsetBeforeStop);
            assertNoOffsetDuplicates();

            stopJob(String.valueOf(restoreJobId));
            awaitJobStatus(restoreJobId, "CANCELED");
            Container.ExecResult restoreResult = restoreFuture.get();
            Assertions.assertEquals(0, restoreResult.getExitCode(), restoreResult.getStderr());
        } finally {
            FileUtils.deleteFile(SINK_OUTPUT_DIR);
        }
    }

    /**
     * Documents today's baseline behavior for restoring from a checkpoint whose on-disk file has
     * been corrupted: checkpoint restore has no checksum/CRC validation anywhere in its read path
     * ({@code AbstractCheckpointStorage}/{@code LocalFileStorage} deserialize whatever bytes are on
     * disk with no integrity check), and there is no fallback to an older, uncorrupted checkpoint
     * even though {@code checkpoint.storage.max-retained} keeps several around.
     *
     * <p>This test drives a real streaming job to 2 completed checkpoints, stops it cleanly (so
     * both checkpoint files survive on disk, since the source job's env enables {@code
     * checkpoint.retain-after-job-cancelled}), then corrupts ONLY the file the storage plugin's own
     * file-name convention identifies as the latest one -- leaving the older, valid one untouched
     * -- and restores with {@code --restore-with-checkpoint} (i.e. {@code RestoreMode.CHECKPOINT}).
     *
     * <p>Verified end-to-end by reading current source (not assumed from the design doc): {@code
     * --restore-with-checkpoint} resolves to {@link
     * org.apache.seatunnel.engine.core.job.RestoreMode#CHECKPOINT}, which routes {@code
     * CheckpointManager.getLatestCheckpointStateByType} through {@code
     * LocalFileStorage.getCheckpointsByJobIdAndPipelineId}. That method's per-file {@code catch
     * (IOException e)} does NOT catch the failure a corrupted file actually produces: protostuff's
     * {@code ProtostuffIOUtil.mergeFrom} wraps deserialization failures in an unchecked {@code
     * RuntimeException} (confirmed empirically against the exact protostuff version this repo
     * pins), so the failure is never swallowed -- it propagates out of {@code CheckpointManager}'s
     * constructor, out of {@code JobMaster.initCheckPointManager()}, and is caught by {@code
     * JobMaster.init()}, which rethrows it as the job-submission's terminal exception. Because this
     * is a brand-new job submission (not a master-failover re-init), {@code restart=false} there,
     * so {@code cancelJob()} itself is not invoked, but the exception still fails {@code
     * CoordinatorService.submitJob}'s {@code jobSubmitFuture} -- so the observable outcome is a
     * clean, well-defined job-submission failure (non-zero CLI exit code), never a silent restart
     * from scratch and never a silent fallback to the older checkpoint.
     *
     * <p>This is a regression guard for TODAY's behavior, not a correctness assertion that this is
     * the ideal behavior: a future fix that adds checksum validation with fallback-to-previous-
     * checkpoint would (rightly) turn this corrupted-latest-checkpoint restore into a success that
     * continues from the older checkpoint, at which point this test must be updated together with
     * that fix.
     */
    @Test
    public void testRestoreFailsWhenLatestCheckpointFileIsCorrupted()
            throws IOException, InterruptedException, java.util.concurrent.ExecutionException {
        FileUtils.createNewDir(SINK_OUTPUT_DIR);
        try {
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
            // A meaningful fallback-to-previous-checkpoint fix needs an older checkpoint to fall
            // back to, so this test requires 2 completed checkpoints (well under this module's
            // configured max-retained of 3) before stopping the job.
            awaitCompletedCheckpointCount(sourceJobId, 2);

            stopJob(String.valueOf(sourceJobId));
            awaitJobStatus(sourceJobId, "CANCELED");
            Assertions.assertEquals(0, sourceJobFuture.get().getExitCode());

            List<Long> offsetsBeforeRestoreAttempt = readObservedOffsets();
            Assertions.assertFalse(
                    offsetsBeforeRestoreAttempt.isEmpty(),
                    "Expected committed offsets before stop");

            String checkpointDir = getCheckpointDirectory(sourceJobId);
            List<String> checkpointFiles = listCheckpointFiles(sourceJobId);
            Assertions.assertTrue(
                    checkpointFiles.size() >= 2,
                    "Test setup requires at least 2 retained checkpoint files on disk in "
                            + checkpointDir
                            + ", found: "
                            + checkpointFiles);

            String latestFile = findLatestCheckpointFile(checkpointFiles);
            List<String> olderFiles = new ArrayList<>(checkpointFiles);
            olderFiles.remove(latestFile);
            String olderFileChecksumsBeforeCorruption = checksumFiles(checkpointDir, olderFiles);

            // Corrupt ONLY the file the storage plugin's own naming convention treats as latest;
            // the older, valid checkpoint file(s) are deliberately left untouched so the test can
            // prove whether (or not) restore falls back to them.
            corruptCheckpointFile(checkpointDir, latestFile);

            long restoreJobId = JobIdGenerator.newJobId();
            CompletableFuture<Container.ExecResult> restoreFuture =
                    CompletableFuture.supplyAsync(
                            () -> {
                                try {
                                    return restoreJobWithCheckpoint(
                                            CONF_FILE,
                                            String.valueOf(sourceJobId),
                                            String.valueOf(restoreJobId));
                                } catch (Exception e) {
                                    throw new RuntimeException(e);
                                }
                            });

            Container.ExecResult restoreResult;
            try {
                restoreResult = restoreFuture.get(2, TimeUnit.MINUTES);
            } catch (java.util.concurrent.TimeoutException e) {
                throw new AssertionError(
                        "Restoring from a job whose latest checkpoint is corrupted is expected to "
                                + "fail fast with a clean, well-defined error; instead the restore "
                                + "command never returned within 2 minutes, which would itself be a "
                                + "regression (e.g. an unbounded retry loop) worth investigating.",
                        e);
            }

            // Baseline finding: no checksum validation anywhere in the read path means a corrupted
            // latest checkpoint surfaces as a hard job-submission failure (non-zero exit code),
            // not a hang and not a silent success.
            Assertions.assertNotEquals(
                    0,
                    restoreResult.getExitCode(),
                    "Restoring from a corrupted latest checkpoint is expected to fail cleanly "
                            + "rather than silently succeed; stdout="
                            + restoreResult.getStdout()
                            + " stderr="
                            + restoreResult.getStderr());
            Assertions.assertFalse(
                    (restoreResult.getStdout() + restoreResult.getStderr()).trim().isEmpty(),
                    "Expected the failed restore attempt to surface a diagnostic message");

            // Neither a silent restart-from-scratch nor a silent fallback to the older checkpoint
            // happened: not a single new row was ever appended to the sink after the failed
            // restore attempt, because the job never reached RUNNING.
            List<Long> offsetsAfterFailedRestore = readObservedOffsets();
            Assertions.assertEquals(
                    offsetsBeforeRestoreAttempt.size(),
                    offsetsAfterFailedRestore.size(),
                    "A failed restore must not silently process any new data, whether by "
                            + "restarting from scratch or by silently falling back to the older "
                            + "checkpoint");

            // The untouched older checkpoint file(s) remain byte-for-byte identical: this proves
            // the corruption was scoped to only the latest file, and that the failed restore
            // attempt did not itself mutate them.
            String olderFileChecksumsAfterFailedRestore = checksumFiles(checkpointDir, olderFiles);
            Assertions.assertEquals(
                    olderFileChecksumsBeforeCorruption,
                    olderFileChecksumsAfterFailedRestore,
                    "The older, non-corrupted checkpoint file(s) must remain untouched");
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

    private long getCompletedCheckpointCount(long jobId) {
        return getPipelineCounter(jobId, "completed");
    }

    private void awaitCompletedCheckpointCount(long jobId, long minCount) {
        Awaitility.await()
                .atMost(2, TimeUnit.MINUTES)
                .untilAsserted(
                        () ->
                                Assertions.assertTrue(
                                        getCompletedCheckpointCount(jobId) >= minCount));
    }

    /**
     * Absolute in-container directory holding this job's checkpoint files (see {@link
     * #CHECKPOINT_STORAGE_ROOT}); matches {@code AbstractCheckpointStorage}'s own layout of {@code
     * <namespace>/<jobId>/<file>}.
     */
    private String getCheckpointDirectory(long jobId) {
        return CHECKPOINT_STORAGE_ROOT + jobId + "/";
    }

    /**
     * Lists this job's checkpoint files directly inside the container (the checkpoint storage root
     * is not host-bind-mounted, unlike the sink output directory), by shelling out via
     * Testcontainers' {@code execInContainer} rather than assuming any particular host-visible
     * mount.
     */
    private List<String> listCheckpointFiles(long jobId) throws IOException, InterruptedException {
        Container.ExecResult result =
                server.execInContainer(
                        "sh",
                        "-c",
                        "ls -1 " + getCheckpointDirectory(jobId) + " 2>/dev/null || true");
        return Arrays.stream(result.getStdout().split("\n"))
                .map(String::trim)
                .filter(line -> line.endsWith(".ser"))
                .collect(Collectors.toList());
    }

    /**
     * Replicates {@code AbstractCheckpointStorage}'s own "latest checkpoint" selection rule so the
     * test corrupts precisely the file production code would read back during restore: file names
     * are {@code <epochMillis>-<random>-<pipelineId>-<checkpointId>.ser}, and the storage plugin
     * picks the file with the largest leading epoch-millis segment as latest (see {@code
     * AbstractCheckpointStorage#getLatestCheckpointFileNameByJobIdAndPipelineId}).
     */
    private String findLatestCheckpointFile(List<String> fileNames) {
        return fileNames.stream()
                .max(Comparator.comparingLong(name -> Long.parseLong(name.split("-")[0])))
                .orElseThrow(
                        () -> new IllegalStateException("No checkpoint files found: " + fileNames));
    }

    /**
     * MD5 checksums (one {@code "<hash> <path>"} line per file, in {@code md5sum}'s own stable
     * ordering) used to prove a set of checkpoint files was left byte-for-byte untouched across an
     * operation.
     */
    private String checksumFiles(String directory, List<String> fileNames)
            throws IOException, InterruptedException {
        if (fileNames.isEmpty()) {
            return "";
        }
        String paths =
                fileNames.stream().map(name -> directory + name).collect(Collectors.joining(" "));
        Container.ExecResult result = server.execInContainer("sh", "-c", "md5sum " + paths);
        Assertions.assertEquals(
                0,
                result.getExitCode(),
                "Failed to checksum checkpoint files: " + result.getStderr());
        return result.getStdout();
    }

    /**
     * Overwrites a checkpoint file in place with random bytes of the exact same length, simulating
     * realistic on-disk corruption (e.g. a bad block, or a crash mid-write landing on top of an old
     * file) without changing the file's size, then forces the file's very first byte to {@code
     * 0x00}. The checkpoint storage layer performs no checksum validation on read, so this
     * corrupted file is exactly what {@code LocalFileStorage} will try to deserialize as the latest
     * checkpoint during restore.
     *
     * <p>The leading zero byte is deliberate, not incidental: protostuff/protobuf's wire format
     * reads a leading varint field tag starting at byte 0, and field number {@code 0} is a
     * reserved, always-invalid tag that every decoder rejects immediately, before any other byte is
     * even interpreted -- confirmed with a standalone {@code RuntimeSchema}/{@code
     * ProtostuffIOUtil} reproduction against a same-shaped POJO, forcing byte 0 to {@code 0x00}
     * threw on 3600/3600 trials across file sizes from 1 byte to 2014 bytes. Plain full-file random
     * overwrite alone does NOT reliably reproduce this test's documented failure mode: the same
     * reproduction showed random bytes forming a self-consistent, non-throwing protostuff field
     * sequence on roughly 1 in 5 attempts, independent of file size, which would make a
     * pure-random-bytes version of this regression test flaky. Forcing only the first byte to an
     * invalid tag, while leaving every other byte genuinely random, keeps the corruption realistic
     * (e.g. a torn write or block zeroing landing on the start of the file) while making
     * deserialization failure deterministic.
     */
    private void corruptCheckpointFile(String directory, String fileName)
            throws IOException, InterruptedException {
        String path = directory + fileName;
        Container.ExecResult sizeResult = server.execInContainer("sh", "-c", "wc -c < " + path);
        Assertions.assertEquals(
                0,
                sizeResult.getExitCode(),
                "Failed to stat checkpoint file " + path + ": " + sizeResult.getStderr());
        long size = Long.parseLong(sizeResult.getStdout().trim());
        Assertions.assertTrue(size > 0, "Checkpoint file unexpectedly empty: " + path);
        Container.ExecResult corruptResult =
                server.execInContainer(
                        "sh",
                        "-c",
                        "head -c "
                                + size
                                + " /dev/urandom > "
                                + path
                                + " && head -c 1 /dev/zero | dd of="
                                + path
                                + " bs=1 count=1 conv=notrunc 2>/dev/null");
        Assertions.assertEquals(
                0,
                corruptResult.getExitCode(),
                "Failed to corrupt checkpoint file " + path + ": " + corruptResult.getStderr());
        Container.ExecResult verifySizeResult =
                server.execInContainer("sh", "-c", "wc -c < " + path);
        Assertions.assertEquals(
                0,
                verifySizeResult.getExitCode(),
                "Failed to verify corrupted checkpoint file size "
                        + path
                        + ": "
                        + verifySizeResult.getStderr());
        Assertions.assertEquals(
                size,
                Long.parseLong(verifySizeResult.getStdout().trim()),
                "Corruption must not change the checkpoint file's size, since this test documents"
                        + " behavior for same-size on-disk corruption, not truncation");
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

    private void assertRestoreContinuesAfterCheckpoint(
            List<Long> offsetsBeforeStop, long maxOffsetBeforeStop) {
        Awaitility.await()
                .atMost(2, TimeUnit.MINUTES)
                .untilAsserted(
                        () -> {
                            List<Long> offsetsAfterRestore = readObservedOffsets();
                            List<Long> restoredOffsets = new ArrayList<>();
                            for (Long offset : offsetsAfterRestore) {
                                if (offset > maxOffsetBeforeStop) {
                                    restoredOffsets.add(offset);
                                }
                            }

                            Assertions.assertTrue(
                                    !restoredOffsets.isEmpty(),
                                    "Expected restored run to continue from checkpoint boundary "
                                            + maxOffsetBeforeStop);

                            Assertions.assertEquals(
                                    offsetsBeforeStop.size() + restoredOffsets.size(),
                                    offsetsAfterRestore.size(),
                                    "Expected restore to append only new offsets after checkpoint boundary "
                                            + maxOffsetBeforeStop);
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
