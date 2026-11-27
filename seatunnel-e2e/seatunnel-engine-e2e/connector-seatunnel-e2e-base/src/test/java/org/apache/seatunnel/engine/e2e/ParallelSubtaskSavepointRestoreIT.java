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

import org.apache.seatunnel.common.config.Common;
import org.apache.seatunnel.common.config.DeployMode;
import org.apache.seatunnel.engine.client.SeaTunnelClient;
import org.apache.seatunnel.engine.client.job.ClientJobProxy;
import org.apache.seatunnel.engine.client.job.JobClient;
import org.apache.seatunnel.engine.common.config.ConfigProvider;
import org.apache.seatunnel.engine.common.config.JobConfig;
import org.apache.seatunnel.engine.common.config.SeaTunnelConfig;
import org.apache.seatunnel.engine.common.job.JobStatus;
import org.apache.seatunnel.engine.core.job.JobPipelineCheckpointData;
import org.apache.seatunnel.engine.core.job.RestoreMode;
import org.apache.seatunnel.engine.server.SeaTunnelServer;
import org.apache.seatunnel.engine.server.SeaTunnelServerStarter;
import org.apache.seatunnel.engine.server.master.JobMaster;

import org.awaitility.Awaitility;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.instance.impl.HazelcastInstanceImpl;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Guards that a stop-with-savepoint followed by a restore maps every per-subtask checkpoint state
 * back onto the subtask that produced it when the restored pipeline has parallelism greater than
 * one.
 *
 * <p><b>Why this is a boundary with real regression risk.</b> {@code
 * CheckpointCoordinator#restoreTaskState(TaskLocation)} does not restore a task's state directly
 * from the checkpoint. It walks the {@code (ActionStateKey, index)} tuples of the checkpoint plan
 * and remaps the saved subtask states onto the *current* parallelism of each action ({@code for
 * (int i = index; i < savedParallelism; i += currentParallelism)}). If that stride is derived from
 * something that is always one per task (for example the {@code pipelineTasks} map, which is keyed
 * by {@code TaskLocation#getTaskVertexId()} and therefore holds exactly one entry per subtask),
 * then subtask 0 receives the saved state of *every* subtask. For parallelism greater than one the
 * duplicated state is handed back to a reader that already owns it, so the restored job re-reads
 * data that the previous run had already committed. With parallelism one there is nothing to
 * duplicate, so the bug is invisible in every single-parallelism restore test.
 *
 * <p><b>Why the existing coverage does not reach it.</b> The regression was fixed in <a
 * href="https://github.com/apache/seatunnel/pull/12195">#12195</a>, whose only test ({@code
 * CheckpointCoordinatorTest}) fabricates an already-built plan and already-saved subtask states and
 * calls the remap indirectly. The end-to-end restore tests in this package ({@code
 * SavepointRestoreIT}, {@code CheckpointRestoreWithStopIT}, {@code
 * SavepointClusterRestartRestoreIT}) all run the sequence fixture with {@code parallelism = 1} and
 * {@code split_num = 1}, so they never restore two distinct per-subtask states onto two subtasks.
 *
 * <p><b>What this test proves.</b> It runs a two-subtask pipeline in which the checkpointable
 * sequence source hands exactly one split to every reader, so both subtasks checkpoint a non-empty
 * state of their own. It stops the job with a real savepoint, restores it once, and then requires
 * the union of the offsets committed before and after the restore to be exactly {@code [0, 4000)}
 * with no duplicate. A state-remap regression duplicates the readers' saved offsets, which makes
 * one subtask re-emit and re-commit a range that is already in the sink output, so the duplicate
 * check fails instead of the job silently losing exactly-once semantics.
 */
@Slf4j
public class ParallelSubtaskSavepointRestoreIT {

    /** Two parallel readers, one split each, so both subtasks checkpoint a non-empty state. */
    private static final String TEMPLATE_CONF =
            "stream_checkpointable_sequence_two_parallel_restore_template.conf";

    /** Total number of offsets produced by the sequence fixture ({@code end_offset}). */
    private static final long TOTAL_OFFSETS = 4000L;

    /** Offsets produced by each of the two splits; the fixture splits the range in half. */
    private static final long OFFSETS_PER_SPLIT = TOTAL_OFFSETS / 2;

    /** How many committed offsets to wait for before taking the savepoint. */
    private static final int WARM_UP_COMMITTED_OFFSETS = 200;

    private static final long CLUSTER_FORMATION_TIMEOUT_SECONDS = 60L;
    private static final long JOB_STATUS_TIMEOUT_SECONDS = 120L;
    private static final long OFFSET_PROGRESS_TIMEOUT_SECONDS = 180L;
    private static final long JOB_CLEANUP_TIMEOUT_SECONDS = 120L;

    @Test
    public void testSavepointRestoreKeepsPerSubtaskState() throws Exception {
        String testCaseName = "testSavepointRestoreKeepsPerSubtaskState";
        String testClusterName = "ParallelSubtaskSavepointRestoreIT_" + System.currentTimeMillis();
        Path sinkDir =
                Paths.get(
                        System.getProperty("user.dir"),
                        "target",
                        "parallel-subtask-restore-" + System.currentTimeMillis());
        Path confFile =
                Paths.get("/tmp/test_conf", testCaseName + "-" + System.nanoTime() + ".conf");
        Files.createDirectories(sinkDir);
        Files.createDirectories(confFile.getParent());

        HazelcastInstanceImpl masterNode = null;
        HazelcastInstanceImpl workerNode = null;
        SeaTunnelClient engineClient = null;
        ClientJobProxy sourceProxy = null;
        ClientJobProxy restoredProxy = null;
        try {
            Map<String, String> values = new HashMap<>();
            values.put("dynamic_sink_path", sinkDir.toString());
            TestUtils.createTestConfigFileFromTemplate(TEMPLATE_CONF, values, confFile.toString());

            masterNode =
                    SeaTunnelServerStarter.createMasterHazelcastInstance(
                            getSeaTunnelConfig(testClusterName));
            workerNode =
                    SeaTunnelServerStarter.createWorkerHazelcastInstance(
                            getSeaTunnelConfig(testClusterName));
            awaitClusterSize(masterNode, 2);
            awaitWorkersRegistered(masterNode, 1);

            Common.setDeployMode(DeployMode.CLIENT);
            ClientConfig clientConfig = ConfigProvider.locateAndGetClientConfig();
            clientConfig.setClusterName(TestUtils.getClusterName(testClusterName));
            engineClient = new SeaTunnelClient(clientConfig);
            JobClient jobClient = engineClient.getJobClient();

            JobConfig jobConfig = new JobConfig();
            jobConfig.setName(testCaseName);
            sourceProxy =
                    engineClient
                            .createExecutionContext(
                                    confFile.toString(),
                                    jobConfig,
                                    getSeaTunnelConfig(testClusterName))
                            .execute();
            long jobId = sourceProxy.getJobId();
            awaitJobStatus(sourceProxy, JobStatus.RUNNING);

            // Wait until both subtasks have committed offsets. Each reader owns exactly one split,
            // so a range from both halves proves that both parallel subtasks produced checkpoint
            // state before the savepoint. Without that precondition the restore would have nothing
            // to remap and the test would silently degrade into the single-subtask case.
            awaitCommittedOffsetsFromBothSplits(sinkDir);

            jobClient.savePointJob(jobId);
            awaitJobStatus(sourceProxy, JobStatus.SAVEPOINT_DONE);
            long savepointId = awaitLatestSavepointId(jobClient, jobId);
            awaitJobMasterRemoved(masterNode, jobId);

            List<Long> committedBeforeRestore = readCommittedOffsets(sinkDir);
            Assertions.assertFalse(
                    committedBeforeRestore.isEmpty(),
                    "The savepoint must not happen before the sink committed any offset");
            Assertions.assertTrue(
                    committedBeforeRestore.size() < TOTAL_OFFSETS,
                    "The savepoint must interrupt the job mid-stream, but the sink already committed "
                            + committedBeforeRestore.size()
                            + " of "
                            + TOTAL_OFFSETS
                            + " offsets");
            assertBothSplitsRepresented(committedBeforeRestore, "before the restore");
            log.warn(
                    "Source job {} saved at checkpoint {} after committing {} offsets",
                    jobId,
                    savepointId,
                    committedBeforeRestore.size());

            restoredProxy =
                    engineClient
                            .restoreExecutionContext(
                                    confFile.toString(),
                                    jobConfig,
                                    getSeaTunnelConfig(testClusterName),
                                    jobId)
                            .execute();
            Assertions.assertEquals(
                    jobId, restoredProxy.getJobId(), "A savepoint restore must keep the job id");
            awaitJobStatus(restoredProxy, JobStatus.RUNNING);

            // Exactly-once across the restore boundary: every offset in [0, TOTAL_OFFSETS) must be
            // committed exactly once. A duplicated subtask state makes the restored reader replay
            // an already committed range, which shows up as an extra committed offset here.
            Awaitility.await()
                    .atMost(OFFSET_PROGRESS_TIMEOUT_SECONDS, TimeUnit.SECONDS)
                    .pollInterval(500, TimeUnit.MILLISECONDS)
                    .untilAsserted(
                            () -> {
                                List<Long> committed = readCommittedOffsets(sinkDir);
                                Set<Long> distinct = new HashSet<>(committed);
                                Assertions.assertEquals(
                                        committed.size(),
                                        distinct.size(),
                                        "The restored job committed offsets more than once, so a"
                                                + " subtask restored the state of another subtask: "
                                                + duplicates(committed));
                                Assertions.assertEquals(
                                        TOTAL_OFFSETS,
                                        distinct.size(),
                                        "Waiting for the restored job to commit every offset of"
                                                + " both splits, committed "
                                                + distinct.size()
                                                + " of "
                                                + TOTAL_OFFSETS);
                                Assertions.assertEquals(
                                        0L,
                                        Collections.min(distinct),
                                        "The lowest committed offset must be 0");
                                Assertions.assertEquals(
                                        TOTAL_OFFSETS - 1,
                                        Collections.max(distinct),
                                        "The highest committed offset must be "
                                                + (TOTAL_OFFSETS - 1));
                            });

            // The fixture splits cover the whole configured range, so once the restored job has
            // drained them its unbounded source reports no more elements and the job must end
            // FINISHED. Requiring that terminal state also proves the restored job was fully
            // operational: every reader got a split, checkpointed it, and the restored coordinator
            // drove the pipeline to a clean end instead of leaving a reader parked forever.
            awaitJobStatus(restoredProxy, JobStatus.FINISHED);

            List<Long> committedAfterRestore = readCommittedOffsets(sinkDir);
            assertBothSplitsRepresented(committedAfterRestore, "after the restore");
        } finally {
            if (restoredProxy != null) {
                cancelQuietly(restoredProxy);
            }
            if (sourceProxy != null) {
                cancelQuietly(sourceProxy);
            }
            if (engineClient != null) {
                engineClient.close();
            }
            if (workerNode != null) {
                workerNode.shutdown();
            }
            if (masterNode != null) {
                masterNode.shutdown();
            }
            deleteQuietly(confFile);
            deleteQuietly(sinkDir);
        }
    }

    private static void cancelQuietly(ClientJobProxy proxy) {
        try {
            if (!proxy.getJobStatus().isEndState()) {
                proxy.cancelJob();
            }
        } catch (Exception e) {
            log.warn("Failed to cancel job {}: {}", proxy.getJobId(), e.getMessage());
        }
    }

    /**
     * Waits until the sink committed offsets from both halves of the sequence range. The sequence
     * fixture gives every reader at most one split, so a committed offset below and above {@link
     * #OFFSETS_PER_SPLIT} can only be produced by two different subtasks. This is what makes the
     * following savepoint carry two distinct per-subtask states.
     */
    private static void awaitCommittedOffsetsFromBothSplits(Path sinkDir) {
        Awaitility.await()
                .atMost(OFFSET_PROGRESS_TIMEOUT_SECONDS, TimeUnit.SECONDS)
                .pollInterval(200, TimeUnit.MILLISECONDS)
                .untilAsserted(
                        () -> {
                            List<Long> committed = readCommittedOffsets(sinkDir);
                            Assertions.assertTrue(
                                    committed.size() >= WARM_UP_COMMITTED_OFFSETS,
                                    "Waiting for the parallel readers to commit at least "
                                            + WARM_UP_COMMITTED_OFFSETS
                                            + " offsets, committed "
                                            + committed.size());
                            assertBothSplitsRepresented(committed, "before the savepoint");
                        });
    }

    private static void assertBothSplitsRepresented(List<Long> offsets, String phase) {
        boolean firstSplit = offsets.stream().anyMatch(offset -> offset < OFFSETS_PER_SPLIT);
        boolean secondSplit = offsets.stream().anyMatch(offset -> offset >= OFFSETS_PER_SPLIT);
        Assertions.assertTrue(
                firstSplit && secondSplit,
                "Expected committed offsets from both splits "
                        + phase
                        + ", got "
                        + summarize(offsets));
    }

    private static List<Long> duplicates(List<Long> offsets) {
        return offsets.stream()
                .collect(Collectors.groupingBy(offset -> offset, Collectors.counting()))
                .entrySet()
                .stream()
                .filter(entry -> entry.getValue() > 1)
                .map(Map.Entry::getKey)
                .sorted()
                .collect(Collectors.toList());
    }

    private static String summarize(List<Long> offsets) {
        if (offsets.isEmpty()) {
            return "no committed offsets";
        }
        return "min="
                + Collections.min(offsets)
                + ", max="
                + Collections.max(offsets)
                + ", count="
                + offsets.size();
    }

    /**
     * Reads the offsets committed by the local file sink. The file sink stages every transaction
     * under its {@code tmp_path} and only moves a transaction into {@code path} when the checkpoint
     * completes, so this directory holds exactly the committed rows and never a partially written
     * transaction.
     */
    private static List<Long> readCommittedOffsets(Path sinkDir) {
        if (!Files.exists(sinkDir)) {
            return Collections.emptyList();
        }
        List<Long> offsets = new ArrayList<>();
        try (Stream<Path> paths = Files.walk(sinkDir)) {
            List<Path> files = paths.filter(Files::isRegularFile).collect(Collectors.toList());
            for (Path file : files) {
                for (String line : Files.readAllLines(file, StandardCharsets.UTF_8)) {
                    String trimmed = line.trim();
                    if (!trimmed.isEmpty()) {
                        offsets.add(Long.parseLong(trimmed));
                    }
                }
            }
        } catch (IOException e) {
            throw new IllegalStateException("Failed to read the sink output under " + sinkDir, e);
        }
        return offsets;
    }

    private static void deleteQuietly(Path path) {
        if (path == null || !Files.exists(path)) {
            return;
        }
        try (Stream<Path> paths = Files.walk(path)) {
            paths.sorted(Collections.reverseOrder())
                    .forEach(
                            current -> {
                                try {
                                    Files.deleteIfExists(current);
                                } catch (IOException e) {
                                    log.warn("Failed to delete {}: {}", current, e.getMessage());
                                }
                            });
        } catch (IOException e) {
            log.warn("Failed to clean up {}: {}", path, e.getMessage());
        }
    }

    private static void awaitJobStatus(ClientJobProxy jobProxy, JobStatus expected) {
        Awaitility.await()
                .atMost(JOB_STATUS_TIMEOUT_SECONDS, TimeUnit.SECONDS)
                .pollInterval(200, TimeUnit.MILLISECONDS)
                .untilAsserted(
                        () ->
                                Assertions.assertEquals(
                                        expected,
                                        jobProxy.getJobStatus(),
                                        "Waiting for job status " + expected));
    }

    private static long awaitLatestSavepointId(JobClient jobClient, long jobId) {
        Awaitility.await()
                .atMost(JOB_STATUS_TIMEOUT_SECONDS, TimeUnit.SECONDS)
                .pollInterval(200, TimeUnit.MILLISECONDS)
                .untilAsserted(
                        () ->
                                Assertions.assertFalse(
                                        jobClient
                                                .getCheckpointData(jobId, RestoreMode.SAVEPOINT)
                                                .isEmpty(),
                                        "No savepoint data was stored for job " + jobId));
        return jobClient.getCheckpointData(jobId, RestoreMode.SAVEPOINT).stream()
                .mapToLong(JobPipelineCheckpointData::getCheckpointId)
                .max()
                .orElseThrow(() -> new AssertionError("No savepoint data was stored"));
    }

    private static void awaitJobMasterRemoved(HazelcastInstanceImpl masterNode, long jobId) {
        Awaitility.await()
                .atMost(JOB_CLEANUP_TIMEOUT_SECONDS, TimeUnit.SECONDS)
                .pollInterval(200, TimeUnit.MILLISECONDS)
                .untilAsserted(
                        () ->
                                Assertions.assertNull(
                                        getJobMaster(masterNode, jobId),
                                        "The stopped job's master must be cleaned up before the"
                                                + " restore is submitted"));
    }

    private static JobMaster getJobMaster(HazelcastInstanceImpl masterNode, long jobId) {
        SeaTunnelServer server = seaTunnelServer(masterNode);
        return server.getCoordinatorService() == null
                ? null
                : server.getCoordinatorService().getJobMaster(jobId);
    }

    private static SeaTunnelServer seaTunnelServer(HazelcastInstanceImpl node) {
        return node.node.getNodeEngine().getService(SeaTunnelServer.SERVICE_NAME);
    }

    private static void awaitClusterSize(HazelcastInstanceImpl node, int expected) {
        Awaitility.await()
                .atMost(CLUSTER_FORMATION_TIMEOUT_SECONDS, TimeUnit.SECONDS)
                .pollInterval(200, TimeUnit.MILLISECONDS)
                .untilAsserted(
                        () ->
                                Assertions.assertEquals(
                                        expected, node.getCluster().getMembers().size()));
    }

    private static void awaitWorkersRegistered(
            HazelcastInstanceImpl masterNode, int expectedWorkers) {
        Awaitility.await()
                .atMost(CLUSTER_FORMATION_TIMEOUT_SECONDS, TimeUnit.SECONDS)
                .pollInterval(200, TimeUnit.MILLISECONDS)
                .untilAsserted(
                        () -> {
                            SeaTunnelServer server = seaTunnelServer(masterNode);
                            Assertions.assertNotNull(
                                    server.getCoordinatorService(),
                                    "The coordinator service is not active yet");
                            Assertions.assertEquals(
                                    expectedWorkers,
                                    server.getCoordinatorService()
                                            .getResourceManager()
                                            .workerCount(Collections.emptyMap()),
                                    "Waiting for every worker to register its slots");
                        });
    }

    private static SeaTunnelConfig getSeaTunnelConfig(String testClusterName) {
        SeaTunnelConfig seaTunnelConfig = ConfigProvider.locateAndGetSeaTunnelConfig();
        seaTunnelConfig
                .getHazelcastConfig()
                .setClusterName(TestUtils.getClusterName(testClusterName));
        // The REST endpoint is irrelevant here and would otherwise contend for a fixed port.
        seaTunnelConfig.getEngineConfig().getHttpConfig().setEnabled(false);
        return seaTunnelConfig;
    }
}
