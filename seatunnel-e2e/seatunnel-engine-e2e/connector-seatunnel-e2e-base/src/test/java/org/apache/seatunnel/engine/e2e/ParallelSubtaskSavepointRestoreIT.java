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
import java.util.Base64;
import java.util.Collections;
import java.util.Comparator;
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
 * state of their own. The fixture parks every reader at a deterministic, non-terminal offset of its
 * own split, so the savepoint is taken while the input is provably only half processed instead of
 * racing the readers to the end of their finite range. After the restore the test requires the
 * union of the offsets committed before and after the restore to be exactly {@code [0, 4000)} with
 * no duplicate, both while the restored job is draining and again on the terminal snapshot taken
 * after it reached {@code FINISHED}. A state-remap regression duplicates the readers' saved
 * offsets, which makes one subtask re-emit and re-commit a range that is already in the sink
 * output, so the duplicate check fails instead of the job silently losing exactly-once semantics.
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

    /**
     * Offsets every reader emits before the fixture gate parks it. The value matches {@code
     * pause_after_offsets_per_split} in the template, so the committed snapshot taken at the
     * savepoint is a deterministic prefix of both splits instead of a wall-clock guess about how
     * far the readers got.
     */
    private static final long PAUSE_OFFSETS_PER_SPLIT = 1000L;

    /** Name fragment of the source action whose per-subtask state the savepoint must contain. */
    private static final String SOURCE_ACTION_NAME_FRAGMENT = "CheckpointableSequenceSource";

    /** Number of parallel source subtasks that must each own one saved state. */
    private static final int EXPECTED_SOURCE_SUBTASKS = 2;

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
        Path gateDir =
                Paths.get(
                        System.getProperty("user.dir"),
                        "target",
                        "parallel-subtask-restore-gate-" + System.currentTimeMillis());
        Path resumeFile = gateDir.resolve("resume");
        Path confFile =
                Paths.get("/tmp/test_conf", testCaseName + "-" + System.nanoTime() + ".conf");
        Set<Long> parkedOffsets = expectedOffsets(PAUSE_OFFSETS_PER_SPLIT);
        Set<Long> allOffsets = expectedOffsets(OFFSETS_PER_SPLIT);
        Files.createDirectories(sinkDir);
        Files.createDirectories(gateDir);
        Files.createDirectories(confFile.getParent());

        HazelcastInstanceImpl masterNode = null;
        HazelcastInstanceImpl workerNode = null;
        SeaTunnelClient engineClient = null;
        ClientJobProxy sourceProxy = null;
        ClientJobProxy restoredProxy = null;
        try {
            Map<String, String> values = new HashMap<>();
            values.put("dynamic_sink_path", sinkDir.toString());
            values.put("resume_file", resumeFile.toString());
            TestUtils.createTestConfigFileFromTemplate(TEMPLATE_CONF, values, confFile.toString());
            Assertions.assertFalse(
                    Files.exists(resumeFile),
                    "The fixture gate must be closed before the savepoint, but "
                            + resumeFile
                            + " already exists");

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

            // Controlled partial-progress precondition. The fixture parks both readers after
            // PAUSE_OFFSETS_PER_SPLIT offsets of their own split and cannot advance past that point
            // until the resume file exists, so this observation is not a race against the remaining
            // emission window: the committed snapshot stays exactly this prefix until the test
            // releases the gate further down. Equality proves that both parallel readers emitted
            // their own half; that both subtasks checkpointed their own state is asserted on the
            // savepoint below.
            awaitCommittedOffsets(sinkDir, parkedOffsets, "while the readers are parked");

            jobClient.savePointJob(jobId);
            awaitJobStatus(sourceProxy, JobStatus.SAVEPOINT_DONE);
            JobPipelineCheckpointData savepoint = awaitLatestSavepoint(jobClient, jobId);
            assertPerSubtaskStateSaved(savepoint);
            awaitJobMasterRemoved(masterNode, jobId);

            List<Long> committedBeforeRestore = readCommittedOffsets(sinkDir);
            Assertions.assertTrue(
                    committedBeforeRestore.size() < TOTAL_OFFSETS,
                    "The savepoint must interrupt the job before it drains the whole input, but the"
                            + " sink already committed "
                            + committedBeforeRestore.size()
                            + " of "
                            + TOTAL_OFFSETS
                            + " offsets");
            assertCommittedOffsets(committedBeforeRestore, parkedOffsets, "before the restore");
            log.warn(
                    "Source job {} saved at checkpoint {} with {} committed offsets, both readers"
                            + " parked mid-split",
                    jobId,
                    savepoint.getCheckpointId(),
                    committedBeforeRestore.size());

            // The original job is gone and its readers are parked, so releasing the gate cannot
            // change the committed snapshot asserted above. The restored readers resume from their
            // own saved offsets and must never park again, because the gate is open from now on.
            Files.write(resumeFile, "resume".getBytes(StandardCharsets.UTF_8));

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
            // an already committed range, which shows up as an extra committed offset here. The
            // retry is a watchdog for the sink to publish committed transactions, not a race with
            // the fixture: the restored readers resume from a fixed saved offset.
            awaitCommittedOffsets(sinkDir, allOffsets, "before the restored job reaches FINISHED");

            // The fixture splits cover the whole configured range, so once the restored job has
            // drained them its unbounded source reports no more elements and the job must end
            // FINISHED. Requiring that terminal state also proves the restored job was fully
            // operational: every reader got a split, checkpointed it, and the restored coordinator
            // drove the pipeline to a clean end instead of leaving a reader parked forever.
            awaitJobStatus(restoredProxy, JobStatus.FINISHED);

            // The restored job can still commit replayed rows in a checkpoint that follows the
            // moment the polling above saw the complete range, so the complete exactly-once oracle
            // is asserted again on the terminal snapshot, this time without any retry.
            assertCommittedOffsets(
                    readCommittedOffsets(sinkDir),
                    allOffsets,
                    "after the restored job reached FINISHED");
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
            deleteQuietly(gateDir);
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
     * Builds the offsets the sink must hold, given how many offsets each of the two splits
     * processed. Both splits start at a multiple of {@link #OFFSETS_PER_SPLIT}, so the expected
     * output of a paused run is a prefix of every split rather than an arbitrary subset.
     */
    private static Set<Long> expectedOffsets(long offsetsPerSplit) {
        Set<Long> expected = new HashSet<>();
        for (long splitStart = 0; splitStart < TOTAL_OFFSETS; splitStart += OFFSETS_PER_SPLIT) {
            for (long offset = 0; offset < Math.min(offsetsPerSplit, OFFSETS_PER_SPLIT); offset++) {
                expected.add(splitStart + offset);
            }
        }
        return expected;
    }

    /**
     * Waits until the sink holds exactly {@code expected} committed offsets.
     *
     * <p>Only the sink and the checkpoint coordinator are timed here: when the fixture gate is
     * closed the expected snapshot is already reached and stays stable, so a timeout means the
     * precondition could not be established (missing commits, replayed rows), never that the test
     * was too slow to interrupt a still running job.
     */
    private static void awaitCommittedOffsets(Path sinkDir, Set<Long> expected, String phase) {
        Awaitility.await()
                .atMost(OFFSET_PROGRESS_TIMEOUT_SECONDS, TimeUnit.SECONDS)
                .pollInterval(200, TimeUnit.MILLISECONDS)
                .untilAsserted(
                        () ->
                                assertCommittedOffsets(
                                        readCommittedOffsets(sinkDir), expected, phase));
    }

    /**
     * Complete exactly-once oracle for one snapshot of the sink output: the committed lines must be
     * exactly {@code expected}, with no duplicate and no gap.
     *
     * <p>It is shared by the deterministic partial snapshot taken while the readers are parked, the
     * snapshot taken before the restore, and the terminal snapshot taken after the restored job
     * reached {@code FINISHED}, so a late replay in the final checkpoints cannot escape it.
     */
    private static void assertCommittedOffsets(
            List<Long> committed, Set<Long> expected, String phase) {
        Set<Long> distinct = new HashSet<>(committed);
        Assertions.assertEquals(
                expected.size(),
                committed.size(),
                "The sink must hold exactly "
                        + expected.size()
                        + " committed offsets "
                        + phase
                        + ", but it committed "
                        + committed.size()
                        + " lines: "
                        + summarize(committed));
        Assertions.assertEquals(
                committed.size(),
                distinct.size(),
                "The restored job committed offsets more than once "
                        + phase
                        + ", so a subtask restored the state of another subtask: "
                        + duplicates(committed));
        Set<Long> missing = new HashSet<>(expected);
        missing.removeAll(distinct);
        Set<Long> unexpected = new HashSet<>(distinct);
        unexpected.removeAll(expected);
        Assertions.assertTrue(
                missing.isEmpty() && unexpected.isEmpty(),
                "The committed offsets "
                        + phase
                        + " are not the expected set: missing "
                        + summarize(missing)
                        + ", unexpected "
                        + summarize(unexpected));
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

    private static String summarize(Iterable<Long> offsets) {
        List<Long> sorted = new ArrayList<>();
        offsets.forEach(sorted::add);
        if (sorted.isEmpty()) {
            return "no offsets";
        }
        Collections.sort(sorted);
        return "min="
                + sorted.get(0)
                + ", max="
                + sorted.get(sorted.size() - 1)
                + ", count="
                + sorted.size();
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

    private static JobPipelineCheckpointData awaitLatestSavepoint(JobClient jobClient, long jobId) {
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
                .max(Comparator.comparingLong(JobPipelineCheckpointData::getCheckpointId))
                .orElseThrow(
                        () -> new AssertionError("No savepoint data was stored for job " + jobId));
    }

    /**
     * Asserts that the savepoint serialized one distinct, non-empty source reader state per
     * parallel subtask.
     *
     * <p>This is the other half of the partial-progress precondition: the committed offsets prove
     * how far the readers got, this proves the savepoint really carries two per-subtask states
     * instead of one state shared by both subtasks. Without it a fixture that handed the same split
     * to both readers would still look like a valid parallel restore.
     */
    private static void assertPerSubtaskStateSaved(JobPipelineCheckpointData savepoint) {
        Map<String, JobPipelineCheckpointData.ActionState> taskStates = savepoint.getTaskStates();
        Assertions.assertNotNull(
                taskStates,
                "The savepoint checkpoint "
                        + savepoint.getCheckpointId()
                        + " must carry the per-action task states");
        List<JobPipelineCheckpointData.ActionSubtaskState> sourceStates = new ArrayList<>();
        for (Map.Entry<String, JobPipelineCheckpointData.ActionState> entry :
                taskStates.entrySet()) {
            if (!entry.getKey().contains(SOURCE_ACTION_NAME_FRAGMENT)) {
                continue;
            }
            for (JobPipelineCheckpointData.ActionSubtaskState subtaskState :
                    entry.getValue().getSubtaskState()) {
                if (subtaskState != null) {
                    sourceStates.add(subtaskState);
                }
            }
        }
        Assertions.assertEquals(
                EXPECTED_SOURCE_SUBTASKS,
                sourceStates.size(),
                "The savepoint must hold one "
                        + SOURCE_ACTION_NAME_FRAGMENT
                        + " state per parallel subtask, got "
                        + sourceStates.size()
                        + " of "
                        + EXPECTED_SOURCE_SUBTASKS
                        + "; action states: "
                        + taskStates.keySet());
        Set<Integer> savedIndexes =
                sourceStates.stream()
                        .map(JobPipelineCheckpointData.ActionSubtaskState::getIndex)
                        .collect(Collectors.toSet());
        Set<Integer> expectedIndexes = new HashSet<>();
        for (int index = 0; index < EXPECTED_SOURCE_SUBTASKS; index++) {
            expectedIndexes.add(index);
        }
        Assertions.assertEquals(
                expectedIndexes,
                savedIndexes,
                "Every source subtask must save its own state, saved subtask indexes: "
                        + savedIndexes);
        for (JobPipelineCheckpointData.ActionSubtaskState sourceState : sourceStates) {
            Assertions.assertNotNull(
                    sourceState.getState(),
                    "Source subtask " + sourceState.getIndex() + " saved no state at all");
            Assertions.assertFalse(
                    sourceState.getState().isEmpty(),
                    "Source subtask " + sourceState.getIndex() + " saved an empty state list");
            Assertions.assertTrue(
                    sourceState.getState().stream()
                            .allMatch(state -> state != null && state.length > 0),
                    "Source subtask "
                            + sourceState.getIndex()
                            + " saved an empty serialized reader state");
        }
        Set<String> fingerprints =
                sourceStates.stream()
                        .map(ParallelSubtaskSavepointRestoreIT::fingerprint)
                        .collect(Collectors.toSet());
        Assertions.assertEquals(
                sourceStates.size(),
                fingerprints.size(),
                "Every subtask must checkpoint its own split state, but the serialized states are"
                        + " not distinct: "
                        + fingerprints);
    }

    private static String fingerprint(JobPipelineCheckpointData.ActionSubtaskState state) {
        return state.getState().stream()
                .map(Base64.getEncoder()::encodeToString)
                .collect(Collectors.joining("|"));
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
