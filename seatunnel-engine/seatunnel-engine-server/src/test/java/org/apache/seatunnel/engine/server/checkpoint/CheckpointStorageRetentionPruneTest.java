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

package org.apache.seatunnel.engine.server.checkpoint;

import org.apache.seatunnel.engine.checkpoint.storage.PipelineState;
import org.apache.seatunnel.engine.checkpoint.storage.api.AbstractCheckpointStorage;
import org.apache.seatunnel.engine.checkpoint.storage.api.CheckpointStorage;
import org.apache.seatunnel.engine.checkpoint.storage.exception.CheckpointStorageException;
import org.apache.seatunnel.engine.common.config.SeaTunnelConfig;
import org.apache.seatunnel.engine.common.config.server.CheckpointConfig;
import org.apache.seatunnel.engine.common.job.JobStatus;
import org.apache.seatunnel.engine.server.AbstractSeaTunnelServerTest;
import org.apache.seatunnel.engine.server.dag.physical.SubPlan;
import org.apache.seatunnel.engine.server.master.JobMaster;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.DisabledOnOs;
import org.junit.jupiter.api.condition.OS;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.awaitility.Awaitility.await;

/**
 * Verifies that checkpoint retention pruning physically removes the pruned checkpoint files from
 * the backing store, instead of only trusting the storage API's view of the checkpoint set.
 *
 * <p>Regression background: apache/seatunnel#5046, fixed by apache/seatunnel#5054. Both {@code
 * HdfsStorage#deleteCheckpoint} overloads built the delete target from the bare file name instead
 * of {@code <namespace>/<jobId>/<fileName>}. {@code FileSystem#delete} therefore found nothing and
 * returned {@code false} without throwing, the surrounding {@code catch (Exception)} kept the
 * failure invisible, and the coordinator kept "pruning" its in-memory id queue while the files
 * accumulated on disk on every retention cycle. No existing test observes pruning while a job is
 * running: {@link CheckpointStorageTest#testStreamJobWithCancel} only asserts the full wipe after
 * cancel, and the storage plugin unit tests call {@code deleteCheckpoint(jobId)} in teardown
 * without assertions, so neither the {@code (jobId, pipelineId, checkpointIdList)} overload used by
 * {@link CheckpointCoordinator#completePendingCheckpoint} nor its on-disk effect is covered.
 *
 * <p>The test runs a streaming job through the real {@code hdfs} storage plugin backed by {@code
 * fs.defaultFS: file:///} (the implementation that carried the bug, see the module's {@code
 * seatunnel.yaml}) with a small {@code max-retained}, waits until the coordinator has pruned at
 * least one checkpoint id that was previously listed by the storage API, and then lists the job's
 * checkpoint directory directly through {@link Files#list(Path)}. The on-disk listing is the
 * load-bearing assertion: a storage implementation that tracked ids in memory, or whose delete
 * silently failed as in #5046, could still report the expected checkpoint set through its API while
 * the backing store keeps growing.
 */
@DisabledOnOs(OS.WINDOWS)
public class CheckpointStorageRetentionPruneTest extends AbstractSeaTunnelServerTest {

    /**
     * Streaming job with {@code checkpoint.interval = 1000}, shared with {@link
     * CheckpointStorageTest}.
     */
    private static final String STREAM_CONF_WITH_CHECKPOINT_PATH =
            "stream_fake_to_console_with_checkpoint.conf";

    /**
     * Retention bound applied to the embedded server. {@link
     * CheckpointCoordinator#completePendingCheckpoint} prunes the oldest {@code max-retained} ids
     * once {@code 2 * max-retained} checkpoints have completed, so the first prune fires after four
     * checkpoints, a few seconds into the job at the one second checkpoint interval.
     */
    private static final int MAX_RETAINED_CHECKPOINTS = 2;

    /**
     * Layout of a checkpoint file name written by {@code
     * AbstractCheckpointStorage#getCheckPointName}: {@code
     * <timestamp>-<random>-<pipelineId>-<checkpointId>.ser}. Parsed here independently of the
     * storage implementation so that the on-disk check does not rely on the code under test.
     * Temporary files use the {@code .sertmp} suffix and are deliberately not matched.
     */
    private static final Pattern CHECKPOINT_FILE_NAME =
            Pattern.compile("^\\d+-\\d+-(\\d+)-(\\d+)\\.ser$");

    /** Generous bound for the first prune to be observed; the expected time is a few seconds. */
    private static final long PRUNE_TIMEOUT_MILLIS = 120_000L;

    @Override
    public SeaTunnelConfig loadSeaTunnelConfig() {
        SeaTunnelConfig seaTunnelConfig = super.loadSeaTunnelConfig();
        CheckpointConfig checkpointConfig = seaTunnelConfig.getEngineConfig().getCheckpointConfig();
        // JobMaster#createJobCheckpointConfig copies max-retained from the engine level
        // CheckpointStorageConfig into the per-job config, so overriding it here is sufficient.
        checkpointConfig.getStorage().setMaxRetainedCheckpoints(MAX_RETAINED_CHECKPOINTS);
        seaTunnelConfig.getEngineConfig().setCheckpointConfig(checkpointConfig);
        return seaTunnelConfig;
    }

    @Test
    public void testPrunedCheckpointFilesAreRemovedFromStorage() throws Exception {
        long jobId = System.currentTimeMillis();
        CheckpointStorage checkpointStorage = server.getCheckpointService().getCheckpointStorage();
        Assertions.assertTrue(
                checkpointStorage instanceof AbstractCheckpointStorage,
                "expected a file based checkpoint storage but got " + checkpointStorage.getClass());
        int maxRetained =
                server.getSeaTunnelConfig()
                        .getEngineConfig()
                        .getCheckpointConfig()
                        .getStorage()
                        .getMaxRetainedCheckpoints();
        Assertions.assertEquals(
                MAX_RETAINED_CHECKPOINTS,
                maxRetained,
                "the max-retained override in loadSeaTunnelConfig did not reach the server config");

        // The storage plugin consumes (and removes) the namespace key from the plugin config map
        // while initializing, so the running storage instance is the only reliable source of the
        // directory it writes to. With fs.defaultFS=file:/// the Hadoop path maps one-to-one to
        // the local path <namespace>/<jobId>.
        Path jobCheckpointDir =
                Paths.get(
                        ((AbstractCheckpointStorage) checkpointStorage).getStorageParentDirectory(),
                        String.valueOf(jobId));
        Assertions.assertTrue(
                jobCheckpointDir.isAbsolute(),
                "checkpoint namespace must resolve to an absolute path: " + jobCheckpointDir);

        startJob(jobId, STREAM_CONF_WITH_CHECKPOINT_PATH, false);
        await().atMost(120000, TimeUnit.MILLISECONDS)
                .untilAsserted(
                        () ->
                                Assertions.assertEquals(
                                        JobStatus.RUNNING,
                                        server.getCoordinatorService().getJobStatus(jobId)));

        JobMaster jobMaster = server.getCoordinatorService().getJobMaster(jobId);
        List<SubPlan> pipelines = jobMaster.getPhysicalPlan().getPipelineList();
        Assertions.assertEquals(
                1, pipelines.size(), "the test job is expected to consist of a single pipeline");
        String pipelineId = String.valueOf(pipelines.get(0).getPipelineId());

        // Every checkpoint id the storage API has ever reported for this pipeline. Once an id
        // disappears from the API listing while the job is still running, retention pruned it.
        Set<Long> observedCheckpointIds = ConcurrentHashMap.newKeySet();
        await().atMost(PRUNE_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS)
                .pollInterval(200, TimeUnit.MILLISECONDS)
                .untilAsserted(
                        () -> {
                            Set<Long> retained =
                                    listCheckpointIdsThroughApi(
                                            checkpointStorage, jobId, pipelineId);
                            observedCheckpointIds.addAll(retained);
                            Set<Long> pruned = new TreeSet<>(observedCheckpointIds);
                            pruned.removeAll(retained);
                            Assertions.assertFalse(
                                    retained.isEmpty(), "no checkpoint has been stored yet");
                            Assertions.assertFalse(
                                    pruned.isEmpty(),
                                    "retention has not pruned any previously listed checkpoint yet,"
                                            + " observed="
                                            + observedCheckpointIds
                                            + ", retained="
                                            + retained);
                            // Retention must drop the oldest checkpoints, otherwise a restore
                            // would silently lose progress.
                            Assertions.assertTrue(
                                    Collections.max(pruned) < Collections.min(retained),
                                    "retention pruned newer checkpoints instead of the oldest ones,"
                                            + " pruned="
                                            + pruned
                                            + ", retained="
                                            + retained);

                            // Load-bearing check: inspect the backing store directly. This is
                            // exactly what #5046 got wrong while the coordinator believed the
                            // pruned ids were gone.
                            Set<Long> onDisk =
                                    listCheckpointIdsOnDisk(jobCheckpointDir, pipelineId);
                            Assertions.assertTrue(
                                    Collections.disjoint(pruned, onDisk),
                                    "pruned checkpoint files still exist in "
                                            + jobCheckpointDir
                                            + ", pruned="
                                            + pruned
                                            + ", onDisk="
                                            + onDisk);
                            Assertions.assertTrue(
                                    onDisk.containsAll(retained),
                                    "retained checkpoints have no file in "
                                            + jobCheckpointDir
                                            + ", retained="
                                            + retained
                                            + ", onDisk="
                                            + onDisk);
                            // The coordinator lets its queue grow to 2 * max-retained before it
                            // deletes the oldest max-retained entries in the same synchronized
                            // call, so the number of files must never exceed that bound.
                            Assertions.assertTrue(
                                    onDisk.size() <= 2 * maxRetained,
                                    "checkpoint files accumulate beyond the retention bound in "
                                            + jobCheckpointDir
                                            + ": "
                                            + onDisk);
                        });

        jobMaster.cancelJob();
        await().atMost(120000, TimeUnit.MILLISECONDS)
                .untilAsserted(
                        () ->
                                Assertions.assertEquals(
                                        JobStatus.CANCELED,
                                        server.getCoordinatorService().getJobStatus(jobId)));
        // Cancellation wipes the whole job namespace through deleteCheckpoint(jobId). Keep the
        // sibling test's expectation on the API and verify the wipe on disk as well.
        await().atMost(30000, TimeUnit.MILLISECONDS)
                .untilAsserted(
                        () -> {
                            Assertions.assertEquals(
                                    0,
                                    checkpointStorage
                                            .getAllCheckpoints(String.valueOf(jobId))
                                            .size());
                            Set<Long> onDisk =
                                    listCheckpointIdsOnDisk(jobCheckpointDir, pipelineId);
                            Assertions.assertTrue(
                                    onDisk.isEmpty(),
                                    "checkpoint files survived job cancellation in "
                                            + jobCheckpointDir
                                            + ": "
                                            + onDisk);
                        });
    }

    /**
     * Lists the checkpoint ids the storage API currently reports for the pipeline. The ids come
     * from the deserialized checkpoint payloads rather than from file names, so this view is
     * independent from {@link #listCheckpointIdsOnDisk(Path, String)}.
     */
    private static Set<Long> listCheckpointIdsThroughApi(
            CheckpointStorage checkpointStorage, long jobId, String pipelineId)
            throws CheckpointStorageException {
        return checkpointStorage
                .getCheckpointsByJobIdAndPipelineId(String.valueOf(jobId), pipelineId).stream()
                .map(PipelineState::getCheckpointId)
                .collect(Collectors.toCollection(TreeSet::new));
    }

    /**
     * Lists the checkpoint ids that physically exist as {@code .ser} files in the job's checkpoint
     * directory, bypassing the storage plugin entirely. A missing directory yields an empty set,
     * which is the expected state after the job namespace has been wiped.
     */
    private static Set<Long> listCheckpointIdsOnDisk(Path jobCheckpointDir, String pipelineId)
            throws IOException {
        if (!Files.isDirectory(jobCheckpointDir)) {
            return Collections.emptySet();
        }
        try (Stream<Path> files = Files.list(jobCheckpointDir)) {
            return files.map(file -> CHECKPOINT_FILE_NAME.matcher(file.getFileName().toString()))
                    .filter(Matcher::matches)
                    .filter(matcher -> pipelineId.equals(matcher.group(1)))
                    .map(matcher -> Long.parseLong(matcher.group(2)))
                    .collect(Collectors.toCollection(TreeSet::new));
        } catch (NoSuchFileException e) {
            // The directory was removed between the existence check and the listing, which is a
            // legitimate transition while the job namespace is being wiped.
            return Collections.emptySet();
        }
    }
}
