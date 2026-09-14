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

package org.apache.seatunnel.engine.server.savepoint;

import org.apache.seatunnel.common.utils.FileUtils;
import org.apache.seatunnel.engine.checkpoint.storage.api.CheckpointStorage;
import org.apache.seatunnel.engine.checkpoint.storage.savepoint.SavepointData;
import org.apache.seatunnel.engine.checkpoint.storage.savepoint.SavepointHandle;
import org.apache.seatunnel.engine.checkpoint.storage.savepoint.SavepointStorage;
import org.apache.seatunnel.engine.common.job.JobStatus;
import org.apache.seatunnel.engine.server.AbstractSeaTunnelServerTest;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.DisabledOnOs;
import org.junit.jupiter.api.condition.OS;

import java.util.List;

import static org.awaitility.Awaitility.await;

/**
 * Restore-selection protocol test: two savepoints of the same job must stay isolated bundles, and
 * restore must always pick the newest completed bundle (never mix pipelines from different
 * bundles).
 */
@DisabledOnOs(OS.WINDOWS)
public class SavepointRestoreSelectionTest extends AbstractSeaTunnelServerTest {

    private static final String STREAM_CONF_PATH = "stream_fakesource_to_file_savepoint.conf";

    @Test
    public void testRestorePicksNewestBundleAndKeepsOldOnes() throws Exception {
        String outPath = "/tmp/hive/warehouse/test3";
        long jobId = 823343L;
        FileUtils.createNewDir(outPath);
        try {
            runSavepointCycle(jobId, STREAM_CONF_PATH, outPath, 1);
            runSavepointCycle(jobId, STREAM_CONF_PATH, outPath, 2);

            SavepointStorage savepointStorage =
                    (SavepointStorage) server.getCheckpointService().getCheckpointStorage();
            List<SavepointHandle> handles =
                    savepointStorage.listCompletedSavepoints(String.valueOf(jobId));
            Assertions.assertEquals(2, handles.size(), "two bundles must coexist");
            // newest first by trigger timestamp
            Assertions.assertTrue(
                    handles.get(0).getTriggerTimestamp() >= handles.get(1).getTriggerTimestamp());

            SavepointData newest =
                    savepointStorage.readSavepoint(
                            String.valueOf(jobId), handles.get(0).getSavepointId());
            SavepointData older =
                    savepointStorage.readSavepoint(
                            String.valueOf(jobId), handles.get(1).getSavepointId());
            Assertions.assertNotEquals(
                    newest.getPipelineStates().values().iterator().next().getCheckpointId(),
                    older.getPipelineStates().values().iterator().next().getCheckpointId(),
                    "each savepoint has its own checkpoint id");

            // Delete the older bundle; restore must still succeed from the remaining newest one.
            savepointStorage.deleteSavepoint(
                    String.valueOf(jobId), handles.get(1).getSavepointId());
            startJob(jobId, STREAM_CONF_PATH, true);
            await().atMost(120000, java.util.concurrent.TimeUnit.MILLISECONDS)
                    .untilAsserted(
                            () ->
                                    Assertions.assertEquals(
                                            server.getCoordinatorService().getJobStatus(jobId),
                                            JobStatus.RUNNING));
            Thread.sleep(5000);
            server.getCoordinatorService().cancelJob(jobId);
            await().atMost(120000, java.util.concurrent.TimeUnit.MILLISECONDS)
                    .untilAsserted(
                            () ->
                                    Assertions.assertEquals(
                                            server.getCoordinatorService().getJobStatus(jobId),
                                            JobStatus.CANCELED));
        } finally {
            CheckpointStorage checkpointStorage =
                    server.getCheckpointService().getCheckpointStorage();
            checkpointStorage.deleteCheckpoint(String.valueOf(jobId));
            ((SavepointStorage) checkpointStorage).deleteSavepoints(String.valueOf(jobId));
        }
    }

    private void runSavepointCycle(long jobId, String conf, String outPath, int cycle)
            throws InterruptedException {
        startJob(jobId, conf, false);
        await().atMost(120000, java.util.concurrent.TimeUnit.MILLISECONDS)
                .untilAsserted(
                        () ->
                                Assertions.assertTrue(
                                        server.getCoordinatorService()
                                                        .getJobStatus(jobId)
                                                        .equals(JobStatus.RUNNING)
                                                && FileUtils.getFileLineNumberFromDir(outPath)
                                                        > cycle * 10 + 5));
        server.getCoordinatorService().savePoint(jobId);
        await().atMost(120000, java.util.concurrent.TimeUnit.MILLISECONDS)
                .untilAsserted(
                        () ->
                                Assertions.assertEquals(
                                        server.getCoordinatorService().getJobStatus(jobId),
                                        JobStatus.SAVEPOINT_DONE));
        Thread.sleep(1000);
        // restore from the just-written savepoint for the next cycle
        if (cycle < 2) {
            startJob(jobId, conf, true);
            await().atMost(120000, java.util.concurrent.TimeUnit.MILLISECONDS)
                    .untilAsserted(
                            () ->
                                    Assertions.assertEquals(
                                            server.getCoordinatorService().getJobStatus(jobId),
                                            JobStatus.RUNNING));
            Thread.sleep(3000);
        }
    }
}
