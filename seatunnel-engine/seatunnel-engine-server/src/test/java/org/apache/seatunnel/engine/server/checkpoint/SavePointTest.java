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

import org.apache.seatunnel.common.utils.FileUtils;
import org.apache.seatunnel.engine.common.exception.SavePointFailedException;
import org.apache.seatunnel.engine.common.utils.PassiveCompletableFuture;
import org.apache.seatunnel.engine.core.job.JobStatus;
import org.apache.seatunnel.engine.server.AbstractSeaTunnelServerTest;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.DisabledOnOs;
import org.junit.jupiter.api.condition.OS;

import java.util.concurrent.CompletionException;
import java.util.concurrent.TimeUnit;

import static org.awaitility.Awaitility.await;

@DisabledOnOs(OS.WINDOWS)
public class SavePointTest extends AbstractSeaTunnelServerTest<SavePointTest> {
    public static String STREAM_CONF_PATH = "stream_fakesource_to_file_savepoint.conf";
    public static String STREAM_CONF_WITH_ERROR_PATH = "stream_fake_to_inmemory_with_error.conf";
    public static String STREAM_CONF_WITH_SLEEP_PATH = "stream_fake_to_inmemory_with_sleep.conf";
    public static String BATCH_CONF_PATH = "batch_fakesource_to_file.conf";

    @Test
    public void testSavePoint() throws InterruptedException {
        savePointAndRestore(false);
    }

    @Test
    public void testSavePointWithNotExistedJob() {
        CompletionException exception =
                Assertions.assertThrows(
                        CompletionException.class,
                        () -> server.getCoordinatorService().savePoint(1L).join());
        Assertions.assertInstanceOf(SavePointFailedException.class, exception.getCause());
        Assertions.assertEquals(
                "The job with id '1' not running, save point failed",
                exception.getCause().getMessage());
    }

    @Test
    public void testSavePointButJobGoingToFail() throws InterruptedException {
        long jobId = System.currentTimeMillis();
        startJob(jobId, STREAM_CONF_WITH_ERROR_PATH, false);
        Thread.sleep(2000L);
        PassiveCompletableFuture<Void> savepoint1 = server.getCoordinatorService().savePoint(jobId);
        PassiveCompletableFuture<Void> savepoint2 = server.getCoordinatorService().savePoint(jobId);
        PassiveCompletableFuture<Void> savepoint3 = server.getCoordinatorService().savePoint(jobId);
        int errorCount = 0;
        try {
            savepoint1.join();
        } catch (Exception e) {
            errorCount++;
        }
        try {
            savepoint2.join();
        } catch (Exception e) {
            errorCount++;
        }
        try {
            savepoint3.join();
        } catch (Exception e) {
            errorCount++;
        }
        Assertions.assertEquals(3, errorCount);
        await().atMost(120, TimeUnit.SECONDS)
                .untilAsserted(
                        () ->
                                Assertions.assertEquals(
                                        server.getCoordinatorService().getJobStatus(jobId),
                                        JobStatus.FAILED));
    }

    @Test
    public void testSavePointWithMultiTimeRequest() throws InterruptedException {
        long jobId = System.currentTimeMillis();
        startJob(jobId, STREAM_CONF_WITH_SLEEP_PATH, false);
        Thread.sleep(5000L);
        PassiveCompletableFuture<Void> savepoint1 = server.getCoordinatorService().savePoint(jobId);
        Thread.sleep(1000L);
        PendingCheckpoint pendingCheckpoint1 =
                server.getCoordinatorService()
                        .getJobMaster(jobId)
                        .getCheckpointManager()
                        .getCheckpointCoordinator(1)
                        .getSavepointPendingCheckpoint();
        PassiveCompletableFuture<Void> savepoint2 = server.getCoordinatorService().savePoint(jobId);
        Thread.sleep(1000L);
        PendingCheckpoint pendingCheckpoint2 =
                server.getCoordinatorService()
                        .getJobMaster(jobId)
                        .getCheckpointManager()
                        .getCheckpointCoordinator(1)
                        .getSavepointPendingCheckpoint();
        savepoint1.join();
        savepoint2.join();
        Assertions.assertSame(pendingCheckpoint1, pendingCheckpoint2);
        await().atMost(120000, TimeUnit.MILLISECONDS)
                .untilAsserted(
                        () ->
                                Assertions.assertEquals(
                                        server.getCoordinatorService().getJobStatus(jobId),
                                        JobStatus.SAVEPOINT_DONE));
    }

    @Test
    public void testRestoreWithNoSavepointFile() {
        long jobId = System.currentTimeMillis();
        startJob(jobId, BATCH_CONF_PATH, true);
        await().atMost(120000, TimeUnit.MILLISECONDS)
                .untilAsserted(
                        () ->
                                Assertions.assertEquals(
                                        server.getCoordinatorService().getJobStatus(jobId),
                                        JobStatus.FINISHED));
    }

    @Test
    @Disabled()
    public void testSavePointOnServerRestart() throws InterruptedException {
        savePointAndRestore(true);
    }

    public void savePointAndRestore(boolean needRestart) throws InterruptedException {
        String outPath = "/tmp/hive/warehouse/test3";

        long jobId = 823342L;
        FileUtils.createNewDir(outPath);

        // 1 Start a streaming mode job
        startJob(jobId, STREAM_CONF_PATH, false);

        // 2 Wait for the job to running and start outputting data
        await().atMost(120000, TimeUnit.MILLISECONDS)
                .untilAsserted(
                        () ->
                                Assertions.assertTrue(
                                        server.getCoordinatorService()
                                                        .getJobStatus(jobId)
                                                        .equals(JobStatus.RUNNING)
                                                && FileUtils.getFileLineNumberFromDir(outPath)
                                                        > 10));

        // 3 start savePoint
        server.getCoordinatorService().savePoint(jobId);
        await().atMost(10000, TimeUnit.MILLISECONDS)
                .untilAsserted(
                        () -> {
                            JobStatus status = server.getCoordinatorService().getJobStatus(jobId);
                            Assertions.assertEquals(JobStatus.DOING_SAVEPOINT, status);
                        });

        // 4 Wait for savePoint to complete
        await().atMost(120000, TimeUnit.MILLISECONDS)
                .untilAsserted(
                        () ->
                                Assertions.assertEquals(
                                        server.getCoordinatorService().getJobStatus(jobId),
                                        JobStatus.SAVEPOINT_DONE));

        Thread.sleep(1000);

        // restart Server
        if (needRestart) {
            this.restartServer();
        }

        Thread.sleep(1000);

        // 5 Resume from savePoint
        startJob(jobId, STREAM_CONF_PATH, true);

        await().atMost(120000, TimeUnit.MILLISECONDS)
                .untilAsserted(
                        () ->
                                Assertions.assertEquals(
                                        server.getCoordinatorService().getJobStatus(jobId),
                                        JobStatus.RUNNING));

        // 6 Run long enough to ensure that the data write is complete
        Thread.sleep(30000);

        server.getCoordinatorService().cancelJob(jobId);

        await().atMost(120000, TimeUnit.MILLISECONDS)
                .untilAsserted(
                        () ->
                                Assertions.assertEquals(
                                        server.getCoordinatorService().getJobStatus(jobId),
                                        JobStatus.CANCELED));

        // 7 Check the final data count
        Assertions.assertEquals(100, FileUtils.getFileLineNumberFromDir(outPath));

        Thread.sleep(1000);
    }
}
