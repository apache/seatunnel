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
import org.apache.seatunnel.engine.common.utils.PassiveCompletableFuture;
import org.apache.seatunnel.engine.core.dag.logical.LogicalDag;
import org.apache.seatunnel.engine.core.job.JobImmutableInformation;
import org.apache.seatunnel.engine.core.job.JobStatus;
import org.apache.seatunnel.engine.server.AbstractSeaTunnelServerTest;
import org.apache.seatunnel.engine.server.TestUtils;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.DisabledOnOs;
import org.junit.jupiter.api.condition.OS;

import com.hazelcast.internal.serialization.Data;

import java.util.Collections;
import java.util.concurrent.TimeUnit;

import static org.awaitility.Awaitility.await;

@DisabledOnOs(OS.WINDOWS)
public class SavePointTest extends AbstractSeaTunnelServerTest {
    public static String OUT_PATH = "/tmp/hive/warehouse/test3";
    public static String CONF_PATH = "stream_fakesource_to_file_savepoint.conf";
    public static long JOB_ID = 823342L;

    @Test
    public void testSavePoint() throws InterruptedException {
        savePointAndRestore(false);
    }

    @Test
    @Disabled()
    public void testSavePointOnServerRestart() throws InterruptedException {
        savePointAndRestore(true);
    }

    public void savePointAndRestore(boolean needRestart) throws InterruptedException {

        FileUtils.createNewDir(OUT_PATH);

        // 1 Start a streaming mode job
        startJob(JOB_ID, CONF_PATH, false);

        // 2 Wait for the job to running and start outputting data
        await().atMost(120000, TimeUnit.MILLISECONDS)
                .untilAsserted(
                        () -> {
                            Assertions.assertTrue(
                                    server.getCoordinatorService()
                                                    .getJobStatus(JOB_ID)
                                                    .equals(JobStatus.RUNNING)
                                            && FileUtils.getFileLineNumberFromDir(OUT_PATH) > 10);
                        });

        // 3 start savePoint
        server.getCoordinatorService().savePoint(JOB_ID);

        // 4 Wait for savePoint to complete
        await().atMost(120000, TimeUnit.MILLISECONDS)
                .untilAsserted(
                        () -> {
                            Assertions.assertEquals(
                                    server.getCoordinatorService().getJobStatus(JOB_ID),
                                    JobStatus.FINISHED);
                        });

        Thread.sleep(1000);

        // restart Server
        if (needRestart) {
            this.restartServer();
        }

        Thread.sleep(1000);

        // 5 Resume from savePoint
        startJob(JOB_ID, CONF_PATH, true);

        await().atMost(120000, TimeUnit.MILLISECONDS)
                .untilAsserted(
                        () -> {
                            Assertions.assertEquals(
                                    server.getCoordinatorService().getJobStatus(JOB_ID),
                                    JobStatus.RUNNING);
                        });

        // 6 Run long enough to ensure that the data write is complete
        Thread.sleep(30000);

        server.getCoordinatorService().cancelJob(JOB_ID);

        await().atMost(120000, TimeUnit.MILLISECONDS)
                .untilAsserted(
                        () -> {
                            Assertions.assertEquals(
                                    server.getCoordinatorService().getJobStatus(JOB_ID),
                                    JobStatus.CANCELED);
                        });

        // 7 Check the final data count
        Assertions.assertEquals(100, FileUtils.getFileLineNumberFromDir(OUT_PATH));

        Thread.sleep(1000);
    }

    private void startJob(Long jobid, String path, boolean isStartWithSavePoint) {
        LogicalDag testLogicalDag = TestUtils.createTestLogicalPlan(path, jobid.toString(), jobid);

        JobImmutableInformation jobImmutableInformation =
                new JobImmutableInformation(
                        jobid,
                        "Test",
                        isStartWithSavePoint,
                        nodeEngine.getSerializationService().toData(testLogicalDag),
                        testLogicalDag.getJobConfig(),
                        Collections.emptyList(),
                        Collections.emptyList());

        Data data = nodeEngine.getSerializationService().toData(jobImmutableInformation);

        PassiveCompletableFuture<Void> voidPassiveCompletableFuture =
                server.getCoordinatorService().submitJob(jobid, data);
        voidPassiveCompletableFuture.join();
    }
}
