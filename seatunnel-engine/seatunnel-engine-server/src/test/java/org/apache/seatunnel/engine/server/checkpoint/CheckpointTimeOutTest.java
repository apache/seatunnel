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

import org.apache.seatunnel.engine.common.utils.PassiveCompletableFuture;
import org.apache.seatunnel.engine.core.dag.logical.LogicalDag;
import org.apache.seatunnel.engine.core.job.JobImmutableInformation;
import org.apache.seatunnel.engine.core.job.JobStatus;
import org.apache.seatunnel.engine.server.AbstractSeaTunnelServerTest;
import org.apache.seatunnel.engine.server.TestUtils;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import com.hazelcast.internal.serialization.Data;

import java.util.Collections;
import java.util.concurrent.TimeUnit;

import static org.awaitility.Awaitility.await;

public class CheckpointTimeOutTest extends AbstractSeaTunnelServerTest {

    public static String CONF_PATH = "stream_fake_to_console_checkpointTimeOut.conf";
    public static long JOB_ID = System.currentTimeMillis();

    @Test
    public void testJobLevelCheckpointTimeOut() {
        startJob(JOB_ID, CONF_PATH);

        await().atMost(120000, TimeUnit.MILLISECONDS)
                .untilAsserted(
                        () -> {
                            Assertions.assertTrue(
                                    server.getCoordinatorService()
                                            .getJobStatus(JOB_ID)
                                            .equals(JobStatus.RUNNING));
                        });

        await().atMost(120000, TimeUnit.MILLISECONDS)
                .untilAsserted(
                        () -> {
                            Assertions.assertTrue(
                                    server.getCoordinatorService()
                                            .getJobStatus(JOB_ID)
                                            .equals(JobStatus.FAILED));
                        });
    }

    private void startJob(Long jobid, String path) {
        LogicalDag testLogicalDag = TestUtils.createTestLogicalPlan(path, jobid.toString(), jobid);

        JobImmutableInformation jobImmutableInformation =
                new JobImmutableInformation(
                        jobid,
                        "Test",
                        false,
                        nodeEngine.getSerializationService().toData(testLogicalDag),
                        testLogicalDag.getJobConfig(),
                        Collections.emptyList());

        Data data = nodeEngine.getSerializationService().toData(jobImmutableInformation);

        PassiveCompletableFuture<Void> voidPassiveCompletableFuture =
                server.getCoordinatorService().submitJob(jobid, data);
        voidPassiveCompletableFuture.join();
    }
}
