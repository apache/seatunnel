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

package org.apache.seatunnel.engine.server.master;

import static org.awaitility.Awaitility.await;

import org.apache.seatunnel.engine.common.utils.PassiveCompletableFuture;
import org.apache.seatunnel.engine.core.dag.logical.LogicalDag;
import org.apache.seatunnel.engine.core.job.JobImmutableInformation;
import org.apache.seatunnel.engine.server.AbstractSeaTunnelServerTest;
import org.apache.seatunnel.engine.server.TestUtils;

import com.hazelcast.internal.serialization.Data;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.condition.DisabledOnOs;
import org.junit.jupiter.api.condition.OS;

import java.util.Collections;
import java.util.concurrent.TimeUnit;

@DisabledOnOs(OS.WINDOWS)
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class JobHistoryServiceTest extends AbstractSeaTunnelServerTest {

    private static final Long JOB_1 = 1L;
    private static final Long JOB_2 = 2L;
    private static final Long JOB_3 = 3L;

    @Test
    public void testlistJobState() throws Exception {
        startJob(JOB_1, "fake_to_console.conf");

        // waiting for JOB_1 status turn to RUNNING
        await().atMost(60000, TimeUnit.MILLISECONDS)
            .untilAsserted(() -> Assertions.assertTrue(
                server.getCoordinatorService().getJobHistoryService().listAllJob().contains(String.format("{\"jobId\":%s,\"jobStatus\":\"RUNNING\"}", JOB_1))));

        // waiting for JOB_1 status turn to FINISHED
        await().atMost(60000, TimeUnit.MILLISECONDS)
            .untilAsserted(() -> Assertions.assertTrue(
                server.getCoordinatorService().getJobHistoryService().listAllJob().contains(String.format("{\"jobId\":%s,\"jobStatus\":\"FINISHED\"}", JOB_1))));

        startJob(JOB_2, "fake_to_console.conf");
        // waiting for JOB_2 status turn to FINISHED and JOB_2 status turn to RUNNING
        await().atMost(60000, TimeUnit.MILLISECONDS)
            .untilAsserted(() -> Assertions.assertTrue(
                server.getCoordinatorService().getJobHistoryService().listAllJob().contains(String.format("{\"jobId\":%s,\"jobStatus\":\"FINISHED\"}", JOB_1))
                &&
                    server.getCoordinatorService().getJobHistoryService().listAllJob().contains(String.format("{\"jobId\":%s,\"jobStatus\":\"RUNNING\"}", JOB_2))
            ));
    }

    @Test
    public void testGetJobStatus() throws Exception{
        startJob(JOB_3, "fake_to_console.conf");
        // waiting for JOB_3 status turn to RUNNING
        await().atMost(60000, TimeUnit.MILLISECONDS)
            .untilAsserted(() -> Assertions.assertTrue(
                server.getCoordinatorService().getJobHistoryService().getJobDetailStateAsString(JOB_3).contains("TaskGroupLocation")
                &&
                    server.getCoordinatorService().getJobHistoryService().getJobDetailStateAsString(JOB_3).contains("RUNNING")
            ));

        // waiting for job1 status turn to FINISHED
        await().atMost(60000, TimeUnit.MILLISECONDS)
            .untilAsserted(() -> Assertions.assertTrue(
                server.getCoordinatorService().getJobHistoryService().getJobDetailStateAsString(JOB_3).contains("TaskGroupLocation")
                    &&
                    server.getCoordinatorService().getJobHistoryService().getJobDetailStateAsString(JOB_3).contains("FINISHED")
            ));
    }

    private void startJob(Long jobid, String path){
        LogicalDag testLogicalDag =
            TestUtils.createTestLogicalPlan(path, jobid.toString(), jobid);

        JobImmutableInformation jobImmutableInformation = new JobImmutableInformation(jobid,
            nodeEngine.getSerializationService().toData(testLogicalDag), testLogicalDag.getJobConfig(),
            Collections.emptyList());

        Data data = nodeEngine.getSerializationService().toData(jobImmutableInformation);

        PassiveCompletableFuture<Void> voidPassiveCompletableFuture =
            server.getCoordinatorService().submitJob(jobid, data);
        voidPassiveCompletableFuture.join();
    }
}
