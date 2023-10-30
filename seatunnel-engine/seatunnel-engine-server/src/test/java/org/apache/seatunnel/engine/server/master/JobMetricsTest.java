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

import org.apache.seatunnel.api.common.metrics.JobMetrics;
import org.apache.seatunnel.engine.common.utils.PassiveCompletableFuture;
import org.apache.seatunnel.engine.core.dag.logical.LogicalDag;
import org.apache.seatunnel.engine.core.job.JobImmutableInformation;
import org.apache.seatunnel.engine.core.job.JobStatus;
import org.apache.seatunnel.engine.server.AbstractSeaTunnelServerTest;
import org.apache.seatunnel.engine.server.CoordinatorService;
import org.apache.seatunnel.engine.server.TestUtils;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.condition.DisabledOnOs;
import org.junit.jupiter.api.condition.OS;

import com.hazelcast.internal.serialization.Data;

import java.util.Collections;
import java.util.concurrent.TimeUnit;

import static org.apache.seatunnel.api.common.metrics.MetricNames.SINK_WRITE_COUNT;
import static org.apache.seatunnel.api.common.metrics.MetricNames.SINK_WRITE_QPS;
import static org.apache.seatunnel.api.common.metrics.MetricNames.SOURCE_RECEIVED_COUNT;
import static org.apache.seatunnel.api.common.metrics.MetricNames.SOURCE_RECEIVED_QPS;
import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

@DisabledOnOs(OS.WINDOWS)
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class JobMetricsTest extends AbstractSeaTunnelServerTest {

    @Test
    public void testGetJobMetrics() throws Exception {

        long jobId1 = System.currentTimeMillis() + 145234L;
        long jobId2 = System.currentTimeMillis() + 223452L;

        startJob(jobId1, "fake_to_console_job_metrics.conf", false);
        startJob(jobId2, "fake_to_console_job_metrics.conf", false);

        await().atMost(60000, TimeUnit.MILLISECONDS)
                .untilAsserted(
                        () -> {
                            JobMetrics jobMetrics =
                                    server.getCoordinatorService().getJobMetrics(jobId1);
                            if (jobMetrics.get(SINK_WRITE_COUNT).size() > 0) {
                                assertTrue(
                                        (Long) jobMetrics.get(SINK_WRITE_COUNT).get(0).value() > 0);
                                assertTrue(
                                        (Long) jobMetrics.get(SOURCE_RECEIVED_COUNT).get(0).value()
                                                > 0);
                            } else {
                                fail();
                            }
                        });

        // waiting for jobId1 status turn to FINISHED
        await().atMost(60000, TimeUnit.MILLISECONDS)
                .untilAsserted(
                        () ->
                                Assertions.assertTrue(
                                        server.getCoordinatorService()
                                                .getJobHistoryService()
                                                .listAllJob()
                                                .contains(
                                                        String.format(
                                                                "\"jobId\":%s,\"jobName\":\"Test\",\"jobStatus\":\"FINISHED\"",
                                                                jobId1))));

        JobMetrics jobMetrics = server.getCoordinatorService().getJobMetrics(jobId1);
        assertEquals(30, (Long) jobMetrics.get(SINK_WRITE_COUNT).get(0).value());
        assertEquals(30, (Long) jobMetrics.get(SOURCE_RECEIVED_COUNT).get(0).value());
        assertTrue((Double) jobMetrics.get(SOURCE_RECEIVED_QPS).get(0).value() > 0);
        assertTrue((Double) jobMetrics.get(SINK_WRITE_QPS).get(0).value() > 0);
    }

    @Test
    public void testMetricsOnJobRestart() throws InterruptedException {

        long jobId3 = System.currentTimeMillis() + 323475L;

        CoordinatorService coordinatorService = server.getCoordinatorService();
        startJob(jobId3, "stream_fake_to_console.conf", false);
        // waiting for job status turn to running
        await().atMost(120000, TimeUnit.MILLISECONDS)
                .untilAsserted(
                        () ->
                                Assertions.assertEquals(
                                        JobStatus.RUNNING,
                                        server.getCoordinatorService().getJobStatus(jobId3)));

        Thread.sleep(10000);

        System.out.println(coordinatorService.getJobMetrics(jobId3).toJsonString());

        // start savePoint
        coordinatorService.savePoint(jobId3);

        // waiting job FINISHED
        await().atMost(120000, TimeUnit.MILLISECONDS)
                .untilAsserted(
                        () ->
                                Assertions.assertEquals(
                                        JobStatus.FINISHED,
                                        server.getCoordinatorService().getJobStatus(jobId3)));

        // restore job
        startJob(jobId3, "stream_fake_to_console.conf", true);
        await().atMost(120000, TimeUnit.MILLISECONDS)
                .untilAsserted(
                        () ->
                                Assertions.assertEquals(
                                        JobStatus.RUNNING,
                                        server.getCoordinatorService().getJobStatus(jobId3)));

        Thread.sleep(20000);
        // check metrics
        JobMetrics jobMetrics = coordinatorService.getJobMetrics(jobId3);
        System.out.println(jobMetrics.toJsonString());
        assertTrue(40 < (Long) jobMetrics.get(SINK_WRITE_COUNT).get(0).value());
        assertTrue(40 < (Long) jobMetrics.get(SINK_WRITE_COUNT).get(1).value());
        assertTrue(40 < (Long) jobMetrics.get(SOURCE_RECEIVED_COUNT).get(0).value());
        assertTrue(40 < (Long) jobMetrics.get(SOURCE_RECEIVED_COUNT).get(1).value());

        server.getCoordinatorService().cancelJob(jobId3);
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
