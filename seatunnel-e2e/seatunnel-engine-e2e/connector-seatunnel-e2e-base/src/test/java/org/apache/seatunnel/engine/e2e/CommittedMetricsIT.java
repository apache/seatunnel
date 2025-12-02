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

import org.apache.seatunnel.engine.client.SeaTunnelClient;
import org.apache.seatunnel.engine.client.job.ClientJobExecutionEnvironment;
import org.apache.seatunnel.engine.client.job.ClientJobProxy;
import org.apache.seatunnel.engine.common.config.ConfigProvider;
import org.apache.seatunnel.engine.common.config.JobConfig;
import org.apache.seatunnel.engine.common.config.SeaTunnelConfig;
import org.apache.seatunnel.engine.common.job.JobStatus;
import org.apache.seatunnel.engine.server.SeaTunnelServerStarter;
import org.apache.seatunnel.engine.server.rest.RestConstant;

import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.instance.impl.HazelcastInstanceImpl;
import io.restassured.response.Response;
import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static io.restassured.RestAssured.given;
import static org.hamcrest.Matchers.notNullValue;

@Slf4j
public class CommittedMetricsIT {

    private static final String HOST = "http://localhost:";

    private ClientJobProxy streamJobProxy;

    private HazelcastInstanceImpl node1;

    private SeaTunnelClient engineClient;

    private SeaTunnelConfig seaTunnelConfig;

    @BeforeEach
    void beforeClass() throws Exception {
        String testClusterName = TestUtils.getClusterName("CommittedMetricsIT");
        seaTunnelConfig = ConfigProvider.locateAndGetSeaTunnelConfig();
        seaTunnelConfig.getHazelcastConfig().setClusterName(testClusterName);
        seaTunnelConfig.getEngineConfig().getHttpConfig().setPort(18080);
        seaTunnelConfig.getEngineConfig().getHttpConfig().setEnableDynamicPort(true);
        seaTunnelConfig.getEngineConfig().getHttpConfig().setPortRange(200);
        node1 = SeaTunnelServerStarter.createHazelcastInstance(seaTunnelConfig);

        ClientConfig clientConfig = ConfigProvider.locateAndGetClientConfig();
        clientConfig.setClusterName(testClusterName);
        engineClient = new SeaTunnelClient(clientConfig);
    }

    @Test
    public void testCommittedMetricsWithCheckpoint() throws Exception {
        String streamFilePath =
                TestUtils.getResource("stream_fake_multi_table_to_console_with_checkpoint.conf");
        JobConfig streamConf = new JobConfig();
        streamConf.setName("stream_fake_multi_table_to_console_with_checkpoint");
        ClientJobExecutionEnvironment streamJobExecutionEnv =
                engineClient.createExecutionContext(streamFilePath, streamConf, seaTunnelConfig);

        CompletableFuture.runAsync(
                () -> {
                    try {
                        streamJobProxy = streamJobExecutionEnv.execute();
                    } catch (ExecutionException e) {
                        throw new RuntimeException(e);
                    } catch (InterruptedException e) {
                        throw new RuntimeException(e);
                    }
                });

        Awaitility.await()
                .atMost(2, TimeUnit.MINUTES)
                .untilAsserted(
                        () -> {
                            Assertions.assertNotNull(streamJobProxy);
                            Assertions.assertEquals(
                                    JobStatus.RUNNING, streamJobProxy.getJobStatus());
                        });

        log.info("Job is running, job id: {}", streamJobProxy.getJobId());

        Thread.sleep(5000);

        Response responseBeforeCheckpoint =
                given().get(
                                HOST
                                        + node1.getCluster().getLocalMember().getAddress().getPort()
                                        + RestConstant.CONTEXT_PATH
                                        + RestConstant.REST_URL_JOB_INFO
                                        + "/"
                                        + streamJobProxy.getJobId());

        log.info("Metrics before checkpoint: {}", responseBeforeCheckpoint.prettyPrint());

        String writeCountBeforeCP = responseBeforeCheckpoint.path("metrics.SinkWriteCount");
        String committedCountBeforeCP = responseBeforeCheckpoint.path("metrics.SinkCommittedCount");

        long writeBeforeCP = Long.parseLong(writeCountBeforeCP);
        long committedBeforeCP = 0;
        if (committedCountBeforeCP != null) {
            committedBeforeCP = Long.parseLong(committedCountBeforeCP);
        }

        Assertions.assertTrue(writeBeforeCP > 0);
        Assertions.assertEquals(0, committedBeforeCP);

        log.info(
                "Before checkpoint - WriteCount: {}, CommittedCount: {}",
                writeBeforeCP,
                committedBeforeCP);

        Thread.sleep(8000);

        Response responseAfterFirstCheckpoint =
                given().get(
                                HOST
                                        + node1.getCluster().getLocalMember().getAddress().getPort()
                                        + RestConstant.CONTEXT_PATH
                                        + RestConstant.REST_URL_JOB_INFO
                                        + "/"
                                        + streamJobProxy.getJobId());

        log.info("Metrics after first checkpoint: {}", responseAfterFirstCheckpoint.prettyPrint());

        String sinkCommittedCount = responseAfterFirstCheckpoint.path("metrics.SinkCommittedCount");
        String sinkWriteCount = responseAfterFirstCheckpoint.path("metrics.SinkWriteCount");
        Assertions.assertNotNull(sinkCommittedCount);
        Assertions.assertNotNull(sinkWriteCount);

        long committedCountAfterFirstCP = Long.parseLong(sinkCommittedCount);
        long writeCountAfterFirstCP = Long.parseLong(sinkWriteCount);

        Assertions.assertTrue(committedCountAfterFirstCP > 0);
        Assertions.assertTrue(committedCountAfterFirstCP > committedBeforeCP);
        Assertions.assertTrue(committedCountAfterFirstCP <= writeCountAfterFirstCP);

        log.info(
                "After first checkpoint - WriteCount: {}, CommittedCount: {}, Uncommitted: {}",
                writeCountAfterFirstCP,
                committedCountAfterFirstCP,
                writeCountAfterFirstCP - committedCountAfterFirstCP);

        Thread.sleep(12000);

        Response responseFinal =
                given().get(
                                HOST
                                        + node1.getCluster().getLocalMember().getAddress().getPort()
                                        + RestConstant.CONTEXT_PATH
                                        + RestConstant.REST_URL_JOB_INFO
                                        + "/"
                                        + streamJobProxy.getJobId());

        log.info("Metrics after second checkpoint: {}", responseFinal.prettyPrint());

        responseFinal
                .then()
                .statusCode(200)
                .body("jobName", notNullValue())
                .body("jobStatus", notNullValue());

        String finalWriteCount = responseFinal.path("metrics.SinkWriteCount");
        String finalCommittedCount = responseFinal.path("metrics.SinkCommittedCount");
        String finalCommittedBytes = responseFinal.path("metrics.SinkCommittedBytes");
        String finalWriteBytes = responseFinal.path("metrics.SinkWriteBytes");

        long finalWrite = Long.parseLong(finalWriteCount);
        long finalCommitted = Long.parseLong(finalCommittedCount);
        long finalCommittedBytesVal = Long.parseLong(finalCommittedBytes);
        long finalWriteBytesVal = Long.parseLong(finalWriteBytes);

        Assertions.assertTrue(finalCommitted > committedCountAfterFirstCP);
        Assertions.assertTrue(finalCommitted <= finalWrite);
        Assertions.assertTrue(finalCommittedBytesVal > 0);
        Assertions.assertTrue(finalCommittedBytesVal <= finalWriteBytesVal);

        responseFinal
                .then()
                .body("metrics.SinkCommittedQPS", notNullValue())
                .body("metrics.SinkCommittedBytesPerSeconds", notNullValue());

        Double committedQPS = Double.parseDouble(responseFinal.path("metrics.SinkCommittedQPS"));
        Double committedBytesPerSec =
                Double.parseDouble(responseFinal.path("metrics.SinkCommittedBytesPerSeconds"));
        Assertions.assertTrue(committedQPS > 0);
        Assertions.assertTrue(committedBytesPerSec > 0);

        String table1CommittedCount =
                responseFinal.path("metrics.TableSinkCommittedCount.'fake.table1'");
        String table2CommittedCount =
                responseFinal.path("metrics.TableSinkCommittedCount.'fake.public.table2'");
        Assertions.assertNotNull(table1CommittedCount);
        Assertions.assertNotNull(table2CommittedCount);

        long table1Committed = Long.parseLong(table1CommittedCount);
        long table2Committed = Long.parseLong(table2CommittedCount);
        Assertions.assertTrue(table1Committed > 0);
        Assertions.assertTrue(table2Committed > 0);

        Assertions.assertEquals(finalCommitted, table1Committed + table2Committed);

        String table1CommittedBytes =
                responseFinal.path("metrics.TableSinkCommittedBytes.'fake.table1'");
        String table2CommittedBytes =
                responseFinal.path("metrics.TableSinkCommittedBytes.'fake.public.table2'");
        Assertions.assertNotNull(table1CommittedBytes);
        Assertions.assertNotNull(table2CommittedBytes);

        Assertions.assertTrue(Long.parseLong(table1CommittedBytes) > 0);
        Assertions.assertTrue(Long.parseLong(table2CommittedBytes) > 0);

        Double table1CommittedQPS =
                Double.parseDouble(
                        responseFinal.path("metrics.TableSinkCommittedQPS.'fake.table1'"));
        Double table2CommittedQPS =
                Double.parseDouble(
                        responseFinal.path("metrics.TableSinkCommittedQPS.'fake.public.table2'"));
        Assertions.assertTrue(table1CommittedQPS > 0);
        Assertions.assertTrue(table2CommittedQPS > 0);

        Double table1CommittedBytesPerSec =
                Double.parseDouble(
                        responseFinal.path(
                                "metrics.TableSinkCommittedBytesPerSeconds.'fake.table1'"));
        Double table2CommittedBytesPerSec =
                Double.parseDouble(
                        responseFinal.path(
                                "metrics.TableSinkCommittedBytesPerSeconds.'fake.public.table2'"));
        Assertions.assertTrue(table1CommittedBytesPerSec > 0);
        Assertions.assertTrue(table2CommittedBytesPerSec > 0);

        log.info("All committed metrics assertions passed");
        log.info(
                "Final summary - WriteCount: {}, CommittedCount: {}, Uncommitted: {}",
                finalWrite,
                finalCommitted,
                finalWrite - finalCommitted);

        streamJobProxy.cancelJob();

        Awaitility.await()
                .atMost(1, TimeUnit.MINUTES)
                .untilAsserted(
                        () ->
                                Assertions.assertEquals(
                                        JobStatus.CANCELED, streamJobProxy.getJobStatus()));

        log.info("testCommittedMetricsWithCheckpoint completed successfully");
    }

    @AfterEach
    void afterClass() {
        if (engineClient != null) {
            engineClient.close();
        }

        if (node1 != null) {
            node1.shutdown();
        }
    }
}
