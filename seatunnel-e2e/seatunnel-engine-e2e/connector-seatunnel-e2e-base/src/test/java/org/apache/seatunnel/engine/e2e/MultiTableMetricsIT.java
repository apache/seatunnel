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
import org.apache.seatunnel.engine.core.job.JobStatus;
import org.apache.seatunnel.engine.server.SeaTunnelServerStarter;
import org.apache.seatunnel.engine.server.rest.RestConstant;

import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.instance.impl.HazelcastInstanceImpl;

import java.util.Collections;
import java.util.concurrent.TimeUnit;

import static io.restassured.RestAssured.given;
import static org.hamcrest.Matchers.equalTo;

public class MultiTableMetricsIT {

    private static final String HOST = "http://localhost:";

    private static ClientJobProxy batchJobProxy;

    private static HazelcastInstanceImpl node1;

    private static SeaTunnelClient engineClient;

    @BeforeEach
    void beforeClass() throws Exception {
        String testClusterName = TestUtils.getClusterName("RestApiIT");
        SeaTunnelConfig seaTunnelConfig = ConfigProvider.locateAndGetSeaTunnelConfig();
        seaTunnelConfig.getHazelcastConfig().setClusterName(testClusterName);
        node1 = SeaTunnelServerStarter.createHazelcastInstance(seaTunnelConfig);

        ClientConfig clientConfig = ConfigProvider.locateAndGetClientConfig();
        clientConfig.setClusterName(testClusterName);
        engineClient = new SeaTunnelClient(clientConfig);

        String batchFilePath = TestUtils.getResource("batch_fake_multi_table_to_console.conf");
        JobConfig batchConf = new JobConfig();
        batchConf.setName("batch_fake_multi_table_to_console");
        ClientJobExecutionEnvironment batchJobExecutionEnv =
                engineClient.createExecutionContext(batchFilePath, batchConf, seaTunnelConfig);
        batchJobProxy = batchJobExecutionEnv.execute();
        Awaitility.await()
                .atMost(2, TimeUnit.MINUTES)
                .untilAsserted(
                        () ->
                                Assertions.assertEquals(
                                        JobStatus.FINISHED, batchJobProxy.getJobStatus()));
    }

    @Test
    public void multiTableMetrics() {
        Collections.singletonList(node1)
                .forEach(
                        instance -> {
                            given().get(
                                            HOST
                                                    + instance.getCluster()
                                                            .getLocalMember()
                                                            .getAddress()
                                                            .getPort()
                                                    + RestConstant.JOB_INFO_URL
                                                    + "/"
                                                    + batchJobProxy.getJobId())
                                    .then()
                                    .statusCode(200)
                                    .body("jobName", equalTo("batch_fake_multi_table_to_console"))
                                    .body("jobStatus", equalTo("FINISHED"))
                                    .body("metrics.SourceReceivedCount", equalTo("50"))
                                    .body("metrics.SinkWriteCount", equalTo("50"))
                                    .body(
                                            "metrics.TableSourceReceivedCount.'fake.table1'",
                                            equalTo("20"))
                                    .body(
                                            "metrics.TableSourceReceivedCount.'fake.public.table2'",
                                            equalTo("30"))
                                    .body(
                                            "metrics.TableSinkWriteCount.'fake.table1'",
                                            equalTo("20"))
                                    .body(
                                            "metrics.TableSinkWriteCount.'fake.public.table2'",
                                            equalTo("30"));
                        });
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
