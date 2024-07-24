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
import org.apache.seatunnel.engine.common.utils.PassiveCompletableFuture;
import org.apache.seatunnel.engine.core.job.JobResult;
import org.apache.seatunnel.engine.core.job.JobStatus;
import org.apache.seatunnel.engine.server.SeaTunnelServerStarter;

import org.awaitility.Awaitility;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.instance.impl.HazelcastInstanceImpl;
import lombok.extern.slf4j.Slf4j;

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

@Slf4j
public class ClusterIT {

    @Test
    public void getClusterHealthMetrics() {
        HazelcastInstanceImpl node1 = null;
        HazelcastInstanceImpl node2 = null;
        SeaTunnelClient engineClient = null;

        String testClusterName = "Test_getClusterHealthMetrics";

        SeaTunnelConfig seaTunnelConfig = ConfigProvider.locateAndGetSeaTunnelConfig();
        seaTunnelConfig
                .getHazelcastConfig()
                .setClusterName(TestUtils.getClusterName(testClusterName));

        try {
            node1 = SeaTunnelServerStarter.createHazelcastInstance(seaTunnelConfig);
            node2 = SeaTunnelServerStarter.createHazelcastInstance(seaTunnelConfig);
            HazelcastInstanceImpl finalNode = node1;
            Awaitility.await()
                    .atMost(10000, TimeUnit.MILLISECONDS)
                    .untilAsserted(
                            () ->
                                    Assertions.assertEquals(
                                            2, finalNode.getCluster().getMembers().size()));

            ClientConfig clientConfig = ConfigProvider.locateAndGetClientConfig();
            clientConfig.setClusterName(TestUtils.getClusterName(testClusterName));
            engineClient = new SeaTunnelClient(clientConfig);

            Map<String, String> clusterHealthMetrics = engineClient.getClusterHealthMetrics();
            log.info(
                    "=====================================cluster metrics==================================================");
            for (Map.Entry<String, String> entry : clusterHealthMetrics.entrySet()) {
                log.info(entry.getKey());
                log.info(entry.getValue());
                log.info(
                        "======================================================================================================");
            }
            Assertions.assertEquals(2, clusterHealthMetrics.size());

        } finally {
            if (engineClient != null) {
                engineClient.close();
            }

            if (node1 != null) {
                node1.shutdown();
            }

            if (node2 != null) {
                node2.shutdown();
            }
        }
    }

    @Test
    public void testTaskGroupErrorMsgLost() throws Exception {
        HazelcastInstanceImpl node1 = null;
        SeaTunnelClient engineClient = null;

        String testClusterName = "Test_TaskGroupErrorMsgLost";

        SeaTunnelConfig seaTunnelConfig = ConfigProvider.locateAndGetSeaTunnelConfig();
        seaTunnelConfig
                .getHazelcastConfig()
                .setClusterName(TestUtils.getClusterName(testClusterName));
        seaTunnelConfig.getEngineConfig().setClassloaderCacheMode(true);

        try {
            node1 = SeaTunnelServerStarter.createHazelcastInstance(seaTunnelConfig);
            HazelcastInstanceImpl finalNode = node1;
            Awaitility.await()
                    .atMost(10000, TimeUnit.MILLISECONDS)
                    .untilAsserted(
                            () ->
                                    Assertions.assertEquals(
                                            1, finalNode.getCluster().getMembers().size()));

            ClientConfig clientConfig = ConfigProvider.locateAndGetClientConfig();
            clientConfig.setClusterName(TestUtils.getClusterName(testClusterName));
            engineClient = new SeaTunnelClient(clientConfig);

            String filePath =
                    TestUtils.getResource("stream_fake_to_inmemory_with_runtime_list.conf");
            JobConfig jobConfig = new JobConfig();
            jobConfig.setName(testClusterName);
            ClientJobExecutionEnvironment jobExecutionEnv =
                    engineClient.createExecutionContext(filePath, jobConfig, seaTunnelConfig);

            final ClientJobProxy clientJobProxy = jobExecutionEnv.execute();

            CompletableFuture<PassiveCompletableFuture<JobResult>> objectCompletableFuture =
                    CompletableFuture.supplyAsync(clientJobProxy::doWaitForJobComplete);

            Awaitility.await()
                    .atMost(120000, TimeUnit.MILLISECONDS)
                    .untilAsserted(
                            () -> {
                                Thread.sleep(2000);
                                Assertions.assertTrue(objectCompletableFuture.isDone());

                                PassiveCompletableFuture<JobResult>
                                        jobResultPassiveCompletableFuture =
                                                objectCompletableFuture.get();
                                JobResult jobResult = jobResultPassiveCompletableFuture.get();
                                Assertions.assertEquals(JobStatus.FAILED, jobResult.getStatus());
                                Assertions.assertTrue(
                                        jobResult.getError().contains("runtime error 4"));
                            });

        } finally {
            if (engineClient != null) {
                engineClient.close();
            }

            if (node1 != null) {
                node1.shutdown();
            }
        }
    }
}
