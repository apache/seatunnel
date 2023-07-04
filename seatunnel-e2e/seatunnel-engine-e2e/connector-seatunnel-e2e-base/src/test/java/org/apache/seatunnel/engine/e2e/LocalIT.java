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

import org.apache.seatunnel.common.config.Common;
import org.apache.seatunnel.common.config.DeployMode;
import org.apache.seatunnel.engine.client.SeaTunnelClient;
import org.apache.seatunnel.engine.client.job.ClientJobProxy;
import org.apache.seatunnel.engine.client.job.JobExecutionEnvironment;
import org.apache.seatunnel.engine.common.config.ConfigProvider;
import org.apache.seatunnel.engine.common.config.JobConfig;
import org.apache.seatunnel.engine.common.config.SeaTunnelConfig;
import org.apache.seatunnel.engine.core.job.JobStatus;
import org.apache.seatunnel.engine.server.SeaTunnelNodeContext;

import org.awaitility.Awaitility;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.testcontainers.shaded.org.apache.commons.lang3.tuple.ImmutableTriple;

import com.google.common.collect.Lists;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.cluster.Address;
import com.hazelcast.cluster.Member;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.instance.impl.HazelcastInstanceFactory;
import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

@Slf4j
public class LocalIT {

    public static final String TEST_TEMPLATE_FILE_NAME = "stream_fakesource_to_console.conf";

    @Test
    public void testSubmitIndependentJobByLocalSever()
            throws ExecutionException, InterruptedException {
        ImmutableTriple<HazelcastInstance, SeaTunnelClient, ClientJobProxy> jobResult =
                submitJobByLocalSever();
        try {
            Awaitility.await()
                    .atMost(6, TimeUnit.MINUTES)
                    .untilAsserted(
                            () -> {
                                Thread.sleep(2000);
                                Assertions.assertTrue(
                                        JobStatus.RUNNING.equals(
                                                jobResult.getRight().getJobStatus()));
                            });
        } finally {
            close(jobResult);
        }
    }

    @Test
    public void testSubmitTwoIndependentJobByLocalSever()
            throws ExecutionException, InterruptedException {
        ImmutableTriple<HazelcastInstance, SeaTunnelClient, ClientJobProxy> jobResult1 =
                submitJobByLocalSever();
        ImmutableTriple<HazelcastInstance, SeaTunnelClient, ClientJobProxy> jobResult2 =
                submitJobByLocalSever();
        try {
            Awaitility.await()
                    .atMost(6, TimeUnit.MINUTES)
                    .untilAsserted(
                            () -> {
                                Thread.sleep(2000);
                                Assertions.assertTrue(
                                        JobStatus.RUNNING.equals(
                                                jobResult1.getRight().getJobStatus()));
                            });

            Awaitility.await()
                    .atMost(6, TimeUnit.MINUTES)
                    .untilAsserted(
                            () -> {
                                Thread.sleep(2000);
                                Assertions.assertTrue(
                                        JobStatus.RUNNING.equals(
                                                jobResult2.getRight().getJobStatus()));
                            });

            String serverAddress1 =
                    jobResult1.getMiddle().getClusterHealthMetrics().keySet().stream()
                            .findFirst()
                            .get();
            String serverAddress2 =
                    jobResult2.getMiddle().getClusterHealthMetrics().keySet().stream()
                            .findFirst()
                            .get();
            Assertions.assertTrue(!serverAddress1.equals(serverAddress2));
        } finally {
            close(jobResult1);
            close(jobResult2);
        }
    }

    public ImmutableTriple<HazelcastInstance, SeaTunnelClient, ClientJobProxy>
            submitJobByLocalSever() throws ExecutionException, InterruptedException {
        String testCaseName = "submitJobByLocalSever";
        String localServerName = TestUtils.creatRandomClusterName("Test_submitLocalJob");
        SeaTunnelConfig seaTunnelConfig = ConfigProvider.locateAndGetSeaTunnelConfig();
        seaTunnelConfig.getHazelcastConfig().setClusterName(localServerName);

        seaTunnelConfig.getHazelcastConfig().getNetworkConfig().setPortAutoIncrement(true);
        HazelcastInstance node =
                HazelcastInstanceFactory.newHazelcastInstance(
                        seaTunnelConfig.getHazelcastConfig(),
                        localServerName,
                        new SeaTunnelNodeContext(seaTunnelConfig));

        ClientConfig clientConfig = ConfigProvider.locateAndGetClientConfig();
        clientConfig.setClusterName(node.getConfig().getClusterName());

        Address serverAddress = ((Member) node.getLocalEndpoint()).getAddress();
        String connectAddress = serverAddress.getHost() + ":" + serverAddress.getPort();
        clientConfig.getNetworkConfig().setAddresses(Lists.newArrayList(connectAddress));

        Common.setDeployMode(DeployMode.CLIENT);
        SeaTunnelClient engineClient = new SeaTunnelClient(clientConfig);

        String configFile = TestUtils.getResource(TEST_TEMPLATE_FILE_NAME);
        JobConfig jobConfig = new JobConfig();
        jobConfig.setName(testCaseName);
        JobExecutionEnvironment jobExecutionEnv =
                engineClient.createExecutionContext(configFile, jobConfig);
        ClientJobProxy clientJobProxy = jobExecutionEnv.execute();

        return new ImmutableTriple<>(node, engineClient, clientJobProxy);
    }

    public void close(
            ImmutableTriple<HazelcastInstance, SeaTunnelClient, ClientJobProxy> jobResult) {
        if (null != jobResult.getMiddle()) {
            jobResult.getMiddle().shutdown();
        }

        if (null != jobResult.getLeft()) {
            jobResult.getLeft().shutdown();
        }
    }
}
