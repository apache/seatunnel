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
import org.apache.seatunnel.engine.core.job.JobStatus;
import org.apache.seatunnel.engine.server.SeaTunnelServerStarter;

import com.hazelcast.client.config.ClientConfig;
import lombok.extern.slf4j.Slf4j;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

@Slf4j
public class JobExecutionIT {
    @BeforeAll
    public static void beforeClass() throws Exception {
        SeaTunnelServerStarter.createHazelcastInstance(TestUtils.getClusterName("JobExecutionIT"));
    }

    @Test
    public void testSayHello() {
        ClientConfig clientConfig = ConfigProvider.locateAndGetClientConfig();
        clientConfig.setClusterName(TestUtils.getClusterName("JobExecutionIT"));
        SeaTunnelClient engineClient = new SeaTunnelClient(clientConfig);

        String msg = "Hello world";
        String s = engineClient.printMessageToMaster(msg);
        Assertions.assertEquals(msg, s);
    }

    @Test
    public void testExecuteJob() throws Exception {
        Common.setDeployMode(DeployMode.CLIENT);
        String filePath = TestUtils.getResource("batch_fakesource_to_file.conf");
        JobConfig jobConfig = new JobConfig();
        jobConfig.setName("fake_to_file");

        ClientConfig clientConfig = ConfigProvider.locateAndGetClientConfig();
        clientConfig.setClusterName(TestUtils.getClusterName("JobExecutionIT"));
        SeaTunnelClient engineClient = new SeaTunnelClient(clientConfig);
        JobExecutionEnvironment jobExecutionEnv =
            engineClient.createExecutionContext(filePath, jobConfig);

        final ClientJobProxy clientJobProxy = jobExecutionEnv.execute();

        CompletableFuture<JobStatus> objectCompletableFuture = CompletableFuture.supplyAsync(() -> {
            return clientJobProxy.waitForJobComplete();
        });

        Awaitility.await().atMost(20000, TimeUnit.MILLISECONDS)
            .untilAsserted(() -> Assertions.assertTrue(
                objectCompletableFuture.isDone() && JobStatus.FINISHED.equals(objectCompletableFuture.get())));

    }

    @Test
    public void cancelJobTest() throws Exception {
        Common.setDeployMode(DeployMode.CLIENT);
        String filePath = TestUtils.getResource("streaming_fakesource_to_file_complex.conf");
        JobConfig jobConfig = new JobConfig();
        jobConfig.setName("fake_to_file");

        ClientConfig clientConfig = ConfigProvider.locateAndGetClientConfig();
        clientConfig.setClusterName(TestUtils.getClusterName("JobExecutionIT"));
        SeaTunnelClient engineClient = new SeaTunnelClient(clientConfig);
        JobExecutionEnvironment jobExecutionEnv =
            engineClient.createExecutionContext(filePath, jobConfig);

        final ClientJobProxy clientJobProxy = jobExecutionEnv.execute();
        JobStatus jobStatus1 = clientJobProxy.getJobStatus();
        Assertions.assertFalse(jobStatus1.isEndState());
        ClientJobProxy finalClientJobProxy = clientJobProxy;
        CompletableFuture<JobStatus> objectCompletableFuture = CompletableFuture.supplyAsync(() -> {
            return finalClientJobProxy.waitForJobComplete();
        });
        Thread.sleep(1000);
        clientJobProxy.cancelJob();

        Awaitility.await().atMost(20000, TimeUnit.MILLISECONDS)
            .untilAsserted(() -> Assertions.assertTrue(
                objectCompletableFuture.isDone() && JobStatus.CANCELED.equals(objectCompletableFuture.get())));

    }
}
