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

package org.apache.seatunnel.engine.e2e.engine;

import org.apache.seatunnel.common.config.Common;
import org.apache.seatunnel.common.config.DeployMode;
import org.apache.seatunnel.engine.client.SeaTunnelClient;
import org.apache.seatunnel.engine.client.job.ClientJobProxy;
import org.apache.seatunnel.engine.client.job.JobExecutionEnvironment;
import org.apache.seatunnel.engine.common.config.ConfigProvider;
import org.apache.seatunnel.engine.common.config.JobConfig;
import org.apache.seatunnel.engine.common.config.SeaTunnelClientConfig;
import org.apache.seatunnel.engine.common.config.SeaTunnelConfig;
import org.apache.seatunnel.engine.core.job.JobStatus;
import org.apache.seatunnel.engine.e2e.TestUtils;
import org.apache.seatunnel.engine.server.SeaTunnelNodeContext;

import com.google.common.collect.Lists;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.instance.impl.HazelcastInstanceFactory;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CompletableFuture;

public class JobExecutionIT {
    private static final Logger LOGGER = LoggerFactory.getLogger(JobExecutionIT.class);

    @BeforeClass
    public static void beforeClass() throws Exception {
        SeaTunnelConfig seaTunnelConfig = ConfigProvider.locateAndGetSeaTunnelConfig();
        HazelcastInstanceFactory.newHazelcastInstance(seaTunnelConfig.getHazelcastConfig(),
            Thread.currentThread().getName(),
            new SeaTunnelNodeContext(ConfigProvider.locateAndGetSeaTunnelConfig()));
    }

    @Test
    public void testSayHello() {
        SeaTunnelClientConfig seaTunnelClientConfig = new SeaTunnelClientConfig();
        seaTunnelClientConfig.getNetworkConfig().setAddresses(Lists.newArrayList("localhost:5801"));
        SeaTunnelClient engineClient = new SeaTunnelClient(seaTunnelClientConfig);

        String msg = "Hello world";
        String s = engineClient.printMessageToMaster(msg);
        Assert.assertEquals(msg, s);
    }

    @Test
    public void testExecuteJob() {
        TestUtils.initPluginDir();
        Common.setDeployMode(DeployMode.CLIENT);
        String filePath = TestUtils.getResource("/fakesource_to_file_complex.conf");
        JobConfig jobConfig = new JobConfig();
        jobConfig.setName("fake_to_file");

        ClientConfig clientConfig = ConfigProvider.locateAndGetClientConfig();
        SeaTunnelClient engineClient = new SeaTunnelClient(clientConfig);
        JobExecutionEnvironment jobExecutionEnv = engineClient.createExecutionContext(filePath, jobConfig);

        ClientJobProxy clientJobProxy = null;
        try {
            clientJobProxy = jobExecutionEnv.execute();
            JobStatus jobStatus = clientJobProxy.waitForJobComplete();
            Assert.assertEquals(JobStatus.FINISHED, jobStatus);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    public void cancelJobTest() {
        TestUtils.initPluginDir();
        Common.setDeployMode(DeployMode.CLIENT);
        String filePath = TestUtils.getResource("/streaming_fakesource_to_file_complex.conf");
        JobConfig jobConfig = new JobConfig();
        jobConfig.setName("fake_to_file");

        ClientConfig clientConfig = ConfigProvider.locateAndGetClientConfig();
        SeaTunnelClient engineClient = new SeaTunnelClient(clientConfig);
        JobExecutionEnvironment jobExecutionEnv = engineClient.createExecutionContext(filePath, jobConfig);

        ClientJobProxy clientJobProxy = null;
        try {
            clientJobProxy = jobExecutionEnv.execute();
            JobStatus jobStatus1 = clientJobProxy.getJobStatus();
            Assert.assertFalse(jobStatus1.isEndState());
            ClientJobProxy finalClientJobProxy = clientJobProxy;
            CompletableFuture<Object> objectCompletableFuture = CompletableFuture.supplyAsync(() -> {
                JobStatus jobStatus = finalClientJobProxy.waitForJobComplete();
                Assert.assertEquals(JobStatus.CANCELED, jobStatus);
                return null;
            });
            Thread.sleep(500);
            clientJobProxy.cancelJob();
            objectCompletableFuture.join();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
