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

package org.apache.seatunnel.engine.client;

import org.apache.seatunnel.common.config.Common;
import org.apache.seatunnel.common.config.DeployMode;
import org.apache.seatunnel.common.constants.JobMode;
import org.apache.seatunnel.engine.client.job.JobExecutionEnvironment;
import org.apache.seatunnel.engine.client.job.JobProxy;
import org.apache.seatunnel.engine.common.config.ConfigProvider;
import org.apache.seatunnel.engine.common.config.JobConfig;
import org.apache.seatunnel.engine.common.config.SeaTunnelConfig;
import org.apache.seatunnel.engine.core.job.JobStatus;
import org.apache.seatunnel.engine.server.SeaTunnelNodeContext;

import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.instance.impl.HazelcastInstanceFactory;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.util.concurrent.ExecutionException;

@SuppressWarnings("checkstyle:MagicNumber")
@RunWith(JUnit4.class)
public class SeaTunnelClientTest {

    private HazelcastInstance instance;

    @Before
    public void beforeClass() throws Exception {
        SeaTunnelConfig seaTunnelConfig = ConfigProvider.locateAndGetSeaTunnelConfig();
        instance = HazelcastInstanceFactory.newHazelcastInstance(seaTunnelConfig.getHazelcastConfig(),
            Thread.currentThread().getName(),
            new SeaTunnelNodeContext(ConfigProvider.locateAndGetSeaTunnelConfig()));
    }

    @Test
    public void testSayHello() {
        ClientConfig clientConfig = ConfigProvider.locateAndGetClientConfig();
        SeaTunnelClient engineClient = new SeaTunnelClient(clientConfig);

        String msg = "Hello world";
        String s = engineClient.printMessageToMaster(msg);
        Assert.assertEquals(msg, s);
    }

    @Test
    public void testExecuteJob() {
        Common.setDeployMode(DeployMode.CLIENT);
        String filePath = TestUtils.getResource("/fakesource_to_file_complex.conf");
        JobConfig jobConfig = new JobConfig();
        jobConfig.setMode(JobMode.BATCH);
        jobConfig.setName("fake_to_file");

        ClientConfig clientConfig = ConfigProvider.locateAndGetClientConfig();
        SeaTunnelClient engineClient = new SeaTunnelClient(clientConfig);
        JobExecutionEnvironment jobExecutionEnv = engineClient.createExecutionContext(filePath, jobConfig);

        JobProxy jobProxy = null;
        try {
            jobProxy = jobExecutionEnv.execute();
            JobStatus jobStatus = jobProxy.waitForJobComplete();
            Assert.assertEquals(JobStatus.FINISHED, jobStatus);
        } catch (ExecutionException | InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    @After
    public void after() {
        instance.shutdown();
    }
}
