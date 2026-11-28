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
import org.apache.seatunnel.engine.client.job.ClientJobExecutionEnvironment;
import org.apache.seatunnel.engine.client.job.ClientJobProxy;
import org.apache.seatunnel.engine.common.config.ConfigProvider;
import org.apache.seatunnel.engine.common.config.JobConfig;
import org.apache.seatunnel.engine.common.config.SeaTunnelConfig;
import org.apache.seatunnel.engine.common.job.JobStatus;
import org.apache.seatunnel.engine.server.SeaTunnelServerStarter;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.instance.impl.HazelcastInstanceImpl;

import java.util.Arrays;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static org.awaitility.Awaitility.await;

/**
 * E2E coverage for job env options propagation on the Zeta path: the whole env block must reach the
 * source, transform and sink factory contexts of a real (in-process) Zeta cluster.
 *
 * <p>The probe value rendered by {@link EnvProbeSupport} reads one engine-declared key, one
 * user-defined top-level key and one nested {@code custom_parameters} key, so these tests prove the
 * entire env block is forwarded. The transform additionally copies the value into the serializable
 * transform object, proving it survives job serialization through to runtime.
 */
public class EnvOptionsPropagationIT {

    private static final String CLUSTER_NAME = TestUtils.getClusterName("EnvOptionsPropagationIT");

    private static final String EXPECTED_PROBE = "BATCH|env-value|nested-value";

    private HazelcastInstanceImpl hazelcastInstance;

    private SeaTunnelConfig seatunnelConfig;

    @BeforeEach
    public void beforeClass() {
        EnvFakeSourceFactory.reset();
        EnvProbeSinkFactory.reset();
        seatunnelConfig = ConfigProvider.locateAndGetSeaTunnelConfig();
        seatunnelConfig.getHazelcastConfig().setClusterName(CLUSTER_NAME);
        hazelcastInstance = SeaTunnelServerStarter.createHazelcastInstance(seatunnelConfig);
    }

    @AfterEach
    public void afterClass() {
        if (hazelcastInstance != null) {
            hazelcastInstance.shutdown();
        }
    }

    @Test
    public void testSourceFactoryReceivesEnvOptions() throws Exception {
        runJobAndWaitForFinished("env_options_source_probe.conf");

        Assertions.assertEquals(EXPECTED_PROBE, EnvFakeSourceFactory.capturedEnv());
    }

    @Test
    public void testTransformFactoryReceivesEnvOptionsAndValueFlowsToData() throws Exception {
        // The Assert sink verifies env_probe == BATCH|env-value|nested-value on every row, which
        // proves the value was captured at factory time AND carried into the runtime transform.
        runJobAndWaitForFinished("env_options_transform_probe.conf");
    }

    @Test
    public void testSinkFactoryReceivesEnvOptionsAndRows() throws Exception {
        runJobAndWaitForFinished("env_options_sink_probe.conf");

        Assertions.assertEquals(EXPECTED_PROBE, EnvProbeSinkFactory.capturedEnv());
        Assertions.assertEquals(1, EnvProbeSinkFactory.receivedRows().size());
        Assertions.assertEquals(
                Arrays.asList(1, "hello"), EnvProbeSinkFactory.receivedRows().get(0));
    }

    /**
     * Real-scenario pipeline: source, transform and sink factories of one job all read env, and the
     * env value is materialized into the actual data that reaches the sink. The final row checked
     * at the sink carries the original source data plus the env probe appended by the transform,
     * proving env flowed through factory -> serialized plugin -> runtime -> data.
     */
    @Test
    public void testFullPipelineAllFactoriesReadEnvInOneJob() throws Exception {
        runJobAndWaitForFinished("env_options_full_pipeline.conf");

        Assertions.assertEquals(EXPECTED_PROBE, EnvFakeSourceFactory.capturedEnv());
        Assertions.assertEquals(EXPECTED_PROBE, EnvProbeSinkFactory.capturedEnv());
        Assertions.assertEquals(1, EnvProbeSinkFactory.receivedRows().size());
        Assertions.assertEquals(
                Arrays.asList(1, "hello", EXPECTED_PROBE),
                EnvProbeSinkFactory.receivedRows().get(0));
    }

    private void runJobAndWaitForFinished(String confFile) throws Exception {
        Common.setDeployMode(DeployMode.CLIENT);
        String filePath = TestUtils.getResource(confFile);
        JobConfig jobConfig = new JobConfig();
        jobConfig.setName(confFile);
        ClientConfig clientConfig = ConfigProvider.locateAndGetClientConfig();
        clientConfig.setClusterName(CLUSTER_NAME);
        try (SeaTunnelClient engineClient = new SeaTunnelClient(clientConfig)) {
            ClientJobExecutionEnvironment jobExecutionEnv =
                    engineClient.createExecutionContext(filePath, jobConfig, seatunnelConfig);
            ClientJobProxy clientJobProxy = jobExecutionEnv.execute();
            CompletableFuture<JobStatus> future =
                    CompletableFuture.supplyAsync(clientJobProxy::waitForJobComplete);
            await().atMost(120000, TimeUnit.MILLISECONDS)
                    .untilAsserted(() -> Assertions.assertEquals(JobStatus.FINISHED, future.get()));
        }
    }
}
