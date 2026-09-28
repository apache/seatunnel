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

import org.awaitility.Awaitility;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.instance.impl.HazelcastInstanceImpl;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

public class SeaTunnelSlotIT {
    @Test
    public void testSlotNotEnough() throws Exception {
        HazelcastInstanceImpl node1 = null;
        SeaTunnelClient engineClient = null;

        try {
            String testClusterName = "testSlotNotEnough";
            SeaTunnelConfig seaTunnelConfig = ConfigProvider.locateAndGetSeaTunnelConfig();
            seaTunnelConfig.getHazelcastConfig().setClusterName(testClusterName);
            seaTunnelConfig.getEngineConfig().getHttpConfig().setEnabled(false);
            // slot num is 3
            seaTunnelConfig.getEngineConfig().getSlotServiceConfig().setDynamicSlot(false);
            seaTunnelConfig.getEngineConfig().getSlotServiceConfig().setSlotNum(3);

            node1 = SeaTunnelServerStarter.createHazelcastInstance(seaTunnelConfig);

            // client config
            Common.setDeployMode(DeployMode.CLIENT);
            String filePath = TestUtils.getResource("batch_slot_not_enough.conf");
            JobConfig jobConfig = new JobConfig();
            jobConfig.setName(testClusterName);

            ClientConfig clientConfig = ConfigProvider.locateAndGetClientConfig();
            clientConfig.setClusterName(testClusterName);
            engineClient = new SeaTunnelClient(clientConfig);
            ClientJobExecutionEnvironment jobExecutionEnv =
                    engineClient.createExecutionContext(filePath, jobConfig, seaTunnelConfig);

            final ClientJobProxy clientJobProxy = jobExecutionEnv.execute();
            CompletableFuture<JobStatus> objectCompletableFuture =
                    CompletableFuture.supplyAsync(clientJobProxy::waitForJobComplete);
            Awaitility.await()
                    .atMost(300000, TimeUnit.MILLISECONDS)
                    .untilAsserted(
                            () -> {
                                Thread.sleep(2000);
                                Assertions.assertTrue(
                                        objectCompletableFuture.isDone()
                                                && JobStatus.FAILED.equals(
                                                        objectCompletableFuture.get()));
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

    @Test
    public void testSlotEnough() throws Exception {
        HazelcastInstanceImpl node1 = null;
        SeaTunnelClient engineClient = null;

        try {
            String testClusterName = "testSlotEnough";
            SeaTunnelConfig seaTunnelConfig = ConfigProvider.locateAndGetSeaTunnelConfig();
            seaTunnelConfig.getHazelcastConfig().setClusterName(testClusterName);
            seaTunnelConfig.getEngineConfig().getHttpConfig().setEnabled(false);
            // slot num is 10
            seaTunnelConfig.getEngineConfig().getSlotServiceConfig().setDynamicSlot(false);
            seaTunnelConfig.getEngineConfig().getSlotServiceConfig().setSlotNum(10);

            node1 = SeaTunnelServerStarter.createHazelcastInstance(seaTunnelConfig);

            // client config
            Common.setDeployMode(DeployMode.CLIENT);
            String filePath = TestUtils.getResource("batch_slot_not_enough.conf");
            JobConfig jobConfig = new JobConfig();
            jobConfig.setName(testClusterName);

            ClientConfig clientConfig = ConfigProvider.locateAndGetClientConfig();
            clientConfig.setClusterName(testClusterName);
            engineClient = new SeaTunnelClient(clientConfig);
            ClientJobExecutionEnvironment jobExecutionEnv =
                    engineClient.createExecutionContext(filePath, jobConfig, seaTunnelConfig);

            final ClientJobProxy clientJobProxy = jobExecutionEnv.execute();
            CompletableFuture<JobStatus> objectCompletableFuture =
                    CompletableFuture.supplyAsync(clientJobProxy::waitForJobComplete);
            Awaitility.await()
                    .atMost(300000, TimeUnit.MILLISECONDS)
                    .untilAsserted(
                            () -> {
                                Thread.sleep(2000);
                                Assertions.assertTrue(
                                        objectCompletableFuture.isDone()
                                                && JobStatus.FINISHED.equals(
                                                        objectCompletableFuture.get()));
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

    /**
     * Regression test for the slot-release bug fixed by <a
     * href="https://github.com/apache/seatunnel/pull/6763">#6763</a> ("[fix][zeta] fix can't
     * release resource issue"). Before that fix, when a pipeline needed more slots than were
     * available, the slots that had already been successfully granted before the shortfall was
     * detected were never released: {@code ResourceUtils#applyResourceForPipeline} joined every
     * per-task resource future, but a failed or missing future was silently dropped instead of
     * triggering release of the ones that did succeed, so {@code NoEnoughResourceException}
     * propagated with those slots still marked owned forever.
     *
     * <p>{@link #testSlotNotEnough()} above already proves the job reaches {@code FAILED} in this
     * scenario, but that assertion alone cannot tell a pre-fix leak apart from a regression: a job
     * with permanently-leaked slots still fails with the same status. The only accompanying test
     * the original fix shipped with ({@code FixSlotResourceTest#testNotEnoughResource} in
     * seatunnel-engine-server) asserts the release directly against a mocked, single-JVM {@code
     * ResourceManager} — it does not prove a real cluster's slot pool is actually usable again
     * afterward.
     *
     * <p>This test closes that gap functionally rather than by reaching into internal bookkeeping:
     * it drives the exact same undersized-cluster failure as {@link #testSlotNotEnough()} (slot-num
     * 3, a job that needs more), then submits a second, minimal single-slot job against the same
     * still-running cluster and asserts it reaches {@code FINISHED}. If the slots granted to the
     * first job were never released, the cluster would still show 0 free slots and the second job
     * would fail with the same {@code NoEnoughResourceException} instead.
     */
    @Test
    public void testSlotReleasedAfterNotEnoughResourceFailure() throws Exception {
        HazelcastInstanceImpl node1 = null;
        SeaTunnelClient engineClient = null;

        try {
            String testClusterName = "testSlotReleasedAfterNotEnoughResourceFailure";
            SeaTunnelConfig seaTunnelConfig = ConfigProvider.locateAndGetSeaTunnelConfig();
            seaTunnelConfig.getHazelcastConfig().setClusterName(testClusterName);
            seaTunnelConfig.getEngineConfig().getHttpConfig().setEnabled(false);
            // Same undersized pool as testSlotNotEnough: enough to grant some, not all, of the
            // first job's slots.
            seaTunnelConfig.getEngineConfig().getSlotServiceConfig().setDynamicSlot(false);
            seaTunnelConfig.getEngineConfig().getSlotServiceConfig().setSlotNum(3);

            node1 = SeaTunnelServerStarter.createHazelcastInstance(seaTunnelConfig);

            Common.setDeployMode(DeployMode.CLIENT);
            ClientConfig clientConfig = ConfigProvider.locateAndGetClientConfig();
            clientConfig.setClusterName(testClusterName);
            engineClient = new SeaTunnelClient(clientConfig);

            JobConfig oversizedJobConfig = new JobConfig();
            oversizedJobConfig.setName(testClusterName + "_oversized");
            ClientJobExecutionEnvironment oversizedJobEnv =
                    engineClient.createExecutionContext(
                            TestUtils.getResource("batch_slot_not_enough.conf"),
                            oversizedJobConfig,
                            seaTunnelConfig);
            ClientJobProxy oversizedJobProxy = oversizedJobEnv.execute();
            CompletableFuture<JobStatus> oversizedJobComplete =
                    CompletableFuture.supplyAsync(oversizedJobProxy::waitForJobComplete);
            Awaitility.await()
                    .atMost(300000, TimeUnit.MILLISECONDS)
                    .untilAsserted(
                            () -> {
                                Thread.sleep(2000);
                                Assertions.assertTrue(
                                        oversizedJobComplete.isDone()
                                                && JobStatus.FAILED.equals(
                                                        oversizedJobComplete.get()));
                            });

            // The oversized job's own resource shortfall must be fully resolved on the master
            // before this proves anything: submit a second, minimal job that needs far fewer
            // slots than the pool holds, against the same still-running cluster.
            JobConfig minimalJobConfig = new JobConfig();
            minimalJobConfig.setName(testClusterName + "_minimal");
            ClientJobExecutionEnvironment minimalJobEnv =
                    engineClient.createExecutionContext(
                            TestUtils.getResource("batch_fake_to_console_minimal_slot.conf"),
                            minimalJobConfig,
                            seaTunnelConfig);
            ClientJobProxy minimalJobProxy = minimalJobEnv.execute();
            CompletableFuture<JobStatus> minimalJobComplete =
                    CompletableFuture.supplyAsync(minimalJobProxy::waitForJobComplete);
            Awaitility.await()
                    .atMost(60000, TimeUnit.MILLISECONDS)
                    .untilAsserted(
                            () ->
                                    Assertions.assertTrue(
                                            minimalJobComplete.isDone()
                                                    && JobStatus.FINISHED.equals(
                                                            minimalJobComplete.get()),
                                            "Minimal job could not get a slot after the oversized "
                                                    + "job failed; the earlier failure's slots "
                                                    + "were likely never released"));
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
