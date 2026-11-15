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
import org.apache.seatunnel.engine.common.job.JobResult;
import org.apache.seatunnel.engine.common.job.JobStatus;
import org.apache.seatunnel.engine.server.SeaTunnelServer;
import org.apache.seatunnel.engine.server.SeaTunnelServerStarter;
import org.apache.seatunnel.engine.server.resourcemanager.ResourceManager;
import org.apache.seatunnel.engine.server.resourcemanager.resource.ResourceProfile;
import org.apache.seatunnel.engine.server.resourcemanager.worker.WorkerProfile;

import org.awaitility.Awaitility;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.cluster.Address;
import com.hazelcast.instance.impl.HazelcastInstanceImpl;
import com.hazelcast.spi.impl.NodeEngineImpl;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

/**
 * Documents today's actual dynamic-slot resource accounting on a single Zeta worker: in the default
 * {@code dynamic-slot: true} mode, the built-in slot capacity check never rejects a request on
 * resource grounds, so a single worker hands out a slot for every requested task group with no
 * built-in bound other than real JVM/OS limits.
 *
 * <p>Root cause, confirmed by reading current HEAD source (not the design doc, which may be stale):
 *
 * <ul>
 *   <li>{@code ResourceUtils#applyResourceForTask} requests a slot with {@code new
 *       ResourceProfile()} for every real task (see the {@code // TODO custom resource size}
 *       comment there), which is always 0 CPU / 0 heap memory (see {@code ResourceProfile}'s no-arg
 *       constructor).
 *   <li>{@code DefaultSlotService#getNodeResource} always advertises the worker's own CPU capacity
 *       as {@code CPU.of(0)} (only heap memory is a real value), and {@code
 *       DefaultSlotService#requestSlot} in dynamic-slot mode grants a slot whenever {@code
 *       unassignedResource.enoughThan(profile)}, which is trivially true for a 0/0 profile.
 *   <li>Granting a slot then runs {@code unassignedResource.subtract(profile)} with that same 0/0
 *       profile, which is a mathematical no-op ({@code ResourceProfile#subtract}), so the tracked
 *       unassigned resource never actually decreases no matter how many slots have already been
 *       handed out. Request number 1000 has exactly the same chance of being granted as request
 *       number 1.
 * </ul>
 *
 * <p>This is a missing capacity *safety net*, not a correctness bug: this test also confirms the
 * job still completes normally at this concurrency, so there is no data-processing regression, only
 * an absent resource guard rail. Existing coverage ({@code SeaTunnelSlotIT}) only exercises the
 * fixed-slot path (3 or 10 slots); no existing test exercises dynamic-slot mode under concurrency
 * anywhere close to real capacity limits.
 */
public class DynamicSlotNoCapacityCheckIT {

    /**
     * Number of concurrent FakeSource parallel task groups submitted to a single worker. Chosen to
     * be far more than any real capacity-aware system would ever grant on a small test worker (the
     * sibling fixed-slot tests in {@code SeaTunnelSlotIT} use only 3 or 10 slots as their whole
     * worker capacity) while staying small enough (each task group is a single short-lived thread
     * under the default {@code TASK_EXECUTION_THREAD_SHARE_MODE=OFF}) to run reliably on a shared
     * CI runner.
     */
    private static final int PARALLELISM = 80;

    /**
     * Total task groups that must each be granted their own slot for this job:
     *
     * <ul>
     *   <li>{@code PARALLELISM} source+sink task groups, one per partition: {@code
     *       PhysicalPlanGenerator#getSourceTask} unconditionally chains each source partition with
     *       its downstream sink into a single task group connected by an in-memory queue ({@code
     *       PhysicalPlanGenerator#splitSinkFromFlow}), independent of whether the sink has an
     *       aggregated committer.
     *   <li>Exactly one coordinator task group running the FakeSource's split enumerator (see
     *       {@code PhysicalPlanGenerator#getEnumeratorTask}, one per source regardless of
     *       parallelism).
     *   <li>No separate committer task group: {@code ConsoleSink} extends {@code
     *       AbstractSimpleSink}, whose {@code createAggregatedCommitter()} is {@code final} and
     *       always returns {@code Optional.empty()}, so {@code
     *       PhysicalPlanGenerator#getCommitterTask} creates nothing for it.
     * </ul>
     */
    private static final int EXPECTED_GRANTED_SLOTS = PARALLELISM + 1;

    @Test
    public void testDynamicSlotGrantsAllRequestsWithoutCapacityRejection() throws Exception {
        HazelcastInstanceImpl node1 = null;
        SeaTunnelClient engineClient = null;

        try {
            String testClusterName = "testDynamicSlotNoCapacityCheck";
            SeaTunnelConfig seaTunnelConfig = ConfigProvider.locateAndGetSeaTunnelConfig();
            seaTunnelConfig.getHazelcastConfig().setClusterName(testClusterName);
            seaTunnelConfig.getEngineConfig().getHttpConfig().setEnabled(false);
            // Explicit even though the shared test seatunnel.yaml already defaults to true, so
            // this test keeps documenting dynamic-slot behavior even if that default ever changes.
            seaTunnelConfig.getEngineConfig().getSlotServiceConfig().setDynamicSlot(true);

            node1 = SeaTunnelServerStarter.createHazelcastInstance(seaTunnelConfig);

            // LOCAL execution mode: the single node plays both master and worker roles, so it
            // self-registers with its own resource manager as the (only) worker.
            NodeEngineImpl nodeEngine = node1.node.nodeEngine;
            Address workerAddress = node1.node.address;
            SeaTunnelServer server = nodeEngine.getService(SeaTunnelServer.SERVICE_NAME);
            ResourceManager resourceManager = server.getCoordinatorService().getResourceManager();

            Awaitility.await()
                    .atMost(15, TimeUnit.SECONDS)
                    .untilAsserted(
                            () ->
                                    Assertions.assertTrue(
                                            resourceManager
                                                    .getRegisterWorker()
                                                    .containsKey(workerAddress),
                                            "worker should self-register with the resource manager"));

            WorkerProfile initialWorkerProfile =
                    resourceManager.getRegisterWorker().get(workerAddress);
            ResourceProfile initialUnassignedResource =
                    initialWorkerProfile.getUnassignedResource();
            // Confirms the worker's own advertised capacity is CPU.of(0); only heap memory is a
            // real, non-zero value (DefaultSlotService#getNodeResource).
            Assertions.assertEquals(0, initialUnassignedResource.getCpu().getCore());
            Assertions.assertTrue(initialUnassignedResource.getHeapMemory().getBytes() > 0);

            Common.setDeployMode(DeployMode.CLIENT);
            String filePath = TestUtils.getResource("dynamic_slot_no_capacity_check.conf");
            JobConfig jobConfig = new JobConfig();
            jobConfig.setName(testClusterName);

            ClientConfig clientConfig = ConfigProvider.locateAndGetClientConfig();
            clientConfig.setClusterName(testClusterName);
            engineClient = new SeaTunnelClient(clientConfig);
            ClientJobExecutionEnvironment jobExecutionEnv =
                    engineClient.createExecutionContext(filePath, jobConfig, seaTunnelConfig);

            final ClientJobProxy clientJobProxy = jobExecutionEnv.execute();

            // Each of the PARALLELISM FakeSource readers sleeps ~1s after fully draining a split
            // (FakeSourceReader#pollNext's Thread.sleep(1000L); only skipped while a single
            // oversized split is still being emitted across multiple poll calls, guarded by
            // splitInProgress there). This config's splits are 1 row each -- row.num=5 with
            // split.num=6 makes FakeSourceSplitEnumerator compute splitRowNum=ceil(5/6)=1, so
            // each reader gets 5 one-row splits -- far below the MAX_ROWS_PER_POLL=4096 batching
            // threshold, so splitInProgress never triggers and every split costs a full ~1s sleep
            // regardless of the split.read-interval setting. That keeps this BATCH job alive for
            // several seconds: long enough to reliably observe the mid-flight slot grants below
            // before the job releases them on completion.
            Awaitility.await()
                    .atMost(60, TimeUnit.SECONDS)
                    .pollInterval(200, TimeUnit.MILLISECONDS)
                    .untilAsserted(
                            () -> {
                                WorkerProfile duringJobProfile =
                                        resourceManager.getRegisterWorker().get(workerAddress);
                                Assertions.assertEquals(
                                        EXPECTED_GRANTED_SLOTS,
                                        duringJobProfile.getAssignedSlots().length,
                                        "dynamic-slot mode should hand out a slot for every "
                                                + "requested task group with no capacity-based "
                                                + "rejection");
                            });

            WorkerProfile duringJobProfile = resourceManager.getRegisterWorker().get(workerAddress);
            ResourceProfile duringJobUnassignedResource = duringJobProfile.getUnassignedResource();
            // The money assertion: even with EXPECTED_GRANTED_SLOTS slots just granted, the
            // worker's tracked unassigned resource is untouched, because every granted slot
            // subtracted a 0/0 ResourceProfile (ResourceUtils#applyResourceForTask) from it.
            Assertions.assertEquals(
                    initialUnassignedResource.getCpu().getCore(),
                    duringJobUnassignedResource.getCpu().getCore(),
                    "unassigned CPU must be unchanged: the capacity check never actually "
                            + "decrements");
            Assertions.assertEquals(
                    initialUnassignedResource.getHeapMemory().getBytes(),
                    duringJobUnassignedResource.getHeapMemory().getBytes(),
                    "unassigned heap memory must be unchanged: the capacity check never "
                            + "actually decrements");

            // Finally, confirm this is purely a missing capacity safety net and not a
            // correctness bug: with every slot granted and zero rejections, the job still runs
            // all EXPECTED_GRANTED_SLOTS task groups through to a normal, successful completion.
            CompletableFuture<JobResult> jobResultFuture =
                    CompletableFuture.supplyAsync(clientJobProxy::waitForJobCompleteV2);
            Awaitility.await()
                    .atMost(300, TimeUnit.SECONDS)
                    .untilAsserted(
                            () -> {
                                Assertions.assertTrue(jobResultFuture.isDone());
                                JobResult jobResult = jobResultFuture.get();
                                Assertions.assertEquals(
                                        JobStatus.FINISHED,
                                        jobResult.getStatus(),
                                        "job should complete successfully at this concurrency "
                                                + "(no correctness regression), error: "
                                                + jobResult.getError());
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
