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

package org.apache.seatunnel.engine.server.resourcemanager;

import org.apache.seatunnel.engine.common.config.EngineConfig;
import org.apache.seatunnel.engine.common.config.server.AllocateStrategy;
import org.apache.seatunnel.engine.common.utils.concurrent.CompletableFuture;
import org.apache.seatunnel.engine.server.AbstractSeaTunnelServerTest;
import org.apache.seatunnel.engine.server.SeaTunnelServer;
import org.apache.seatunnel.engine.server.resourcemanager.allocation.strategy.RandomStrategy;
import org.apache.seatunnel.engine.server.resourcemanager.opeartion.ReleaseSlotOperation;
import org.apache.seatunnel.engine.server.resourcemanager.resource.CPU;
import org.apache.seatunnel.engine.server.resourcemanager.resource.Memory;
import org.apache.seatunnel.engine.server.resourcemanager.resource.ResourceProfile;
import org.apache.seatunnel.engine.server.resourcemanager.resource.SlotProfile;
import org.apache.seatunnel.engine.server.resourcemanager.worker.WorkerProfile;
import org.apache.seatunnel.engine.server.service.slot.SlotService;
import org.apache.seatunnel.engine.server.telemetry.metrics.entity.RequestSlotOperationStats;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import com.hazelcast.cluster.Address;
import com.hazelcast.spi.impl.NodeEngine;
import com.hazelcast.spi.impl.operationservice.Operation;

import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

public class ResourceManagerTest extends AbstractSeaTunnelServerTest<ResourceManagerTest> {

    private ResourceManager resourceManager;

    private final long jobId = 5;

    @BeforeAll
    public void before() {
        super.before();
        resourceManager = server.getCoordinatorService().getResourceManager();
        server.getSlotService();
    }

    @Test
    public void testHaveWorkerWhenUseHybridDeployment() {
        Assertions.assertEquals(1, resourceManager.workerCount(null));
    }

    @Test
    public void testApplyRequest() throws ExecutionException, InterruptedException {
        List<ResourceProfile> resourceProfiles = new ArrayList<>();
        resourceProfiles.add(new ResourceProfile(CPU.of(0), Memory.of(100)));
        resourceProfiles.add(new ResourceProfile(CPU.of(0), Memory.of(200)));
        resourceProfiles.add(new ResourceProfile(CPU.of(0), Memory.of(300)));
        List<SlotProfile> slotProfiles =
                resourceManager.applyResources(jobId, resourceProfiles, null).get();

        Assertions.assertEquals(
                resourceProfiles.get(0).getHeapMemory().getBytes(),
                slotProfiles.get(0).getResourceProfile().getHeapMemory().getBytes());
        Assertions.assertEquals(
                resourceProfiles.get(1).getHeapMemory().getBytes(),
                slotProfiles.get(1).getResourceProfile().getHeapMemory().getBytes());
        Assertions.assertEquals(
                resourceProfiles.get(2).getHeapMemory().getBytes(),
                slotProfiles.get(2).getResourceProfile().getHeapMemory().getBytes());

        // release not existed job id
        resourceManager.releaseResources(jobId + 1, slotProfiles).get();
        resourceManager.releaseResources(jobId, slotProfiles).get();
        // release already released resource
        resourceManager.releaseResources(jobId, slotProfiles).get();
        Assertions.assertThrows(
                ExecutionException.class,
                () ->
                        resourceManager
                                .applyResource(
                                        jobId,
                                        new ResourceProfile(CPU.of(0), Memory.of(Long.MAX_VALUE)),
                                        null)
                                .get());
    }

    @Test
    public void testApplyResourceWithRandomResult()
            throws ExecutionException, InterruptedException {
        FakeResourceManager resourceManager = new FakeResourceManager(nodeEngine);
        resourceManager
                .getEngineConfig()
                .getSlotServiceConfig()
                .setAllocateStrategy(AllocateStrategy.RANDOM);
        boolean hasDifferentWorker = false;
        for (int i = 0; i < 5; i++) {
            List<ResourceProfile> resourceProfiles = new ArrayList<>();
            resourceProfiles.add(new ResourceProfile());
            resourceProfiles.add(new ResourceProfile());
            resourceProfiles.add(new ResourceProfile());
            resourceProfiles.add(new ResourceProfile());
            resourceProfiles.add(new ResourceProfile());
            List<SlotProfile> slotProfiles =
                    resourceManager.applyResources(1L, resourceProfiles, null).get();
            Assertions.assertEquals(slotProfiles.size(), 5);
            Set<Address> addresses =
                    slotProfiles.stream().map(SlotProfile::getWorker).collect(Collectors.toSet());
            hasDifferentWorker |= addresses.size() > 1;
        }
        Assertions.assertTrue(hasDifferentWorker, "should have different worker for each slot");
    }

    @Test
    public void testApplyResourceWithRetryWhenSameNodeNoSlotSuited()
            throws ExecutionException, InterruptedException {
        // test retry request slot times 1
        FakeResourceManagerForRequestSlotRetryTest resourceManager =
                new FakeResourceManagerForRequestSlotRetryTest(nodeEngine, 2, 1);
        List<ResourceProfile> resourceProfiles = new ArrayList<>();
        resourceProfiles.add(new ResourceProfile());
        resourceProfiles.add(new ResourceProfile());
        List<SlotProfile> slotProfiles =
                resourceManager.applyResources(1L, resourceProfiles, null).get();
        Assertions.assertEquals(slotProfiles.size(), 2);

        // test retry request slot time 2 but no enough slot with worker
        resourceManager = new FakeResourceManagerForRequestSlotRetryTest(nodeEngine, 2, 2);
        FakeResourceManagerForRequestSlotRetryTest finalResourceManager = resourceManager;
        List<ResourceProfile> finalResourceProfiles = resourceProfiles;
        ExecutionException exception =
                Assertions.assertThrows(
                        ExecutionException.class,
                        () ->
                                finalResourceManager
                                        .applyResources(1L, finalResourceProfiles, null)
                                        .get());
        Assertions.assertInstanceOf(NoEnoughResourceException.class, exception.getCause());

        // test retry request slot time 4 so that more than max retry times
        resourceProfiles = new ArrayList<>();
        resourceProfiles.add(new ResourceProfile());
        resourceManager = new FakeResourceManagerForRequestSlotRetryTest(nodeEngine, 5, 4);
        List<ResourceProfile> finalResourceProfiles2 = resourceProfiles;
        FakeResourceManagerForRequestSlotRetryTest finalResourceManager2 = resourceManager;
        ExecutionException exception2 =
                Assertions.assertThrows(
                        ExecutionException.class,
                        () ->
                                finalResourceManager2
                                        .applyResources(1L, finalResourceProfiles2, null)
                                        .get());
        Assertions.assertInstanceOf(NoEnoughResourceException.class, exception2.getCause());
        Assertions.assertEquals(
                "can't apply resource request with retry times: 3",
                exception2.getCause().getMessage());
    }

    @Test
    public void testRequestSlotOperationStatsForSuccessAndNoSlot()
            throws ExecutionException, InterruptedException {
        FakeResourceManagerForRequestSlotRetryTest resourceManager =
                new FakeResourceManagerForRequestSlotRetryTest(nodeEngine, 2, 1);

        List<SlotProfile> slotProfiles =
                resourceManager
                        .applyResources(1L, Collections.singletonList(new ResourceProfile()), null)
                        .get();

        Assertions.assertEquals(1, slotProfiles.size());
        RequestSlotOperationStats stats = resourceManager.getRequestSlotOperationStats();
        Assertions.assertEquals(1, stats.getSuccessCount());
        Assertions.assertEquals(1, stats.getNoSlotCount());
        Assertions.assertEquals(0, stats.getFailureCount());
    }

    @Test
    public void testRequestSlotOperationStatsAggregation() {
        FakeResourceManagerForRequestSlotRetryTest resourceManager =
                new FakeResourceManagerForRequestSlotRetryTest(nodeEngine, 0, 0);

        resourceManager.recordRequestSlotOperationSuccess(10L);
        resourceManager.recordRequestSlotOperationNoSlot(20L);
        resourceManager.recordRequestSlotOperationFailure(15L);

        RequestSlotOperationStats stats = resourceManager.getRequestSlotOperationStats();
        Assertions.assertEquals(1, stats.getSuccessCount());
        Assertions.assertEquals(1, stats.getNoSlotCount());
        Assertions.assertEquals(1, stats.getFailureCount());
        Assertions.assertEquals(15L, stats.getLastInvocationLatencyMs());
        Assertions.assertEquals(20L, stats.getMaxInvocationLatencyMs());
    }

    @Test
    public void testApplyResourcesPreserveCauseForExternalException()
            throws ExecutionException, InterruptedException {
        FakeResourceManagerForExternalExceptionTest resourceManager =
                new FakeResourceManagerForExternalExceptionTest(nodeEngine);
        List<ResourceProfile> resourceProfiles = new ArrayList<>();
        resourceProfiles.add(new ResourceProfile());
        ExecutionException exception =
                Assertions.assertThrows(
                        ExecutionException.class,
                        () -> resourceManager.applyResources(1L, resourceProfiles, null).get());
        Assertions.assertInstanceOf(NoEnoughResourceException.class, exception.getCause());
        Assertions.assertInstanceOf(
                IllegalStateException.class,
                ((NoEnoughResourceException) exception.getCause()).getCause());

        RequestSlotOperationStats stats = resourceManager.getRequestSlotOperationStats();
        Assertions.assertEquals(0, stats.getSuccessCount());
        Assertions.assertEquals(0, stats.getNoSlotCount());
        Assertions.assertEquals(1, stats.getFailureCount());
    }

    /**
     * Verifies that local slot release stays on the local worker path instead of going through a
     * self-targeted Hazelcast operation.
     */
    @Test
    public void testReleaseResourceOnLocalWorkerShouldBypassOperationInvocation()
            throws ExecutionException, InterruptedException, UnknownHostException {
        Address localAddress = new Address("localhost", 5801);
        NodeEngine mockNodeEngine = Mockito.mock(NodeEngine.class);
        SeaTunnelServer mockServer = Mockito.mock(SeaTunnelServer.class);
        SlotService mockSlotService = Mockito.mock(SlotService.class);
        SlotProfile slotProfile = new SlotProfile(localAddress, 1, new ResourceProfile(), "slot-1");
        WorkerProfile workerProfile =
                new WorkerProfile(
                        localAddress,
                        new ResourceProfile(),
                        new ResourceProfile(),
                        true,
                        new SlotProfile[] {},
                        new SlotProfile[] {},
                        Collections.emptyMap());

        Mockito.when(mockNodeEngine.getThisAddress()).thenReturn(localAddress);
        Mockito.when(mockNodeEngine.getService(SeaTunnelServer.SERVICE_NAME))
                .thenReturn(mockServer);
        Mockito.when(mockServer.getSlotService()).thenReturn(mockSlotService);
        Mockito.when(mockSlotService.getWorkerProfile()).thenReturn(workerProfile);
        AtomicBoolean releaseSlotSentToMember = new AtomicBoolean(false);

        AbstractResourceManager resourceManager =
                new AbstractResourceManager(mockNodeEngine, new EngineConfig()) {
                    @Override
                    protected <E> CompletableFuture<E> sendToMember(
                            Operation operation, Address address) {
                        if (operation instanceof ReleaseSlotOperation) {
                            releaseSlotSentToMember.set(true);
                            return (CompletableFuture<E>)
                                    CompletableFuture.completedFuture(workerProfile);
                        }
                        return CompletableFuture.completedFuture(null);
                    }
                };

        resourceManager.releaseResource(jobId, slotProfile).get();

        Mockito.verify(mockSlotService).releaseSlot(jobId, slotProfile);
        Assertions.assertFalse(releaseSlotSentToMember.get());
        Assertions.assertEquals(
                workerProfile, resourceManager.getRegisterWorker().get(localAddress));
    }

    @Test
    public void testPreCheckWorkerResourceWithDynamicSlot() throws UnknownHostException {
        testPreCheckWorkerResource(true);
        testPreCheckWorkerResource(false);
    }

    public void testPreCheckWorkerResource(boolean dynamicSlot) throws UnknownHostException {
        List<ResourceProfile> resourceProfiles = new ArrayList<>();
        resourceProfiles.add(new ResourceProfile());
        ConcurrentMap<Address, WorkerProfile> registerWorker = new ConcurrentHashMap<>();
        Address address1 = new Address("127.0.0.1", 5801);
        WorkerProfile workerProfile1 =
                new WorkerProfile(
                        address1,
                        new ResourceProfile(),
                        new ResourceProfile(),
                        dynamicSlot,
                        new SlotProfile[] {},
                        new SlotProfile[] {},
                        Collections.emptyMap());
        registerWorker.put(address1, workerProfile1);

        Address address2 = new Address("127.0.0.1", 5802);
        WorkerProfile workerProfile2 =
                new WorkerProfile(
                        address2,
                        new ResourceProfile(),
                        new ResourceProfile(),
                        dynamicSlot,
                        new SlotProfile[] {},
                        new SlotProfile[] {},
                        Collections.emptyMap());
        registerWorker.put(address2, workerProfile2);
        Optional<WorkerProfile> result =
                new ResourceRequestHandler(
                                jobId,
                                resourceProfiles,
                                registerWorker,
                                (AbstractResourceManager) this.resourceManager,
                                new RandomStrategy())
                        .preCheckWorkerResource(new ResourceProfile());
        Assertions.assertEquals(result.isPresent(), dynamicSlot);
    }
}
