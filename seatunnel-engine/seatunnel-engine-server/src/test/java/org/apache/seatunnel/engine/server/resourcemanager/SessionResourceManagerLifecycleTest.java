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
import org.apache.seatunnel.engine.common.config.spec.SessionSpecification;
import org.apache.seatunnel.engine.common.config.spec.WorkerSpecification;
import org.apache.seatunnel.engine.common.utils.concurrent.CompletableFuture;
import org.apache.seatunnel.engine.server.resourcemanager.resource.ResourceID;
import org.apache.seatunnel.engine.server.resourcemanager.resource.ResourceProfile;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.hazelcast.cluster.Address;
import com.hazelcast.spi.impl.NodeEngine;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

import static org.awaitility.Awaitility.await;
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class SessionResourceManagerLifecycleTest {

    private NodeEngine nodeEngine;
    private SessionSpecification specification;

    @BeforeEach
    void setUp() throws Exception {
        nodeEngine = mock(NodeEngine.class, RETURNS_DEEP_STUBS);
        when(nodeEngine.getThisAddress()).thenReturn(new Address("127.0.0.1", 5801));
        when(nodeEngine.getClusterService().getMembers()).thenReturn(Collections.emptySet());
        specification =
                new SessionSpecification(
                        "test-session",
                        "test-cluster",
                        2,
                        3,
                        new WorkerSpecification(1024, 1, 8),
                        true,
                        5_000L,
                        20L);
    }

    @AfterEach
    void tearDown() {
        // Managers created by individual tests are closed in the test body.
    }

    @Test
    void createsMinWorkersAndReplacesTerminatedWorker() {
        FakeDriver driver = new FakeDriver();
        SessionResourceManager<ResourceID> manager = newManager(driver);
        try {
            manager.init();
            await().atMost(5, TimeUnit.SECONDS)
                    .untilAsserted(() -> Assertions.assertEquals(2, driver.requested.get()));
            Assertions.assertEquals(2, manager.getWorkerIds().size());

            ResourceID terminated = driver.lastWorker();
            manager.onWorkerTerminated(terminated, "test termination");

            await().atMost(5, TimeUnit.SECONDS)
                    .untilAsserted(() -> Assertions.assertEquals(3, driver.requested.get()));
            Assertions.assertTrue(driver.released.contains(terminated));
            Assertions.assertEquals(2, manager.getWorkerIds().size());
        } finally {
            manager.close();
        }
        Assertions.assertTrue(driver.closed.get());
    }

    @Test
    void doesNotExceedMaxWorkersWhenCreatedPodsHaveNotRegistered() {
        FakeDriver driver = new FakeDriver();
        SessionResourceManager<ResourceID> manager =
                new SessionResourceManager<>(
                        nodeEngine, new EngineConfig(), sessionSpecification(2, 2), driver);
        try {
            manager.init();
            await().atMost(5, TimeUnit.SECONDS)
                    .untilAsserted(() -> Assertions.assertEquals(2, driver.requested.get()));

            Assertions.assertThrows(
                    NoEnoughResourceException.class,
                    () ->
                            manager.findNewWorker(
                                    Collections.singletonList(new ResourceProfile()),
                                    Collections.emptyMap()));
            Assertions.assertEquals(2, driver.requested.get());
        } finally {
            manager.close();
        }
    }

    @Test
    void factoryCreatesFreshDriverAfterManagerClose() {
        AtomicInteger suppliers = new AtomicInteger();
        List<FakeDriver> drivers = new ArrayList<>();
        ResourceManagerFactory factory =
                new ResourceManagerFactory(
                        specification,
                        () -> {
                            suppliers.incrementAndGet();
                            FakeDriver driver = new FakeDriver();
                            drivers.add(driver);
                            return driver;
                        });

        ResourceManager first = factory.createResourceManager(nodeEngine, new EngineConfig());
        first.init();
        first.close();

        ResourceManager second = factory.createResourceManager(nodeEngine, new EngineConfig());
        second.init();
        second.close();

        Assertions.assertEquals(2, suppliers.get());
        Assertions.assertEquals(2, drivers.size());
        Assertions.assertTrue(drivers.get(0) != drivers.get(1));
        Assertions.assertTrue(drivers.get(0).closed.get());
        Assertions.assertTrue(drivers.get(1).closed.get());
    }

    private SessionResourceManager<ResourceID> newManager(FakeDriver driver) {
        return new SessionResourceManager<>(nodeEngine, new EngineConfig(), specification, driver);
    }

    private static SessionSpecification sessionSpecification(int minWorkers, int maxWorkers) {
        return new SessionSpecification(
                "test-session",
                "test-cluster",
                minWorkers,
                maxWorkers,
                new WorkerSpecification(1024, 1, 8),
                true,
                5_000L,
                20L);
    }

    private static final class FakeDriver implements ResourceManagerDriver<ResourceID> {
        private final AtomicInteger requested = new AtomicInteger();
        private final AtomicBoolean closed = new AtomicBoolean();
        private final List<ResourceID> workers = new ArrayList<>();
        private final List<ResourceID> released = new ArrayList<>();

        @Override
        public void initialize(
                ResourceEventHandler<ResourceID> resourceEventHandler,
                ScheduledExecutorService mainThreadExecutor,
                Executor ioExecutor,
                Supplier<String> masterAddress) {}

        @Override
        public CompletableFuture<ResourceID> requestWorker(WorkerSpecification specification) {
            ResourceID worker = new ResourceID("worker-" + requested.incrementAndGet());
            synchronized (workers) {
                workers.add(worker);
            }
            return CompletableFuture.completedFuture(worker);
        }

        @Override
        public List<ResourceID> listWorkers() {
            synchronized (workers) {
                return new ArrayList<>(workers);
            }
        }

        @Override
        public void adoptWorkers(List<ResourceID> adopted) {
            synchronized (workers) {
                workers.addAll(adopted);
            }
        }

        @Override
        public CompletableFuture<Void> releaseWorker(ResourceID worker) {
            released.add(worker);
            synchronized (workers) {
                workers.remove(worker);
            }
            return CompletableFuture.completedFuture(null);
        }

        @Override
        public void close() {
            closed.set(true);
        }

        ResourceID lastWorker() {
            synchronized (workers) {
                return workers.get(workers.size() - 1);
            }
        }
    }
}
