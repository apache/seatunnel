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

package org.apache.seatunnel.e2e.common.junit;

import org.apache.seatunnel.e2e.common.container.ContainerExtendedFactory;
import org.apache.seatunnel.e2e.common.container.ReusableTestContainer;
import org.apache.seatunnel.e2e.common.container.TestContainerId;

import org.junit.jupiter.api.Test;
import org.testcontainers.containers.Container;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class SharedTestContainerResourceTest {

    private static final ContainerExtendedFactory NO_EXTENSION = container -> {};

    @Test
    void shouldReuseContainerAndCleanEachClassLease() throws Exception {
        CountingTestContainer container = new CountingTestContainer();
        SharedTestContainerResource resource = new SharedTestContainerResource(container);

        assertSame(container, resource.acquire(NO_EXTENSION));
        resource.release();
        assertSame(container, resource.acquire(NO_EXTENSION));
        resource.release();
        resource.close();

        assertEquals(1, container.startCount);
        assertEquals(2, container.prepareCount);
        assertEquals(2, container.extensionCount);
        assertEquals(2, container.cleanupCount);
        assertEquals(1, container.tearDownCount);
    }

    @Test
    void shouldAllowStartupRetryAfterFailure() throws Exception {
        CountingTestContainer container = new CountingTestContainer();
        container.failFirstStartup = true;
        SharedTestContainerResource resource = new SharedTestContainerResource(container);

        assertThrows(IOException.class, () -> resource.acquire(NO_EXTENSION));
        assertSame(container, resource.acquire(NO_EXTENSION));
        resource.release();
        resource.close();

        assertEquals(2, container.startCount);
        assertEquals(1, container.cleanupCount);
        assertEquals(2, container.tearDownCount);
    }

    @Test
    void shouldRestartAfterPreparationFailure() throws Exception {
        CountingTestContainer container = new CountingTestContainer();
        container.failFirstPreparation = true;
        SharedTestContainerResource resource = new SharedTestContainerResource(container);

        assertThrows(IOException.class, () -> resource.acquire(NO_EXTENSION));
        assertSame(container, resource.acquire(NO_EXTENSION));
        resource.release();
        resource.close();

        assertEquals(2, container.startCount);
        assertEquals(2, container.prepareCount);
        assertEquals(1, container.cleanupCount);
        assertEquals(2, container.tearDownCount);
    }

    @Test
    void shouldRestartAfterExtensionFailure() throws Exception {
        CountingTestContainer container = new CountingTestContainer();
        SharedTestContainerResource resource = new SharedTestContainerResource(container);

        assertThrows(
                IOException.class,
                () ->
                        resource.acquire(
                                ignored -> {
                                    throw new IOException("extension failed");
                                }));
        assertSame(container, resource.acquire(NO_EXTENSION));
        resource.release();
        resource.close();

        assertEquals(2, container.startCount);
        assertEquals(2, container.prepareCount);
        assertEquals(2, container.extensionCount);
        assertEquals(1, container.cleanupCount);
        assertEquals(2, container.tearDownCount);
    }

    @Test
    void shouldRestartAfterCleanupFailure() throws Exception {
        CountingTestContainer container = new CountingTestContainer();
        container.failFirstCleanup = true;
        SharedTestContainerResource resource = new SharedTestContainerResource(container);

        assertSame(container, resource.acquire(NO_EXTENSION));
        assertThrows(IOException.class, resource::release);
        assertSame(container, resource.acquire(NO_EXTENSION));
        resource.release();
        resource.close();

        assertEquals(2, container.startCount);
        assertEquals(2, container.prepareCount);
        assertEquals(2, container.cleanupCount);
        assertEquals(2, container.tearDownCount);
    }

    @Test
    void shouldSerializeClassLeases() throws Exception {
        CountingTestContainer container = new CountingTestContainer();
        SharedTestContainerResource resource = new SharedTestContainerResource(container);
        CountDownLatch firstLeaseAcquired = new CountDownLatch(1);
        CountDownLatch releaseFirstLease = new CountDownLatch(1);
        CountDownLatch secondAcquireStarted = new CountDownLatch(1);
        CountDownLatch secondLeaseAcquired = new CountDownLatch(1);
        ExecutorService executor = Executors.newFixedThreadPool(2);

        try {
            Future<?> firstLease =
                    executor.submit(
                            () -> {
                                resource.acquire(NO_EXTENSION);
                                firstLeaseAcquired.countDown();
                                releaseFirstLease.await();
                                resource.release();
                                return null;
                            });
            assertTrue(firstLeaseAcquired.await(10, TimeUnit.SECONDS));

            Future<?> secondLease =
                    executor.submit(
                            () -> {
                                secondAcquireStarted.countDown();
                                resource.acquire(NO_EXTENSION);
                                secondLeaseAcquired.countDown();
                                resource.release();
                                return null;
                            });

            assertTrue(secondAcquireStarted.await(10, TimeUnit.SECONDS));
            assertFalse(secondLeaseAcquired.await(200, TimeUnit.MILLISECONDS));
            releaseFirstLease.countDown();
            firstLease.get(10, TimeUnit.SECONDS);
            secondLease.get(10, TimeUnit.SECONDS);
            resource.close();

            assertEquals(1, container.startCount);
            assertEquals(2, container.prepareCount);
            assertEquals(2, container.cleanupCount);
            assertEquals(1, container.tearDownCount);
        } finally {
            releaseFirstLease.countDown();
            executor.shutdownNow();
            assertTrue(executor.awaitTermination(10, TimeUnit.SECONDS));
        }
    }

    private static final class CountingTestContainer implements ReusableTestContainer {

        private int startCount;
        private int prepareCount;
        private int extensionCount;
        private int cleanupCount;
        private int tearDownCount;
        private boolean failFirstStartup;
        private boolean failFirstPreparation;
        private boolean failFirstCleanup;

        @Override
        public void startUp() throws Exception {
            startCount++;
            if (failFirstStartup) {
                failFirstStartup = false;
                throw new IOException("startup failed");
            }
        }

        @Override
        public void tearDown() {
            tearDownCount++;
        }

        @Override
        public void prepareForTestClass() throws IOException {
            prepareCount++;
            if (failFirstPreparation) {
                failFirstPreparation = false;
                throw new IOException("preparation failed");
            }
        }

        @Override
        public void cleanUpAfterTestClass() throws IOException {
            cleanupCount++;
            if (failFirstCleanup) {
                failFirstCleanup = false;
                throw new IOException("cleanup failed");
            }
        }

        @Override
        public TestContainerId identifier() {
            return TestContainerId.SEATUNNEL;
        }

        @Override
        public void executeExtraCommands(ContainerExtendedFactory extendedFactory)
                throws IOException, InterruptedException {
            extensionCount++;
            extendedFactory.extend(null);
        }

        @Override
        public Container.ExecResult executeJob(String confFile) {
            throw new UnsupportedOperationException();
        }

        @Override
        public Container.ExecResult executeJob(String confFile, List<String> variables) {
            throw new UnsupportedOperationException();
        }

        @Override
        public String getServerLogs() {
            throw new UnsupportedOperationException();
        }

        @Override
        public void copyFileToContainer(String path, String targetPath) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void copyAbsolutePathToContainer(String path, String targetPath) {
            throw new UnsupportedOperationException();
        }
    }
}
