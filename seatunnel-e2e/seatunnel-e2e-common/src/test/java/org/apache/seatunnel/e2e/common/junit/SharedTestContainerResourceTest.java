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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

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
        assertEquals(1, container.tearDownCount);
    }

    private static final class CountingTestContainer implements ReusableTestContainer {

        private int startCount;
        private int extensionCount;
        private int cleanupCount;
        private int tearDownCount;
        private boolean failFirstStartup;

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
        public void cleanUpAfterTestClass() {
            cleanupCount++;
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
