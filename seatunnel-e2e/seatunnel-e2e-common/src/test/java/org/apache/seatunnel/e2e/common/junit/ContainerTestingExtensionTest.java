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

import org.apache.seatunnel.e2e.common.container.EngineType;
import org.apache.seatunnel.e2e.common.container.TestContainer;
import org.apache.seatunnel.e2e.common.container.TestContainerId;
import org.apache.seatunnel.e2e.common.container.TestContainersFactory;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.opentest4j.TestAbortedException;

import java.util.Collections;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class ContainerTestingExtensionTest {

    private final ContainerTestingExtension extension = new ContainerTestingExtension();

    @Test
    void shouldAbortSuiteWhenNoContainerIsDiscovered() {
        assertThrows(
                TestAbortedException.class,
                () -> extension.beforeAll(contextFor(new EmptyFactorySuite())),
                "A suite without any discovered container should be skipped");
    }

    @Test
    void shouldAbortSuiteWhenEveryContainerIsDisabledOnTheSuite() {
        assertThrows(
                TestAbortedException.class,
                () -> extension.beforeAll(contextFor(new ZetaDisabledSuite())),
                "A suite whose containers are all disabled by @DisabledOnContainer should be"
                        + " skipped");
    }

    @Test
    void shouldRunSuiteWhenAtLeastOneContainerIsEnabled() {
        assertDoesNotThrow(() -> extension.beforeAll(contextFor(new ZetaSuite())));
    }

    private ExtensionContext contextFor(Object testInstance) {
        ExtensionContext context = mock(ExtensionContext.class);
        when(context.getRequiredTestInstance()).thenReturn(testInstance);
        doReturn(testInstance.getClass()).when(context).getRequiredTestClass();
        when(context.getStore(any(ExtensionContext.Namespace.class)))
                .thenReturn(mock(ExtensionContext.Store.class));
        return context;
    }

    private static TestContainer container(TestContainerId identifier) {
        TestContainer container = mock(TestContainer.class);
        when(container.identifier()).thenReturn(identifier);
        return container;
    }

    /** Simulates a suite left without containers by the PR engine selection. */
    private static final class EmptyFactorySuite {
        @TestContainers
        private final TestContainersFactory containersFactory = Collections::emptyList;
    }

    /** Simulates a Flink/Spark-only suite on a Zeta-only PR run. */
    @DisabledOnContainer(
            value = {},
            type = EngineType.SEATUNNEL)
    private static final class ZetaDisabledSuite {
        @TestContainers
        private final TestContainersFactory containersFactory =
                () -> Collections.singletonList(container(TestContainerId.SEATUNNEL));
    }

    private static final class ZetaSuite {
        @TestContainers
        private final TestContainersFactory containersFactory =
                () -> Collections.singletonList(container(TestContainerId.SEATUNNEL));
    }
}
