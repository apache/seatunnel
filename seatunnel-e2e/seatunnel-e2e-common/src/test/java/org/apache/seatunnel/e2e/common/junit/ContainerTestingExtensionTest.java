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

import java.util.Arrays;
import java.util.Collections;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class ContainerTestingExtensionTest {

    private final ContainerTestingExtension extension = new ContainerTestingExtension();

    @Test
    void shouldNotSkipSuiteWhoseFactoryCreatedNoContainer() {
        // No engine selection narrows this build, so an empty factory is a broken suite:
        // beforeAll must fall through and let JUnit fail the empty @TestTemplate invocation
        // stream instead of reporting the suite as skipped.
        assertDoesNotThrow(
                () -> extension.beforeAll(contextFor(new EmptyFactorySuite())),
                "A suite whose factory creates no container must fail, not be skipped");
    }

    @Test
    void shouldNotSkipSuiteThatDisablesEveryContainerOfItsOwnFactory() {
        assertDoesNotThrow(
                () -> extension.beforeAll(contextFor(new ZetaDisabledSuite())),
                "A suite whose factory containers are all disabled by @DisabledOnContainer must"
                        + " fail, not be skipped");
    }

    @Test
    void shouldRunSuiteWhenAtLeastOneContainerIsEnabled() {
        assertDoesNotThrow(() -> extension.beforeAll(contextFor(new ZetaSuite())));
    }

    @Test
    void shouldSkipOnlyWhenTheEngineSelectionLeftTheSuiteEmpty() {
        // A Flink/Spark-only suite on a Zeta-only run: without the selection the suite would
        // still run its Flink container, and the selection removes it.
        assertTrue(
                ContainerTestingExtension.engineSelectionLeftSuiteWithoutContainers(
                        Arrays.asList(
                                container(TestContainerId.FLINK_1_18),
                                container(TestContainerId.SEATUNNEL)),
                        Collections.singletonList(container(TestContainerId.SEATUNNEL)),
                        ZetaDisabledSuite.class),
                "A suite the engine selection left without containers should be skipped");
    }

    @Test
    void shouldNotSkipWhenTheSelectionRemovesNoContainerOfTheSuite() {
        // The selection keeps every container the suite could run, so an empty factory output
        // cannot be explained by the engine selection and must keep the failure signal.
        assertFalse(
                ContainerTestingExtension.engineSelectionLeftSuiteWithoutContainers(
                        Arrays.asList(
                                container(TestContainerId.FLINK_1_18),
                                container(TestContainerId.SEATUNNEL)),
                        Arrays.asList(
                                container(TestContainerId.FLINK_1_18),
                                container(TestContainerId.SEATUNNEL)),
                        ZetaSuite.class),
                "A suite emptied by something other than the engine selection must not be"
                        + " skipped");
    }

    @Test
    void shouldNotSkipSuiteWithoutAnyContainerBeforeTheSelection() {
        // The suite disables the only engine it could run: nothing runs even with the
        // selection lifted, which is a suite bug rather than an engine-selection skip.
        assertFalse(
                ContainerTestingExtension.engineSelectionLeftSuiteWithoutContainers(
                        Collections.singletonList(container(TestContainerId.SEATUNNEL)),
                        Collections.emptyList(),
                        ZetaDisabledSuite.class),
                "A suite that cannot run any container even without the engine selection must"
                        + " not be skipped");
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

    /** Simulates a broken suite: its factory creates no container at all. */
    private static final class EmptyFactorySuite {
        @TestContainers
        private final TestContainersFactory containersFactory = Collections::emptyList;
    }

    /** Simulates a suite that disables the Zeta engine at class level, like HbaseIT. */
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
