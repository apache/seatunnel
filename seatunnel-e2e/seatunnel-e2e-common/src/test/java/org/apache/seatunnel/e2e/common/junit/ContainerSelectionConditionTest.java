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

import org.junit.jupiter.api.Test;

import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.apache.seatunnel.e2e.common.container.TestContainerId.FLINK_1_18;
import static org.apache.seatunnel.e2e.common.container.TestContainerId.SEATUNNEL;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class ContainerSelectionConditionTest {

    private static final List<TestContainer> ZETA_ONLY =
            Collections.singletonList(container(SEATUNNEL));

    private static final List<TestContainer> ZETA_AND_FLINK =
            Arrays.asList(container(SEATUNNEL), container(FLINK_1_18));

    @Test
    void shouldDisableSuiteWhenAllSelectedEnginesAreDisabled() {
        assertTrue(
                ContainerSelectionCondition.shouldDisable(
                        ZETA_ONLY, AllEnginesDisabledSuite.class, null));
    }

    @Test
    void shouldDisableSuiteWhenAllSelectedContainersAreDisabledByIdentifier() {
        assertTrue(
                ContainerSelectionCondition.shouldDisable(
                        ZETA_ONLY, ZetaContainerDisabledSuite.class, null));
    }

    @Test
    void shouldKeepSuiteWhenAnotherSelectedEngineRemains() {
        assertFalse(
                ContainerSelectionCondition.shouldDisable(
                        ZETA_AND_FLINK, ZetaEngineDisabledSuite.class, null));
    }

    @Test
    void shouldKeepSuiteWhenNoContainerWasDiscovered() {
        assertFalse(
                ContainerSelectionCondition.shouldDisable(
                        Collections.emptyList(), AllEnginesDisabledSuite.class, null));
    }

    @Test
    void shouldDisableMethodWhenAllSelectedEnginesAreDisabled() throws Exception {
        Method method = MethodLevelSuite.class.getDeclaredMethod("zetaOnly");

        assertTrue(
                ContainerSelectionCondition.shouldDisable(
                        ZETA_ONLY, MethodLevelSuite.class, method));
    }

    @Test
    void shouldKeepMethodWhenAnotherSelectedEngineRemains() throws Exception {
        Method method = MethodLevelSuite.class.getDeclaredMethod("notZeta");

        assertFalse(
                ContainerSelectionCondition.shouldDisable(
                        ZETA_AND_FLINK, MethodLevelSuite.class, method));
    }

    private static TestContainer container(TestContainerId identifier) {
        TestContainer container = mock(TestContainer.class);
        when(container.identifier()).thenReturn(identifier);
        return container;
    }

    @DisabledOnContainer(
            value = {},
            type = {EngineType.SEATUNNEL, EngineType.FLINK})
    private static class AllEnginesDisabledSuite {}

    @DisabledOnContainer(value = SEATUNNEL)
    private static class ZetaContainerDisabledSuite {}

    @DisabledOnContainer(
            value = {},
            type = EngineType.SEATUNNEL)
    private static class ZetaEngineDisabledSuite {}

    private static class MethodLevelSuite {

        @DisabledOnContainer(
                value = {},
                type = EngineType.SEATUNNEL)
        void zetaOnly() {}

        @DisabledOnContainer(
                value = {},
                type = EngineType.FLINK)
        void notZeta() {}
    }
}
