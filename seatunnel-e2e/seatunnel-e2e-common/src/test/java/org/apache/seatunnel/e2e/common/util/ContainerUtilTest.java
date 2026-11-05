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

package org.apache.seatunnel.e2e.common.util;

import org.apache.seatunnel.e2e.common.container.TestContainer;
import org.apache.seatunnel.e2e.common.container.TestContainerId;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class ContainerUtilTest {

    private static final List<TestContainer> CONTAINERS =
            Arrays.asList(
                    container(TestContainerId.FLINK_1_18),
                    container(TestContainerId.FLINK_1_20),
                    container(TestContainerId.SPARK_3_3),
                    container(TestContainerId.SEATUNNEL));

    @Test
    void shouldKeepEveryContainerWhenRunEnginesIsUnset() {
        assertEquals(CONTAINERS, ContainerUtil.filterByRunEngines(CONTAINERS, null));
        assertEquals(CONTAINERS, ContainerUtil.filterByRunEngines(CONTAINERS, " "));
        assertEquals(CONTAINERS, ContainerUtil.filterByRunEngines(CONTAINERS, "[]"));
        assertEquals(CONTAINERS, ContainerUtil.filterByRunEngines(CONTAINERS, "all"));
    }

    @Test
    void shouldKeepOnlyTheContainersOfTheListedEngines() {
        assertEquals(
                Arrays.asList(TestContainerId.FLINK_1_18, TestContainerId.FLINK_1_20),
                identifiers(ContainerUtil.filterByRunEngines(CONTAINERS, "flink")));
        assertEquals(
                Arrays.asList(TestContainerId.SPARK_3_3),
                identifiers(ContainerUtil.filterByRunEngines(CONTAINERS, "'spark'")));
        assertEquals(
                Arrays.asList(
                        TestContainerId.FLINK_1_18,
                        TestContainerId.FLINK_1_20,
                        TestContainerId.SPARK_3_3),
                identifiers(
                        ContainerUtil.filterByRunEngines(CONTAINERS, "[\"flink\", \"spark\"]")));
    }

    @Test
    void shouldAcceptTheZetaAliases() {
        assertEquals(
                Arrays.asList(TestContainerId.SEATUNNEL),
                identifiers(ContainerUtil.filterByRunEngines(CONTAINERS, "zeta")));
        assertEquals(
                Arrays.asList(TestContainerId.SEATUNNEL),
                identifiers(ContainerUtil.filterByRunEngines(CONTAINERS, "SEATUNNEL")));
    }

    @Test
    void shouldKeepEveryContainerWhenNoDiscoveredContainerMatches() {
        List<TestContainer> zetaOnly = Arrays.asList(container(TestContainerId.SEATUNNEL));

        assertEquals(zetaOnly, ContainerUtil.filterByRunEngines(zetaOnly, "flink,spark"));
    }

    @Test
    void shouldFailFastForUnknownEngineNames() {
        IllegalArgumentException failure =
                assertThrows(
                        IllegalArgumentException.class,
                        () -> ContainerUtil.filterByRunEngines(CONTAINERS, "zetta"));

        assertTrue(failure.getMessage().contains("zetta"), failure.getMessage());
    }

    private static List<TestContainerId> identifiers(List<TestContainer> containers) {
        return containers.stream().map(TestContainer::identifier).collect(Collectors.toList());
    }

    private static TestContainer container(TestContainerId identifier) {
        TestContainer container = mock(TestContainer.class);
        when(container.identifier()).thenReturn(identifier);
        return container;
    }
}
