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

package org.apache.seatunnel.engine.common.runtime.source;

import org.apache.seatunnel.api.source.SeaTunnelSource;
import org.apache.seatunnel.api.source.managed.ManagedSourceCapability;
import org.apache.seatunnel.engine.common.config.server.ManagedSourceRuntimeConfig;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

class ManagedSourceRuntimeSelectorTest {

    @Test
    void shouldKeepLegacyAsTheDefaultWhenFeatureIsDisabled() {
        SeaTunnelSource<?, ?, ?> source = Mockito.mock(SeaTunnelSource.class);
        Mockito.when(source.getPluginName()).thenReturn("FakeSource");
        ManagedSourceRuntimeConfig config = new ManagedSourceRuntimeConfig();

        Assertions.assertEquals(
                ManagedSourceRuntimeMode.LEGACY,
                ManagedSourceRuntimeSelector.select(source, config).getMode());
    }

    @Test
    void shouldSelectCompatibleCapabilityWhenFeatureEnabled() {
        SeaTunnelSource<?, ?, ?> source = Mockito.mock(SeaTunnelSource.class);
        Mockito.when(source.getPluginName()).thenReturn("FakeSource");
        Mockito.when(source.getManagedSourceCapability())
                .thenReturn(
                        ManagedSourceCapability.builder()
                                .supportsManagedReader(true)
                                .supportsManagedCoordinator(true)
                                .supportsBoundedPoll(true)
                                .supportsWakeup(true)
                                .supportsAttemptFencing(true)
                                .usesSourceEvents(false)
                                .supportsAsyncEnumerator(true)
                                .stableSplitIdentifiers(true)
                                .build());
        ManagedSourceRuntimeConfig config = new ManagedSourceRuntimeConfig();
        config.setEnabled(true);

        ManagedSourceRuntimeSelection selection =
                ManagedSourceRuntimeSelector.select(source, config);

        Assertions.assertEquals(
                ManagedSourceRuntimeMode.MANAGED_READER_AND_COORDINATOR, selection.getMode());
    }

    @Test
    void shouldFailClosedWhenEnabledConnectorDeclaresNoCapability() {
        SeaTunnelSource<?, ?, ?> source = Mockito.mock(SeaTunnelSource.class);
        Mockito.when(source.getPluginName()).thenReturn("FakeSource");
        Mockito.when(source.getManagedSourceCapability())
                .thenReturn(ManagedSourceCapability.legacy());
        ManagedSourceRuntimeConfig config = new ManagedSourceRuntimeConfig();
        config.setEnabled(true);

        Assertions.assertThrows(
                IllegalStateException.class,
                () -> ManagedSourceRuntimeSelector.select(source, config));
    }

    @Test
    void shouldRejectManagedConnectorsThatUseSourceEvents() {
        SeaTunnelSource<?, ?, ?> source = Mockito.mock(SeaTunnelSource.class);
        Mockito.when(source.getPluginName()).thenReturn("FakeSource");
        Mockito.when(source.getManagedSourceCapability())
                .thenReturn(
                        ManagedSourceCapability.builder()
                                .supportsManagedCoordinator(true)
                                .usesSourceEvents(true)
                                .supportsAsyncEnumerator(true)
                                .build());
        ManagedSourceRuntimeConfig config = new ManagedSourceRuntimeConfig();
        config.setEnabled(true);

        Assertions.assertThrows(
                IllegalStateException.class,
                () -> ManagedSourceRuntimeSelector.select(source, config));
    }

    @Test
    void shouldRejectManagedCoordinatorsWithoutEventLoopSafeRunContract() {
        SeaTunnelSource<?, ?, ?> source = Mockito.mock(SeaTunnelSource.class);
        Mockito.when(source.getPluginName()).thenReturn("FakeSource");
        Mockito.when(source.getManagedSourceCapability())
                .thenReturn(
                        ManagedSourceCapability.builder()
                                .supportsManagedCoordinator(true)
                                .usesSourceEvents(false)
                                .build());
        ManagedSourceRuntimeConfig config = new ManagedSourceRuntimeConfig();
        config.setEnabled(true);

        Assertions.assertThrows(
                IllegalStateException.class,
                () -> ManagedSourceRuntimeSelector.select(source, config));
    }

    @Test
    void shouldRequireManagedCapabilitiesToDeclareSourceEventUsage() {
        Assertions.assertThrows(
                IllegalArgumentException.class,
                () ->
                        ManagedSourceCapability.builder()
                                .supportsManagedCoordinator(true)
                                .supportsAsyncEnumerator(true)
                                .build());
    }

    @Test
    void shouldRejectUnsafeCapacityConfiguration() {
        ManagedSourceRuntimeConfig config = new ManagedSourceRuntimeConfig();
        config.setReaderReservedControlCommands(config.getReaderMailboxMaxCommands());

        Assertions.assertThrows(IllegalArgumentException.class, config::validate);
    }
}
