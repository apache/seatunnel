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

package org.apache.seatunnel.connectors.seatunnel.activecampaign;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.source.Collector;
import org.apache.seatunnel.api.source.SourceReader;
import org.apache.seatunnel.api.source.SourceSplitEnumerator;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;

import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.util.Collections;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

class ActiveCampaignCheckpointTest {
    private static final ActiveCampaignSourceOptions.Settings SETTINGS =
            new ActiveCampaignSourceOptions.Settings(
                    ReadonlyConfig.fromMap(
                            Map.of(
                                    "base_url",
                                    "https://tenant.example.test",
                                    "token",
                                    "fixture-token")));

    @Test
    @SuppressWarnings("unchecked")
    void preservesEnumeratorFallbackWhenReaderNeverReceivedItsAssignment() {
        SourceSplitEnumerator.Context<ActiveCampaignSplit> context =
                mock(SourceSplitEnumerator.Context.class);
        when(context.registeredReaders()).thenReturn(Collections.singleton(0));
        ActiveCampaignSource.Enumerator initial =
                new ActiveCampaignSource.Enumerator(context, SETTINGS, SETTINGS.initial());
        ActiveCampaignSource.Enumerator restored =
                new ActiveCampaignSource.Enumerator(context, SETTINGS, initial.snapshotState(1));
        restored.addSplitsBack(Collections.emptyList(), 0);
        restored.run();
        ArgumentCaptor<ActiveCampaignSplit> capture =
                ArgumentCaptor.forClass(ActiveCampaignSplit.class);
        verify(context).assignSplit(eq(0), capture.capture());
        assertEquals(0, capture.getValue().resource);
        assertEquals(0, capture.getValue().offset);
        assertEquals(0, capture.getValue().emitted);
    }

    @Test
    @SuppressWarnings("unchecked")
    void reassignsTerminalReaderStateWithoutStartingARequest() throws Exception {
        SourceReader.Context context = mock(SourceReader.Context.class);
        Collector<SeaTunnelRow> output = mock(Collector.class);
        ActiveCampaignSplit terminal = SETTINGS.initial();
        terminal.resource = SETTINGS.resources.size();
        try (ActiveCampaignReader reader = new ActiveCampaignReader(SETTINGS, context)) {
            assertTrue(reader.snapshotState(1).isEmpty());
            reader.open();
            reader.addSplits(Collections.singletonList(terminal));
            reader.pollNext(output);
            reader.pollNext(output);
            assertEquals(terminal.resource, reader.snapshotState(2).get(0).resource);
            verify(context).signalNoMoreElement();
            verifyNoInteractions(output);
        }
    }
}
