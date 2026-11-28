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
package org.apache.seatunnel.connectors.seatunnel.assemblyai;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.source.Collector;
import org.apache.seatunnel.api.source.SourceReader;
import org.apache.seatunnel.api.source.SourceSplitEnumerator;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;

import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.io.ByteArrayOutputStream;
import java.io.ObjectOutputStream;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

/**
 * Covers coordinator and terminal reader transitions independently of remote transport.
 *
 * <p>The tests verify frozen windows and region scope before a new or restored split is assigned.
 */
class AssemblyAICheckpointTest {
    @Test
    @SuppressWarnings("unchecked")
    void restoresTheOriginalWindowBeforeAnyReaderAssignment() throws Exception {
        SourceSplitEnumerator.Context<AssemblyAISplit> context =
                mock(SourceSplitEnumerator.Context.class);
        AssemblyAISource initial = new AssemblyAISource(options("EU", "fixture-initial-secret"));
        AssemblyAISplit early = initial.createEnumerator(context).snapshotState(1);
        try (ByteArrayOutputStream bytes = new ByteArrayOutputStream();
                ObjectOutputStream state = new ObjectOutputStream(bytes)) {
            state.writeObject(early);
            state.flush();
            assertFalse(
                    bytes.toString(StandardCharsets.ISO_8859_1).contains("fixture-initial-secret"));
        }
        when(context.registeredReaders()).thenReturn(Set.of(0, 1));
        AssemblyAISource rotated = new AssemblyAISource(options("EU", "fixture-rotated-secret"));
        SourceSplitEnumerator<AssemblyAISplit, AssemblyAISplit> restored =
                rotated.restoreEnumerator(context, early);
        restored.run();
        ArgumentCaptor<AssemblyAISplit> split = ArgumentCaptor.forClass(AssemblyAISplit.class);
        verify(context).assignSplit(eq(0), split.capture());
        verify(context).signalNoMoreSplits(1);
        assertEquals(early.start, split.getValue().start);
        assertEquals(early.end, split.getValue().end);
        assertThrows(
                IllegalStateException.class,
                () ->
                        new AssemblyAISource(options("US", "fixture-rotated-secret"))
                                .restoreEnumerator(context, early));
    }

    @Test
    @SuppressWarnings("unchecked")
    void terminalStateNeverStartsAProviderRequest() throws Exception {
        AssemblyAIConfig config = new AssemblyAIConfig(options("US", "fixture-secret"));
        AssemblyAISplit done = config.newSplit(Instant.parse("2026-08-06T15:00:00Z"));
        done.finished = true;
        SourceReader.Context context = mock(SourceReader.Context.class);
        Collector<SeaTunnelRow> output = mock(Collector.class);
        try (AssemblyAIReader reader = new AssemblyAIReader(config, context)) {
            reader.open();
            reader.addSplits(List.of(done));
            reader.pollNext(output);
            reader.pollNext(output);
            verify(context).signalNoMoreElement();
            verifyNoInteractions(output);
        }
    }

    @Test
    void resolvesTheRegionAndFreezesDateBounds() {
        AssemblyAIConfig us = new AssemblyAIConfig(options("US", "fixture-secret"));
        AssemblyAIConfig eu = new AssemblyAIConfig(options("EU", "fixture-secret"));
        Instant timestamp = Instant.parse("2026-11-25T06:45:17Z");
        assertEquals("api.assemblyai.com", us.api.getHost());
        assertEquals("api.eu.assemblyai.com", eu.api.getHost());
        assertEquals(Instant.parse("2026-10-26T06:45:17Z"), eu.newSplit(timestamp).start);
        assertEquals(timestamp, eu.newSplit(timestamp).end);

        AssemblyAIConfig explicit =
                new AssemblyAIConfig(
                        ReadonlyConfig.fromMap(
                                Map.of(
                                        "token",
                                        "fixture-secret",
                                        "start_date",
                                        "2026-11-20T04:00:00+02:00",
                                        "end_date",
                                        "2026-11-26T00:00:00Z")));
        assertEquals(Instant.parse("2026-11-20T02:00:00Z"), explicit.newSplit(timestamp).start);
        assertEquals(timestamp, explicit.newSplit(timestamp).end);
    }

    private static ReadonlyConfig options(String region, String token) {
        return ReadonlyConfig.fromMap(
                Map.of("region", region, "token", token, "resources", List.of("transcripts")));
    }
}
