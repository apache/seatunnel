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
package org.apache.seatunnel.connectors.seatunnel.apptivo;

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
 * Exercises early coordinator restore and terminal reader state without opening a network request.
 *
 * <p>These component cases isolate scope and credential rotation from transport scheduling.
 */
class ApptivoCheckpointTest {
    @Test
    @SuppressWarnings("unchecked")
    void bindsCheckpointToFirmWhileAllowingCredentialRotation() throws Exception {
        SourceSplitEnumerator.Context<ApptivoSplit> context =
                mock(SourceSplitEnumerator.Context.class);
        ApptivoSource initial =
                new ApptivoSource(options("7", "fixture-api-secret", "fixture-access-secret"));
        ApptivoSplit state = initial.createEnumerator(context).snapshotState(1);
        try (ByteArrayOutputStream bytes = new ByteArrayOutputStream();
                ObjectOutputStream output = new ObjectOutputStream(bytes)) {
            output.writeObject(state);
            output.flush();
            String serialized = bytes.toString(StandardCharsets.ISO_8859_1);
            assertFalse(serialized.contains("fixture-api-secret"));
            assertFalse(serialized.contains("fixture-access-secret"));
        }

        when(context.registeredReaders()).thenReturn(Set.of(0, 1));
        ApptivoSource rotated = new ApptivoSource(options("7", "rotated-api", "rotated-access"));
        SourceSplitEnumerator<ApptivoSplit, ApptivoSplit> restored =
                rotated.restoreEnumerator(context, state);
        restored.run();
        ArgumentCaptor<ApptivoSplit> assigned = ArgumentCaptor.forClass(ApptivoSplit.class);
        verify(context).assignSplit(eq(0), assigned.capture());
        verify(context).signalNoMoreSplits(1);
        assertEquals("7", assigned.getValue().firm);
        assertEquals(0, assigned.getValue().offset);

        ApptivoSource otherFirm = new ApptivoSource(options("8", "rotated-api", "rotated-access"));
        assertThrows(
                IllegalStateException.class, () -> otherFirm.restoreEnumerator(context, state));
    }

    @Test
    @SuppressWarnings("unchecked")
    void signalsTerminalRestoreWithoutEmittingOrFetching() throws Exception {
        ApptivoConfig config = new ApptivoConfig(options("7", "fixture-api", "fixture-access"));
        ApptivoSplit finished = new ApptivoSplit(config);
        finished.nextResource();
        SourceReader.Context context = mock(SourceReader.Context.class);
        Collector<SeaTunnelRow> output = mock(Collector.class);
        try (ApptivoReader reader = new ApptivoReader(config, context)) {
            reader.open();
            reader.addSplits(List.of(finished));
            reader.pollNext(output);
            reader.pollNext(output);
            verify(context).signalNoMoreElement();
            verifyNoInteractions(output);
        }
    }

    private static ReadonlyConfig options(String firm, String token, String access) {
        return ReadonlyConfig.fromMap(
                Map.of(
                        "token",
                        token,
                        "access_key",
                        access,
                        "firm_id",
                        firm,
                        "resources",
                        List.of("cases")));
    }
}
