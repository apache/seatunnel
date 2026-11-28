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
package org.apache.seatunnel.connectors.seatunnel.akeneo;

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
import java.util.Collections;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

class AkeneoCheckpointTest {
    @Test
    @SuppressWarnings("unchecked")
    void restoresEarlyCoordinatorStateWithoutPersistingCredentials() throws Exception {
        String origin = "https://pim.example.test";
        SourceSplitEnumerator.Context<AkeneoSplit> context =
                mock(SourceSplitEnumerator.Context.class);
        AkeneoSource initial = source(origin, "fixture-secret-before", "fixture-password-before");
        AkeneoSplit early = initial.createEnumerator(context).snapshotState(1);
        try (ByteArrayOutputStream bytes = new ByteArrayOutputStream();
                ObjectOutputStream serial = new ObjectOutputStream(bytes)) {
            serial.writeObject(early);
            serial.flush();
            String encoded = bytes.toString(StandardCharsets.ISO_8859_1);
            assertFalse(encoded.contains("fixture-secret-before"));
            assertFalse(encoded.contains("fixture-password-before"));
            assertFalse(encoded.contains("fixture-user-id"));
        }

        AkeneoSource rotated = source(origin, "fixture-secret-after", "fixture-password-after");
        when(context.registeredReaders()).thenReturn(Collections.singleton(0));
        SourceSplitEnumerator<AkeneoSplit, AkeneoSplit> restored =
                rotated.restoreEnumerator(context, early);
        restored.addSplitsBack(Collections.emptyList(), 0);
        restored.run();
        ArgumentCaptor<AkeneoSplit> assignment = ArgumentCaptor.forClass(AkeneoSplit.class);
        verify(context).assignSplit(eq(0), assignment.capture());
        assertEquals(early.scope, assignment.getValue().scope);
        assertEquals(1, assignment.getValue().root.number);
        assertEquals(0, assignment.getValue().group);
        assertThrows(
                IllegalArgumentException.class,
                () ->
                        source("https://other.example.test", "new-secret", "new-password")
                                .restoreEnumerator(context, early));
    }

    @Test
    @SuppressWarnings("unchecked")
    void restoresTerminalAndSignalsCompletionOnlyOnce() throws Exception {
        AkeneoSource source =
                source("https://pim.example.test", "fixture-secret", "fixture-password");
        SourceSplitEnumerator.Context<AkeneoSplit> coordinator =
                mock(SourceSplitEnumerator.Context.class);
        AkeneoSplit terminal = source.createEnumerator(coordinator).snapshotState(1);
        terminal.nextGroup();

        SourceReader.Context context = mock(SourceReader.Context.class);
        Collector<SeaTunnelRow> collector = mock(Collector.class);
        try (SourceReader<SeaTunnelRow, AkeneoSplit> reader = source.createReader(context)) {
            reader.open();
            reader.addSplits(Collections.singletonList(terminal));
            reader.pollNext(collector);
            reader.pollNext(collector);
            assertEquals(1, reader.snapshotState(2).get(0).group);
            verify(context).signalNoMoreElement();
            verifyNoInteractions(collector);
        }
    }

    private static AkeneoSource source(String origin, String secret, String password) {
        return new AkeneoSource(
                ReadonlyConfig.fromMap(
                        Map.of(
                                "base_url",
                                origin,
                                "client_id",
                                "fixture-client",
                                "client_secret",
                                secret,
                                "username",
                                "fixture-user-id",
                                "password",
                                password,
                                "resources",
                                Collections.singletonList("products"))));
    }
}
