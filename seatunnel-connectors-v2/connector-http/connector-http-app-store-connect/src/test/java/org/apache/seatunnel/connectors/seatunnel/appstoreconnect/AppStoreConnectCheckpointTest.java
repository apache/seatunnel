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
package org.apache.seatunnel.connectors.seatunnel.appstoreconnect;

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
import java.security.KeyPairGenerator;
import java.security.spec.ECGenParameterSpec;
import java.time.Instant;
import java.util.Base64;
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
 * Covers coordinator state before assignment and terminal reader behavior independent of transport.
 *
 * <p>These cases isolate the state transitions that the storage-facing E2E tests cannot schedule
 * precisely.
 */
class AppStoreConnectCheckpointTest {
    @Test
    @SuppressWarnings("unchecked")
    void preservesThePreAssignmentClockAndAllowsKeyRotation() throws Exception {
        SourceSplitEnumerator.Context<AppStoreConnectSplit> context =
                mock(SourceSplitEnumerator.Context.class);
        String originalKey = newKey();
        AppStoreConnectSource initial =
                new AppStoreConnectSource(config("team-a", "original-kid", originalKey));
        AppStoreConnectSplit early = initial.createEnumerator(context).snapshotState(1);
        try (ByteArrayOutputStream bytes = new ByteArrayOutputStream();
                ObjectOutputStream serialized = new ObjectOutputStream(bytes)) {
            serialized.writeObject(early);
            serialized.flush();
            String stored = bytes.toString(StandardCharsets.ISO_8859_1);
            assertFalse(stored.contains(originalKey));
            assertFalse(stored.contains("original-kid"));
        }

        String rotatedKey = newKey();
        AppStoreConnectSource rotated =
                new AppStoreConnectSource(config("team-a", "rotated-kid", rotatedKey));
        when(context.registeredReaders()).thenReturn(Set.of(0, 1));
        SourceSplitEnumerator<AppStoreConnectSplit, AppStoreConnectSplit> restored =
                rotated.restoreEnumerator(context, early);
        assertEquals(1, restored.currentUnassignedSplitSize());
        restored.run();
        ArgumentCaptor<AppStoreConnectSplit> assigned =
                ArgumentCaptor.forClass(AppStoreConnectSplit.class);
        verify(context).assignSplit(eq(0), assigned.capture());
        verify(context).signalNoMoreSplits(1);
        assertEquals(early.cutoff, assigned.getValue().cutoff);
        assertEquals(0, assigned.getValue().resource);
        assertEquals(0, restored.currentUnassignedSplitSize());
        assertThrows(
                IllegalStateException.class,
                () ->
                        new AppStoreConnectSource(config("team-b", "rotated-kid", rotatedKey))
                                .restoreEnumerator(context, early));
    }

    @Test
    @SuppressWarnings("unchecked")
    void terminalStateNeverOpensAnApiRequest() throws Exception {
        AppStoreConnectConfig config =
                new AppStoreConnectConfig(config("team-a", "kid-a", newKey()));
        AppStoreConnectSplit done =
                new AppStoreConnectSplit(config.scope, Instant.parse("2026-06-03T12:00:00Z"));
        done.nextResource();
        SourceReader.Context context = mock(SourceReader.Context.class);
        Collector<SeaTunnelRow> output = mock(Collector.class);
        try (AppStoreConnectReader reader = new AppStoreConnectReader(config, context)) {
            reader.open();
            reader.addSplits(List.of(done));
            reader.pollNext(output);
            reader.pollNext(output);
            verify(context).signalNoMoreElement();
            verifyNoInteractions(output);
        }
    }

    private static ReadonlyConfig config(String issuer, String kid, String secret) {
        return ReadonlyConfig.fromMap(
                Map.of(
                        "issuer_id",
                        issuer,
                        "key_id",
                        kid,
                        "secret_key",
                        secret,
                        "resources",
                        List.of("list_id_apps")));
    }

    private static String newKey() throws Exception {
        KeyPairGenerator ec = KeyPairGenerator.getInstance("EC");
        ec.initialize(new ECGenParameterSpec("secp256r1"));
        return Base64.getEncoder().encodeToString(ec.generateKeyPair().getPrivate().getEncoded());
    }
}
