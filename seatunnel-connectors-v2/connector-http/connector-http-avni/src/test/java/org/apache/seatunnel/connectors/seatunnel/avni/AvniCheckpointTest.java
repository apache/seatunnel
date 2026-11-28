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
package org.apache.seatunnel.connectors.seatunnel.avni;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.source.Collector;
import org.apache.seatunnel.api.source.SourceReader;
import org.apache.seatunnel.api.source.SourceSplitEnumerator;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;

import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

/**
 * Checks bounded time-window calculation and coordinator restore scope.
 *
 * <p>The engine integration module independently exercises Avni and Cognito requests.
 */
class AvniCheckpointTest {
    private static final Instant CLOCK = Instant.parse("2025-07-01T00:00:10.123456Z");

    @Test
    @SuppressWarnings("unchecked")
    void restoresInitialWindowAndAllowsPasswordRotationForTheSameAccount() throws Exception {
        SourceSplitEnumerator.Context<AvniSplit> context =
                mock(SourceSplitEnumerator.Context.class);
        AvniSource original = new AvniSource(options("fixture-account", "fixture-old-password"));
        AvniSplit initial = original.createEnumerator(context).snapshotState(1);
        AvniSplit saved;
        try (ByteArrayOutputStream bytes = new ByteArrayOutputStream();
                ObjectOutputStream output = new ObjectOutputStream(bytes)) {
            output.writeObject(initial);
            output.flush();
            String wire = bytes.toString(StandardCharsets.ISO_8859_1);
            assertFalse(wire.contains("fixture-account"));
            assertFalse(wire.contains("fixture-old-password"));
            try (ObjectInputStream input =
                    new ObjectInputStream(new ByteArrayInputStream(bytes.toByteArray()))) {
                saved = (AvniSplit) input.readObject();
            }
        }
        when(context.registeredReaders()).thenReturn(Set.of(0, 1));
        AvniSource rotated = new AvniSource(options("fixture-account", "fixture-new-password"));
        SourceSplitEnumerator<AvniSplit, AvniSplit> restored =
                rotated.restoreEnumerator(context, saved);
        restored.run();
        ArgumentCaptor<AvniSplit> active = ArgumentCaptor.forClass(AvniSplit.class);
        verify(context).assignSplit(eq(0), active.capture());
        verify(context).signalNoMoreSplits(1);
        assertEquals(initial.start, active.getValue().start);
        assertEquals(initial.end, active.getValue().end);
        assertThrows(
                IllegalStateException.class,
                () ->
                        new AvniSource(options("different-account", "fixture-new-password"))
                                .restoreEnumerator(context, saved));
    }

    @Test
    void appliesTheTenSecondCutoffAndAllowsAnEarlierExplicitEnd() {
        AvniSplit defaults =
                new AvniConfig(options("fixture-account", "fixture-password")).newSplit(CLOCK);
        assertEquals(Instant.parse("2025-07-01T00:00:00.123Z"), defaults.end);
        Map<String, Object> explicit = values("fixture-account", "fixture-password");
        explicit.put("end_date", "2025-06-30T19:59:59.999-04:00");
        AvniSplit earlier = new AvniConfig(ReadonlyConfig.fromMap(explicit)).newSplit(CLOCK);
        assertEquals(Instant.parse("2025-06-30T23:59:59.999Z"), earlier.end);
        explicit.put("end_date", "2025-07-01T00:00:45.123Z");
        assertEquals(
                defaults.end, new AvniConfig(ReadonlyConfig.fromMap(explicit)).newSplit(CLOCK).end);
        assertEquals(Instant.parse("2025-06-01T00:00:00Z"), defaults.start);
    }

    @Test
    @SuppressWarnings("unchecked")
    void aWindowWhoseCutoffCannotFollowItsStartEndsWithoutOutput() throws Exception {
        AvniConfig config = new AvniConfig(options("fixture-account", "fixture-password"));
        AvniSplit empty = config.newSplit(Instant.parse("2025-06-01T00:00:10Z"));
        assertTrue(empty.finished());
        SourceReader.Context context = mock(SourceReader.Context.class);
        Collector<SeaTunnelRow> output = mock(Collector.class);
        try (AvniReader reader = new AvniReader(config, context)) {
            reader.open();
            reader.addSplits(List.of(empty));
            reader.pollNext(output);
            reader.pollNext(output);
            verify(context).signalNoMoreElement();
            verifyNoInteractions(output);
        }
    }

    @Test
    void requiresAnExplicitTrustBoundaryAndTwoDifferentOrigins() {
        Map<String, Object> values = values("fixture-account", "fixture-password");
        values.put("api_origin", "https://avni-peer:22443");
        assertThrows(
                IllegalArgumentException.class,
                () -> new AvniConfig(ReadonlyConfig.fromMap(values)));
        values.put("allow_custom_endpoints", true);
        values.put("identity_origin", "https://avni-peer:22443/");
        assertThrows(
                IllegalArgumentException.class,
                () -> new AvniConfig(ReadonlyConfig.fromMap(values)));
        values.put("identity_origin", "https://avni-idp:22444");
        assertEquals("avni-idp", new AvniConfig(ReadonlyConfig.fromMap(values)).identity.getHost());
    }

    private static ReadonlyConfig options(String username, String password) {
        return ReadonlyConfig.fromMap(values(username, password));
    }

    private static Map<String, Object> values(String username, String password) {
        return new HashMap<>(
                Map.of(
                        "username",
                        username,
                        "password",
                        password,
                        "resources",
                        List.of("subjects"),
                        "start_date",
                        "2025-06-01T00:00:00.000Z"));
    }
}
