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
package org.apache.seatunnel.connectors.seatunnel.auth0;

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
 * Checks coordinator fallback, bounded search arithmetic and terminal reader state.
 *
 * <p>The dedicated engine module covers requests and savepoints against a separate HTTPS fixture.
 */
class Auth0CheckpointTest {
    @Test
    @SuppressWarnings("unchecked")
    void restoresTheUserWindowBeforeAnyReaderAssignment() throws Exception {
        SourceSplitEnumerator.Context<Auth0Split> context =
                mock(SourceSplitEnumerator.Context.class);
        Auth0Source initial = new Auth0Source(options("first.auth0.com", "fixture-initial-token"));
        Auth0Split early = initial.createEnumerator(context).snapshotState(1);
        Auth0Split detached;
        try (ByteArrayOutputStream bytes = new ByteArrayOutputStream();
                ObjectOutputStream state = new ObjectOutputStream(bytes)) {
            state.writeObject(early);
            state.flush();
            assertFalse(
                    bytes.toString(StandardCharsets.ISO_8859_1).contains("fixture-initial-token"));
            try (ObjectInputStream input =
                    new ObjectInputStream(new ByteArrayInputStream(bytes.toByteArray()))) {
                detached = (Auth0Split) input.readObject();
            }
        }
        when(context.registeredReaders()).thenReturn(Set.of(0, 1));
        Auth0Source rotated = new Auth0Source(options("first.auth0.com", "fixture-rotated-token"));
        SourceSplitEnumerator<Auth0Split, Auth0Split> restored =
                rotated.restoreEnumerator(context, detached);
        restored.run();
        ArgumentCaptor<Auth0Split> assigned = ArgumentCaptor.forClass(Auth0Split.class);
        verify(context).assignSplit(eq(0), assigned.capture());
        verify(context).signalNoMoreSplits(1);
        assertEquals(early.windows.getFirst().first, assigned.getValue().windows.getFirst().first);
        assertEquals(early.windows.getFirst().last, assigned.getValue().windows.getFirst().last);
        assertThrows(
                IllegalStateException.class,
                () ->
                        new Auth0Source(options("second.auth0.com", "fixture-rotated-token"))
                                .restoreEnumerator(context, detached));
    }

    @Test
    void splitsInclusiveRangesWithoutReusingABoundaryMillisecond() {
        Auth0Config config = new Auth0Config(options("first.auth0.com", "fixture-token"));
        Auth0Split split = config.newSplit(Instant.parse("2025-06-01T00:00:00.003Z"));
        long first = split.windows.getFirst().first;
        split.setWindowCount(1000);
        assertEquals(2, split.windows.size());
        assertEquals(first + 1, split.windows.getFirst().last);
        assertEquals(first + 2, split.windows.getLast().first);
        assertEquals(first + 3, split.windows.getLast().last);
        split.setWindowCount(1001);
        assertEquals(first, split.windows.getFirst().first);
        assertEquals(first, split.windows.getFirst().last);
        assertThrows(Auth0Resource.Failure.class, () -> split.setWindowCount(1000));
    }

    @Test
    @SuppressWarnings("unchecked")
    void terminalStateDoesNotAllocateARequestAfterRestore() throws Exception {
        Auth0Config config = new Auth0Config(options("first.auth0.com", "fixture-token"));
        Auth0Split done = config.newSplit(Instant.parse("2025-05-31T23:59:59.999Z"));
        SourceReader.Context context = mock(SourceReader.Context.class);
        Collector<SeaTunnelRow> output = mock(Collector.class);
        try (Auth0Reader reader = new Auth0Reader(config, context)) {
            reader.open();
            reader.addSplits(List.of(done));
            reader.pollNext(output);
            reader.pollNext(output);
            verify(context).signalNoMoreElement();
            verifyNoInteractions(output);
        }
    }

    @Test
    void rejectsCredentialMixingAndImplicitCustomOrigins() {
        assertThrows(
                IllegalArgumentException.class,
                () -> new Auth0Config(options("auth0-peer", "fixture-token")));
        assertThrows(
                IllegalArgumentException.class,
                () ->
                        new Auth0Config(
                                ReadonlyConfig.fromMap(
                                        Map.of(
                                                "tenant_url",
                                                "https://first.auth0.com",
                                                "token",
                                                "fixture-token",
                                                "client_secret",
                                                "fixture-secret",
                                                "resources",
                                                List.of("clients")))));
    }

    @Test
    void defaultsToTheHistoricalUserStartWithoutExtendingTheFirstAnnualWindow() {
        Auth0Config config =
                new Auth0Config(
                        ReadonlyConfig.fromMap(
                                Map.of(
                                        "tenant_url",
                                        "https://first.auth0.com",
                                        "token",
                                        "fixture-token",
                                        "resources",
                                        List.of("users"))));
        Auth0Split split = config.newSplit(Instant.parse("2014-06-01T00:00:00Z"));
        assertEquals(
                Instant.parse("2013-01-01T00:00:00Z").toEpochMilli(),
                split.windows.getFirst().first);
        assertEquals(
                Instant.parse("2013-12-31T23:59:59.999Z").toEpochMilli(),
                split.windows.getFirst().last);
    }

    private static ReadonlyConfig options(String host, String token) {
        return ReadonlyConfig.fromMap(
                Map.of(
                        "tenant_url",
                        "https://" + host,
                        "token",
                        token,
                        "resources",
                        List.of("users"),
                        "start_date",
                        "2025-06-01T02:00:00+02:00"));
    }
}
