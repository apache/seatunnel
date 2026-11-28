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
package org.apache.seatunnel.connectors.seatunnel.appleads;

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
import java.time.LocalDate;
import java.util.HashMap;
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
 * Covers pre-assignment, closed organization calendars and terminal Apple Ads checkpoint state.
 *
 * <p>The component cases isolate coordinator and reader transitions that a Zeta job cannot schedule
 * precisely.
 */
class AppleAdsCheckpointTest {
    @Test
    @SuppressWarnings("unchecked")
    void retainsThePreAssignmentCutoffAcrossCredentialRotation() throws Exception {
        SourceSplitEnumerator.Context<AppleAdsSplit> context =
                mock(SourceSplitEnumerator.Context.class);
        AppleAdsSource source = new AppleAdsSource(readonly("first-fixture-secret"));
        AppleAdsSplit checkpoint = source.createEnumerator(context).snapshotState(1);
        try (ByteArrayOutputStream bytes = new ByteArrayOutputStream();
                ObjectOutputStream state = new ObjectOutputStream(bytes)) {
            state.writeObject(checkpoint);
            state.flush();
            String stored = bytes.toString(StandardCharsets.ISO_8859_1);
            assertFalse(stored.contains("first-fixture-secret"));
            assertFalse(stored.contains("fixture-user"));
        }
        when(context.registeredReaders()).thenReturn(Set.of(0, 1));
        AppleAdsSource rotated = new AppleAdsSource(readonly("rotated-fixture-secret"));
        SourceSplitEnumerator<AppleAdsSplit, AppleAdsSplit> restored =
                rotated.restoreEnumerator(context, checkpoint);
        restored.run();
        ArgumentCaptor<AppleAdsSplit> assigned = ArgumentCaptor.forClass(AppleAdsSplit.class);
        verify(context).assignSplit(eq(0), assigned.capture());
        verify(context).signalNoMoreSplits(1);
        assertEquals(checkpoint.cutoff, assigned.getValue().cutoff);
        assertEquals(checkpoint.last, assigned.getValue().last);

        Map<String, Object> other = options("rotated-fixture-secret");
        other.put("ad_account_id", "2");
        assertThrows(
                IllegalStateException.class,
                () ->
                        new AppleAdsSource(ReadonlyConfig.fromMap(other))
                                .restoreEnumerator(context, checkpoint));
    }

    @Test
    @SuppressWarnings("unchecked")
    void signalsTerminalCompletionWithoutIssuingOAuth() throws Exception {
        AppleAdsConfig cfg = new AppleAdsConfig(readonly("fixture-secret"));
        AppleAdsSplit terminal = new AppleAdsSplit(cfg, Instant.parse("2026-10-20T01:00:00Z"));
        terminal.resource = 1;
        SourceReader.Context context = mock(SourceReader.Context.class);
        Collector<SeaTunnelRow> collector = mock(Collector.class);
        try (AppleAdsReader reader = new AppleAdsReader(cfg, context)) {
            reader.open();
            reader.addSplits(List.of(terminal));
            reader.pollNext(collector);
            reader.pollNext(collector);
            verify(context).signalNoMoreElement();
            verifyNoInteractions(collector);
        }
    }

    @Test
    void closesOrganizationDatesAtLocalMidnightAndSeparatesModeIds() {
        Instant at = Instant.parse("2026-06-11T02:00:00Z");
        Map<String, Object> zone = options("fixture-secret");
        zone.put("resources", List.of("ads_report_daily"));
        zone.put("timezone", "ORTZ");
        zone.put("organization_timezone", "America/Los_Angeles");
        zone.put("start_date", "2026-06-01");
        zone.put("report_cursor_date", "2026-06-08");
        zone.put("lookback_days", 2);
        AppleAdsConfig cfg = new AppleAdsConfig(ReadonlyConfig.fromMap(zone));
        assertEquals(LocalDate.parse("2026-06-06"), cfg.first(at));
        assertEquals(LocalDate.parse("2026-06-09"), cfg.last(at));
        assertEquals("https://api.ads.apple.com", cfg.origin.toString());

        zone.put("api_mode", "LEGACY_V5");
        zone.put("org_id", "42");
        assertThrows(
                IllegalArgumentException.class,
                () -> new AppleAdsConfig(ReadonlyConfig.fromMap(zone)));
        zone.remove("ad_account_id");
        AppleAdsConfig legacy = new AppleAdsConfig(ReadonlyConfig.fromMap(zone));
        assertEquals("https://api.searchads.apple.com", legacy.origin.toString());
        assertEquals("orgId", legacy.mode.contextKey);
    }

    private static ReadonlyConfig readonly(String secret) {
        return ReadonlyConfig.fromMap(options(secret));
    }

    private static Map<String, Object> options(String secret) {
        return new HashMap<>(
                Map.of(
                        "ad_account_id",
                        "1",
                        "client_id",
                        "fixture-user",
                        "client_secret",
                        secret,
                        "resources",
                        List.of("campaigns")));
    }
}
