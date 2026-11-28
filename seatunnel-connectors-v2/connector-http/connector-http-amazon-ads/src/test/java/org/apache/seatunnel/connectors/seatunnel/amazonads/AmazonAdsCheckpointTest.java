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
package org.apache.seatunnel.connectors.seatunnel.amazonads;

import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.ObjectMapper;

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

/**
 * Verifies elapsed cutoff recovery before assignment, role-specific calendar and region contracts,
 * and terminal reader completion without contacting an authorization endpoint.
 */
class AmazonAdsCheckpointTest {
    @Test
    @SuppressWarnings("unchecked")
    void preservesAnEarlyCoordinatorCheckpointWithoutSecrets() throws Exception {
        SourceSplitEnumerator.Context<AmazonAdsSplit> context =
                mock(SourceSplitEnumerator.Context.class);
        AmazonAdsSource initial =
                new AmazonAdsSource(config("NA", "fixture-client-secret-a", "fixture-refresh-a"));
        AmazonAdsSplit early = initial.createEnumerator(context).snapshotState(1);
        try (ByteArrayOutputStream output = new ByteArrayOutputStream();
                ObjectOutputStream serial = new ObjectOutputStream(output)) {
            serial.writeObject(early);
            serial.flush();
            String body = output.toString(StandardCharsets.ISO_8859_1);
            assertFalse(body.contains("fixture-client-secret-a"));
            assertFalse(body.contains("fixture-refresh-a"));
        }
        AmazonAdsSource rotated =
                new AmazonAdsSource(config("NA", "fixture-client-secret-b", "fixture-refresh-b"));
        when(context.registeredReaders()).thenReturn(Collections.singleton(0));
        SourceSplitEnumerator<AmazonAdsSplit, AmazonAdsSplit> restored =
                rotated.restoreEnumerator(context, early);
        restored.addSplitsBack(Collections.emptyList(), 0);
        restored.run();
        ArgumentCaptor<AmazonAdsSplit> split = ArgumentCaptor.forClass(AmazonAdsSplit.class);
        verify(context).assignSplit(eq(0), split.capture());
        assertEquals(early.cutoff, split.getValue().cutoff);
        assertEquals(0, split.getValue().profile);
        AmazonAdsSource foreign =
                new AmazonAdsSource(config("EU", "fixture-client-secret-b", "fixture-refresh-b"));
        assertThrows(
                IllegalArgumentException.class, () -> foreign.restoreEnumerator(context, early));
    }

    @Test
    @SuppressWarnings("unchecked")
    void marksACompletedRestoredReaderWithoutAuthorization() throws Exception {
        AmazonAdsConfig config =
                new AmazonAdsConfig(config("NA", "fixture-secret", "fixture-refresh"));
        AmazonAdsSplit terminal = config.startAt(Instant.parse("2026-11-28T00:00:00Z"));
        terminal.profileDigest = AmazonAdsRecord.hash("empty");
        terminal.finished = true;
        SourceReader.Context context = mock(SourceReader.Context.class);
        Collector<SeaTunnelRow> output = mock(Collector.class);
        try (AmazonAdsReader reader = new AmazonAdsReader(config, context)) {
            reader.open();
            reader.addSplits(Collections.singletonList(terminal));
            reader.pollNext(output);
            reader.pollNext(output);
            verify(context).signalNoMoreElement();
            verifyNoInteractions(output);
        }
    }

    @Test
    void separatesRegionalApiAndProfileAttributionCalendars() {
        AmazonAdsConfig north =
                new AmazonAdsConfig(config("NA", "fixture-secret", "fixture-refresh"));
        AmazonAdsConfig europe =
                new AmazonAdsConfig(config("EU", "fixture-secret", "fixture-refresh"));
        AmazonAdsConfig far =
                new AmazonAdsConfig(config("FE", "fixture-secret", "fixture-refresh"));
        assertEquals("advertising-api.amazon.com", north.api.host);
        assertEquals("advertising-api-eu.amazon.com", europe.api.host);
        assertEquals("advertising-api-fe.amazon.com", far.api.host);
        assertEquals("api.amazon.com", far.oauth.host);
        Instant utc = Instant.parse("2026-06-03T01:00:00Z");
        ObjectMapper json = new ObjectMapper();
        AmazonAdsPartition pacific =
                new AmazonAdsPartition(
                        north,
                        new AmazonAdsRecord(
                                "111",
                                json.createObjectNode().put("timezone", "Pacific/Pago_Pago")));
        AmazonAdsPartition japan =
                new AmazonAdsPartition(
                        north,
                        new AmazonAdsRecord(
                                "222", json.createObjectNode().put("timezone", "Asia/Tokyo")));
        assertEquals(
                LocalDate.parse("2026-06-02"),
                pacific.attribution(north, AmazonAdsResource.ATTRIBUTION_REPORT_PRODUCTS, utc).to);
        assertEquals(
                LocalDate.parse("2026-06-03"),
                japan.attribution(north, AmazonAdsResource.ATTRIBUTION_REPORT_PRODUCTS, utc).to);
        AmazonAdsPartition.Window report =
                AmazonAdsPartition.firstReport(
                        north,
                        AmazonAdsResource.SPONSORED_PRODUCTS_CAMPAIGNS_REPORT_STREAM,
                        utc,
                        null);
        assertEquals(LocalDate.parse("2026-05-30"), report.from);
        assertEquals(LocalDate.parse("2026-06-03"), report.to);
    }

    private static ReadonlyConfig config(String region, String secret, String token) {
        return ReadonlyConfig.fromMap(
                Map.of(
                        "client_id", "fixture-client",
                        "client_secret", secret,
                        "refresh_token", token,
                        "region", region,
                        "resources", Collections.singletonList("profiles")));
    }
}
