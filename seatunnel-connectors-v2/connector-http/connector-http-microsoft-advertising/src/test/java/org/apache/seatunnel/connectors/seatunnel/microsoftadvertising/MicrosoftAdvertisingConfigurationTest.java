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
package org.apache.seatunnel.connectors.seatunnel.microsoftadvertising;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.connectors.seatunnel.microsoftadvertising.MicrosoftAdvertisingCatalog.Kind;
import org.apache.seatunnel.connectors.seatunnel.microsoftadvertising.MicrosoftAdvertisingCatalog.Resource;

import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Exercises catalog, cursor and boundary logic that is shared across provider reads.
 *
 * <p>These checks complement the engine suite's independent TLS provider and output assertions.
 */
class MicrosoftAdvertisingConfigurationTest {
    private static final Instant NOW = Instant.parse("2026-07-18T12:00:00Z");

    @Test
    void includesEveryFixedResourceAndAppendsOneCustomReport() {
        MicrosoftAdvertisingConfig config =
                config(
                        Map.of(
                                "custom_reports",
                                List.of(
                                        Map.of(
                                                "name", "DailyClicks",
                                                "reporting_object",
                                                        "AccountPerformanceReportRequest",
                                                "report_aggregation", "Daily",
                                                "report_columns",
                                                        List.of("AccountId", "Clicks")))));
        assertEquals(78, config.resources.size());
        assertEquals(
                4,
                config.resources.stream().filter(resource -> resource.kind == Kind.ENTITY).count());
        assertEquals(
                8,
                config.resources.stream().filter(resource -> resource.kind == Kind.BULK).count());
        Resource custom = config.resources.get(77);
        assertEquals("daily_clicks", custom.name);
        assertEquals(List.of("AccountId", "Clicks", "TimePeriod"), custom.columns);
        assertTrue(custom.identity.isEmpty());
    }

    @Test
    void rejectsAnInvalidCustomColumnAndDuplicateResources() {
        assertThrows(
                IllegalArgumentException.class,
                () ->
                        config(
                                Map.of(
                                        "custom_reports",
                                        List.of(
                                                Map.of(
                                                        "name", "WrongColumns",
                                                        "reporting_object",
                                                                "AccountPerformanceReportRequest",
                                                        "report_aggregation", "Daily",
                                                        "report_columns",
                                                                List.of("UnknownColumn"))))));
        assertThrows(
                IllegalArgumentException.class,
                () -> config(Map.of("resources", List.of("accounts", "accounts"))));
    }

    @Test
    void freezesLookbackAndAppliesReportSpecificRetention() {
        MicrosoftAdvertisingConfig config =
                config(
                        Map.of(
                                "resources",
                                List.of("geographic_performance_report_hourly"),
                                "reports_start_date",
                                "2025-01-01",
                                "reports_resume_date",
                                "2026-06-01",
                                "lookback_window",
                                5));
        MicrosoftAdvertisingSplit split = config.newSplit(NOW);
        assertEquals(LocalDate.of(2026, 5, 27), split.start);
        assertEquals(LocalDate.of(2026, 7, 18), split.end);
        assertEquals(
                LocalDate.of(2026, 7, 4),
                MicrosoftAdvertisingApi.reportStart(config.resources.get(0), split));
        assertThrows(
                MicrosoftAdvertisingFailure.class,
                () -> split.validate(config(Map.of("resources", List.of("accounts")))));
    }

    @Test
    void appliesTheInclusiveBulkDeltaSafetyMargin() {
        Instant cutoff = NOW.minus(Duration.ofDays(30)).plus(Duration.ofMinutes(5));
        assertEquals(cutoff, MicrosoftAdvertisingConfig.supportedDelta(cutoff, NOW));
        assertNull(MicrosoftAdvertisingConfig.supportedDelta(cutoff.minusMillis(1), NOW));
        assertNull(MicrosoftAdvertisingConfig.supportedDelta(null, NOW));
    }

    @Test
    void keepsSummerAndDstBoundaryReportBucketsInUtc() {
        assertEquals("2026-07-18T07:00:00+00:00", MicrosoftAdvertisingFile.hourly("2026-07-18|7"));
        assertEquals("2026-10-25T01:00:00+00:00", MicrosoftAdvertisingFile.hourly("2026-10-25|1"));
        assertThrows(
                MicrosoftAdvertisingFailure.class,
                () -> MicrosoftAdvertisingFile.hourly("2026-10-25|24"));
        assertThrows(
                MicrosoftAdvertisingFailure.class,
                () -> MicrosoftAdvertisingFile.hourly("2026-02-30|01"));
    }

    private static MicrosoftAdvertisingConfig config(Map<String, Object> overrides) {
        Map<String, Object> values =
                new HashMap<>(
                        Map.of(
                                "client_id", "synthetic_advertising_client",
                                "refresh_token", "synthetic_advertising_refresh",
                                "developer_token", "synthetic_advertising_developer"));
        values.putAll(overrides);
        return new MicrosoftAdvertisingConfig(ReadonlyConfig.fromMap(values));
    }
}
