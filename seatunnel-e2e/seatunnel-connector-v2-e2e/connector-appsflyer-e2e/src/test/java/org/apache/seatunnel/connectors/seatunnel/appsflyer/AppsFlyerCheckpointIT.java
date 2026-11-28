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

package org.apache.seatunnel.connectors.seatunnel.appsflyer;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.source.Collector;
import org.apache.seatunnel.api.source.SourceReader;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.connectors.seatunnel.appsflyer.AppsFlyerResource.Report;

import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.lang.reflect.Proxy;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Exercises exact checkpoint matching and DST-only boundaries that require controlled clocks. The
 * engine tests separately save and restore an active source through the public Zeta CLI.
 */
class AppsFlyerCheckpointIT {
    private static final ZoneId NEW_YORK = ZoneId.of("America/New_York");

    @Test
    void partialReplayRequiresTheSameOutputIncludingRowOrder() throws IOException {
        AppsFlyerConfig options = options("fixture-token", "2026-03-07", "UTC");
        AppsFlyerWindow window =
                AppsFlyerWindow.plan(options, Instant.parse("2026-03-08T01:00:00Z")).get(0);
        String first = "2026-03-07 00:00:00,first\n";
        String last = "2026-03-07 00:00:00,last\n";
        AppsFlyerRecords.Snapshot initial = parse(options, window, first + last);
        assertDoesNotThrow(
                () ->
                        AppsFlyerReader.verifyRestored(
                                window, 1, initial.digest, parse(options, window, first + last)));
        AppsFlyerRecords.Snapshot reordered = parse(options, window, last + first);
        AppsFlyerRecords.Snapshot shortened = parse(options, window, first);
        AppsFlyerRecords.Snapshot changed =
                parse(options, window, first + last.replace("last", "different"));
        for (AppsFlyerRecords.Snapshot updated : List.of(reordered, shortened, changed)) {
            IOException error =
                    assertThrows(
                            IOException.class,
                            () ->
                                    AppsFlyerReader.verifyRestored(
                                            window, 1, initial.digest, updated));
            assertTrue(error.getMessage().contains("changed since the checkpoint"));
            assertDoesNotThrow(() -> AppsFlyerReader.verifyRestored(window, 0, null, updated));
        }
    }

    @Test
    void preservesInitialImplicitCutoffAndSeparatesQueryIdentityFromCredentials() {
        AppsFlyerConfig original = options("fixture-old", "2026-03-07", "UTC");
        AppsFlyerConfig rotated = options("fixture-new", "2026-03-07", "UTC");
        AppsFlyerConfig changed = options("fixture-old", "2026-03-07", NEW_YORK.getId());
        assertEquals(original.fingerprint, rotated.fingerprint);
        assertNotEquals(original.fingerprint, changed.fingerprint);
        Instant before = Instant.parse("2026-03-08T01:00:00Z");
        AppsFlyerSource.Cursor saved =
                new AppsFlyerSource.Cursor(
                        original.fingerprint, AppsFlyerWindow.plan(original, before), 0, null);
        assertEquals(before, saved.windows.get(0).end);
        assertNotEquals(
                saved.windows.get(0).end,
                AppsFlyerWindow.plan(rotated, before.plusSeconds(3600)).get(0).end);
        assertFalse(saved.fingerprint.contains("fixture-old"));
        assertThrows(UnsupportedOperationException.class, () -> saved.windows.remove(0));
    }

    @Test
    void neverSplitsOnAmbiguousClockBoundariesAndRejectsNonexistentCsvTimestamp() {
        AppsFlyerWindow fold =
                new AppsFlyerWindow(
                        Report.IN_APP_EVENTS,
                        Instant.parse("2026-11-01T04:59:00Z"),
                        Instant.parse("2026-11-01T07:01:00Z"));
        Instant cut = fold.splitAt(NEW_YORK);
        assertEquals(Instant.parse("2026-11-01T07:00:00Z"), cut);
        assertEquals(
                1,
                NEW_YORK.getRules().getValidOffsets(LocalDateTime.ofInstant(cut, NEW_YORK)).size());

        AppsFlyerConfig config = options("fixture-token", "2026-03-08", NEW_YORK.getId());
        AppsFlyerWindow spring =
                new AppsFlyerWindow(
                        Report.IN_APP_EVENTS,
                        Instant.parse("2026-03-08T05:00:00Z"),
                        Instant.parse("2026-03-09T04:00:00Z"));
        AppsFlyerRecords.InvalidReport reason =
                assertThrows(
                        AppsFlyerRecords.InvalidReport.class,
                        () -> parse(config, spring, "2026-03-08 02:30:00,gap\n"));
        assertTrue(reason.getMessage().contains("does not exist"));
    }

    @Test
    void terminalCheckpointSignalsEofOnceAndRejectsOtherQueryIdentity() throws Exception {
        AppsFlyerConfig original = options("fixture-old", "2026-03-07", "UTC");
        AppsFlyerConfig rotated = options("fixture-new", "2026-03-07", "UTC");
        AtomicInteger ends = new AtomicInteger();
        SourceReader.Context context =
                (SourceReader.Context)
                        Proxy.newProxyInstance(
                                SourceReader.Context.class.getClassLoader(),
                                new Class<?>[] {SourceReader.Context.class},
                                (proxy, method, args) -> {
                                    if ("signalNoMoreElement".equals(method.getName())) {
                                        ends.incrementAndGet();
                                        return null;
                                    }
                                    throw new AssertionError(
                                            "Unexpected source context call " + method.getName());
                                });
        Collector<SeaTunnelRow> collector =
                new Collector<SeaTunnelRow>() {
                    @Override
                    public void collect(SeaTunnelRow row) {
                        throw new AssertionError("Terminal checkpoint emitted a row");
                    }

                    @Override
                    public Object getCheckpointLock() {
                        return this;
                    }
                };
        try (AppsFlyerReader reader = new AppsFlyerReader(rotated, context, "fixture")) {
            reader.open();
            AppsFlyerSource.Cursor incompatible =
                    new AppsFlyerSource.Cursor("other-query", List.of(), 0, null);
            assertThrows(
                    IllegalArgumentException.class, () -> reader.addSplits(List.of(incompatible)));
            AppsFlyerSource.Cursor compatible =
                    new AppsFlyerSource.Cursor(original.fingerprint, List.of(), 0, null);
            reader.addSplits(List.of(compatible));
            reader.pollNext(collector);
            reader.pollNext(collector);
            assertEquals(1, ends.get());
            assertTrue(reader.snapshotState(1L).get(0).windows.isEmpty());
        }
    }

    private static AppsFlyerConfig options(String token, String from, String zone) {
        return new AppsFlyerConfig(
                ReadonlyConfig.fromMap(
                        Map.of(
                                "token",
                                token,
                                "app_id",
                                "fixture-app",
                                "start_date",
                                from,
                                "timezone",
                                zone,
                                "resources",
                                List.of("in_app_events"))));
    }

    private static AppsFlyerRecords.Snapshot parse(
            AppsFlyerConfig config, AppsFlyerWindow window, String rows) throws IOException {
        byte[] bytes = ("Event Time,Event Name\n" + rows).getBytes(StandardCharsets.UTF_8);
        return AppsFlyerRecords.parse(new ByteArrayInputStream(bytes), config, window);
    }
}
