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

package org.apache.seatunnel.connectors.seatunnel.awscloudtrail;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.source.Collector;
import org.apache.seatunnel.api.source.SourceReader;
import org.apache.seatunnel.api.source.SourceSplitEnumerator;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.connectors.seatunnel.awscloudtrail.AwsCloudTrailConfig.Window;
import org.apache.seatunnel.connectors.seatunnel.awscloudtrail.AwsCloudTrailJson.Page;
import org.apache.seatunnel.connectors.seatunnel.awscloudtrail.AwsCloudTrailSource.Cursor;
import org.apache.seatunnel.connectors.seatunnel.awscloudtrail.AwsCloudTrailSource.State;

import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.lang.reflect.Proxy;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Checks exact fixed-time signing and deterministic checkpoint boundaries outside the network lane.
 * Real engine tests separately exercise HTTPS lookup, savepoint and task cancellation.
 */
class AwsCloudTrailCheckpointIT {
    private static final String ACCESS = "AKIATEST000000000001";
    private static final String SECRET = "Synthetic-lookup-signing-secret-0";
    private static final String TOKEN = "synthetic-session-mark-1456+/=";
    private static final long SECOND = 1762813300L;

    @Test
    void signsAnIndependentKnownUtf8PacketAndIncludesTemporaryCredentials() {
        AwsCloudTrailConfig config =
                config(
                        Map.of(
                                "security_token",
                                TOKEN,
                                "lookup_attribute_key",
                                "Username",
                                "lookup_attribute_value",
                                "研发☀,+_"));
        Window bounds = new Window(SECOND, SECOND + 100);
        byte[] payload = AwsCloudTrailJson.request(config, bounds, null);
        assertEquals(
                "{\"StartTime\":1762813300,\"EndTime\":1762813400,\"MaxResults\":50,\"LookupAttributes\":"
                        + "[{\"AttributeKey\":\"Username\",\"AttributeValue\":\"研发☀,+_\"}]}",
                new String(payload, StandardCharsets.UTF_8));
        Map<String, String> signed =
                new AwsCloudTrailSigner(config)
                        .sign(payload, Instant.parse("2025-11-11T22:23:24Z"));
        assertEquals(
                "2b0f421f1709b0a2b0e3a00f02ce467910336036e74f9ab127751b4aaab2355d",
                signed.get("x-amz-content-sha256"));
        assertEquals("20251111T222324Z", signed.get("x-amz-date"));
        assertEquals(TOKEN, signed.get("x-amz-security-token"));
        assertEquals(
                "AWS4-HMAC-SHA256 Credential=AKIATEST000000000001/20251111/us-east-1/cloudtrail/aws4_request, "
                        + "SignedHeaders=content-type;host;x-amz-content-sha256;x-amz-date;x-amz-security-token;x-amz-target, "
                        + "Signature=3938b90b26c833f30a15e08a566ef0f302ce5c2bfc8eeb693f3a3dd020ff5e01",
                signed.get("authorization"));
    }

    @Test
    void freezesPreciseInclusiveHistoryAndAllowsExactlyTheNinetiethDay() {
        Instant now = Instant.parse("2026-11-12T14:12:30Z");
        Instant boundary = now.minus(90, ChronoUnit.DAYS);
        Window exact =
                config(Map.of("start_date", boundary.toString(), "end_date", boundary.toString()))
                        .freeze(now);
        assertEquals(boundary.getEpochSecond(), exact.start);
        assertEquals(exact.start, exact.end);
        assertTrue(AwsCloudTrailConfig.isRetained(exact, now));
        assertFalse(AwsCloudTrailConfig.isRetained(exact, now.plusSeconds(1)));
        assertThrows(
                IllegalArgumentException.class,
                () ->
                        config(Map.of("start_date", boundary.minusSeconds(1).toString()))
                                .freeze(now));

        Window defaults = config(Map.of()).freeze(now);
        assertEquals(Instant.parse("2026-11-12T00:00:00Z").getEpochSecond(), defaults.start);
        assertEquals(now.getEpochSecond(), defaults.end);
        assertEquals(
                Instant.parse("2026-11-09T23:00:00Z").getEpochSecond(),
                config(Map.of("start_date", "2026-11-10T00:00:00+01:00")).freeze(now).start);
    }

    @Test
    void verifiesPartialOffsetAgainstPageOrderingAndContinuation() throws Exception {
        Window fixed = new Window(SECOND - 1, SECOND + 1);
        String first =
                "{\"EventTime\":" + SECOND + ",\"EventId\":\"same\",\"CloudTrailEvent\":\"first\"}";
        String second =
                "{\"EventTime\":"
                        + SECOND
                        + ",\"EventId\":\"same\",\"CloudTrailEvent\":\"second\"}";
        Page original = page(fixed, first + "," + second, "second-page");
        Page reloaded = page(fixed, first + "," + second, "second-page");
        assertDoesNotThrow(() -> AwsCloudTrailReader.verifyRestored(1, original.digest, reloaded));
        List<Page> changed =
                List.of(
                        page(fixed, second + "," + first, "second-page"),
                        page(fixed, first + "," + second, "new-token"),
                        page(fixed, first, "second-page"));
        for (Page page : changed) {
            IOException error =
                    assertThrows(
                            IOException.class,
                            () -> AwsCloudTrailReader.verifyRestored(1, original.digest, page));
            assertTrue(error.getMessage().contains("changed since its checkpoint"));
        }
        assertEquals(2, original.events.size());
        assertEquals(original.events.get(0).id, original.events.get(1).id);
    }

    @Test
    void restoresUnassignedCoordinatorWithOriginalWindowAndChecksCredentialRegionIdentity()
            throws Exception {
        AwsCloudTrailConfig settings = config(Map.of("security_token", TOKEN));
        AwsCloudTrailSource source = new AwsCloudTrailSource(settings);
        List<Cursor> assigned = new ArrayList<>();
        AtomicInteger registered = new AtomicInteger();
        SourceSplitEnumerator.Context<Cursor> context = enumeratorContext(registered, assigned);
        SourceSplitEnumerator<Cursor, State> first = source.createEnumerator(context);
        first.run();
        State saved = first.snapshotState(1L);
        assertEquals(1, first.currentUnassignedSplitSize());
        assertTrue(assigned.isEmpty());

        AwsCloudTrailConfig otherKey = config(Map.of("access_key", "AKIATEST000000000002"));
        AwsCloudTrailConfig otherRegion = config(Map.of("region", "cn-north-1"));
        assertNotEquals(settings.identity, otherKey.identity);
        assertNotEquals(settings.identity, otherRegion.identity);
        assertThrows(
                IllegalArgumentException.class,
                () -> new AwsCloudTrailSource(otherKey).restoreEnumerator(context, saved));
        assertThrows(
                IllegalArgumentException.class,
                () -> new AwsCloudTrailSource(otherRegion).restoreEnumerator(context, saved));

        AwsCloudTrailConfig rotated = config(Map.of("security_token", "successor-session"));
        assertEquals(settings.identity, rotated.identity);
        SourceSplitEnumerator<Cursor, State> restored =
                new AwsCloudTrailSource(rotated).restoreEnumerator(context, saved);
        restored.run();
        registered.set(1);
        restored.registerReader(0);
        restored.handleSplitRequest(0);
        assertEquals(1, assigned.size());
        assertSame(saved.pending.window, assigned.get(0).window);
        assertEquals(0, restored.currentUnassignedSplitSize());
        first.close();
        restored.close();
    }

    @Test
    void terminalCheckpointSignalsOnceAndContainsNoCredentialSecrets() throws Exception {
        AwsCloudTrailConfig settings = config(Map.of("security_token", TOKEN));
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
                                    throw new AssertionError("Unexpected source context method");
                                });
        Collector<SeaTunnelRow> output =
                new Collector<SeaTunnelRow>() {
                    @Override
                    public void collect(SeaTunnelRow row) {
                        throw new AssertionError("Terminal cursor must not emit rows");
                    }

                    @Override
                    public Object getCheckpointLock() {
                        return this;
                    }
                };
        Cursor completed =
                new Cursor(
                        settings.identity,
                        new Window(SECOND, SECOND),
                        null,
                        List.of(),
                        1,
                        0,
                        null,
                        null,
                        true);
        try (AwsCloudTrailReader reader = new AwsCloudTrailReader(settings, context, "test")) {
            reader.open();
            reader.addSplits(List.of(completed));
            reader.pollNext(output);
            reader.pollNext(output);
            assertEquals(1, ends.get());
            assertTrue(reader.snapshotState(1L).get(0).finished);
        }
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        try (ObjectOutputStream snapshot = new ObjectOutputStream(bytes)) {
            snapshot.writeObject(completed);
        }
        String stored = bytes.toString(StandardCharsets.ISO_8859_1);
        assertFalse(stored.contains(ACCESS));
        assertFalse(stored.contains(SECRET));
        assertFalse(stored.contains(TOKEN));
    }

    private static AwsCloudTrailConfig config(Map<String, String> overrides) {
        Map<String, Object> values = new HashMap<>();
        values.put("access_key", ACCESS);
        values.put("secret_key", SECRET);
        values.putAll(overrides);
        return new AwsCloudTrailConfig(ReadonlyConfig.fromMap(values));
    }

    private static Page page(Window window, String rows, String continuation) throws Exception {
        byte[] body =
                ("{\"Events\":[" + rows + "],\"NextToken\":\"" + continuation + "\"}")
                        .getBytes(StandardCharsets.UTF_8);
        return AwsCloudTrailJson.page(
                AwsCloudTrailJson.parse(body), config(Map.of()), window, null);
    }

    @SuppressWarnings("unchecked")
    private static SourceSplitEnumerator.Context<Cursor> enumeratorContext(
            AtomicInteger registered, List<Cursor> assigned) {
        return (SourceSplitEnumerator.Context<Cursor>)
                Proxy.newProxyInstance(
                        SourceSplitEnumerator.Context.class.getClassLoader(),
                        new Class<?>[] {SourceSplitEnumerator.Context.class},
                        (proxy, method, args) -> {
                            switch (method.getName()) {
                                case "currentParallelism":
                                    return 1;
                                case "registeredReaders":
                                    return registered.get() == 0 ? Set.of() : Set.of(0);
                                case "assignSplit":
                                    assigned.add((Cursor) args[1]);
                                    return null;
                                case "signalNoMoreSplits":
                                    return null;
                                default:
                                    throw new AssertionError(
                                            "Unexpected enumerator context method "
                                                    + method.getName());
                            }
                        });
    }
}
