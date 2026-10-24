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

package org.apache.seatunnel.lineage.flink;

import org.apache.seatunnel.lineage.LineageConfig;
import org.apache.seatunnel.lineage.LineageDataset;
import org.apache.seatunnel.lineage.LineageEvent;
import org.apache.seatunnel.lineage.LineageEventType;

import org.apache.flink.api.common.JobID;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

class LineageJobStatusHookTest {

    @BeforeEach
    void resetBackend() throws Exception {
        // The emitting thread is shared by every hook of the process, so an event still queued by
        // the previous test would otherwise be recorded into this test's list.
        LineageJobStatusHook.awaitPendingEmissions(5000);
        RecordingLineageBackend.reset();
    }

    /** Waits for the events the terminal callbacks handed to the emitting thread. */
    private static void awaitEmissions() throws Exception {
        Assertions.assertTrue(
                LineageJobStatusHook.awaitPendingEmissions(5000),
                "the queued lineage events must have been emitted");
    }

    private static LineageConfig config(String token) {
        return config(token, null);
    }

    private static LineageConfig config(String token, Long heartbeatMs) {
        Map<String, Object> options = new LinkedHashMap<>();
        options.put(LineageConfig.ENABLED, true);
        options.put(LineageConfig.TRANSPORT, RecordingLineageBackend.NAME);
        options.put(LineageConfig.URL, "http://127.0.0.1:1");
        if (heartbeatMs != null) {
            options.put(LineageConfig.HEARTBEAT_MIN_INTERVAL_MS, heartbeatMs);
        }
        LineageConfig resolved =
                LineageConfig.resolve(
                        options,
                        Collections.<String, Object>emptyMap(),
                        Collections.<String, Object>emptyMap());
        return resolved.withAuthToken(token);
    }

    private static LineageEvent event() {
        return LineageEvent.builder()
                .runId(UUID.randomUUID())
                .eventTime(ZonedDateTime.now(ZoneOffset.UTC))
                .eventType(LineageEventType.START)
                .jobNamespace("seatunnel")
                .jobName("hook-test")
                .producer("https://seatunnel.apache.org/test")
                .runFacet("seatunnel_properties")
                .outputs(Collections.singletonList(LineageDataset.of("mysql://db", "sales.orders")))
                .build();
    }

    private static final JobID JOB_1 = JobID.fromHexString("00000000000000000000000000000001");
    private static final JobID JOB_2 = JobID.fromHexString("00000000000000000000000000000002");
    private static final JobID JOB_3 = JobID.fromHexString("00000000000000000000000000000003");

    @Test
    void emitsTheTerminalEventTypeThatMatchesTheCallback() throws Throwable {
        LineageJobStatusHook hook =
                new LineageJobStatusHook(config(null), Collections.singletonList(event()), false);

        hook.onFinished(JOB_1);
        hook.onFailed(JOB_2, new IllegalStateException("boom"));
        hook.onCanceled(JOB_3);
        awaitEmissions();

        Assertions.assertEquals(3, RecordingLineageBackend.EVENTS.size());
        Assertions.assertEquals(
                LineageEventType.COMPLETE, RecordingLineageBackend.EVENTS.get(0).eventType());
        Assertions.assertEquals(
                LineageEventType.FAIL, RecordingLineageBackend.EVENTS.get(1).eventType());
        Assertions.assertEquals(
                LineageEventType.ABORT, RecordingLineageBackend.EVENTS.get(2).eventType());
        Assertions.assertEquals(
                JOB_2.toString(),
                RecordingLineageBackend.EVENTS
                        .get(1)
                        .runProperties()
                        .get(LineageJobStatusHook.FLINK_JOB_ID_PROPERTY));
    }

    /**
     * An attached client reports the successful terminal event itself, carrying the output
     * statistics this hook cannot read. Only the successful callback is handed over: a failed or
     * cancelled job resolves the client's result future exceptionally, so the client reports
     * nothing and the hook stays the only source of those events.
     */
    @Test
    void leavesTheSuccessfulTerminalEventToAnAttachedClient() throws Throwable {
        LineageJobStatusHook hook =
                new LineageJobStatusHook(config(null), Collections.singletonList(event()), true);

        hook.onFinished(JOB_1);
        awaitEmissions();
        Assertions.assertTrue(
                RecordingLineageBackend.EVENTS.isEmpty(),
                "the attached client owns the successful terminal event");

        hook.onFailed(JOB_2, new IllegalStateException("boom"));
        hook.onCanceled(JOB_3);
        awaitEmissions();

        Assertions.assertEquals(2, RecordingLineageBackend.EVENTS.size());
        Assertions.assertEquals(
                LineageEventType.FAIL, RecordingLineageBackend.EVENTS.get(0).eventType());
        Assertions.assertEquals(
                LineageEventType.ABORT, RecordingLineageBackend.EVENTS.get(1).eventType());
    }

    /**
     * The JobGraph carrying this hook is written to the BlobServer and to HA storage, so the
     * instance must never hold a receiver credential. The JobManager resolves the token again from
     * its own configuration and environment.
     */
    @Test
    void survivesTheJobGraphRoundTripWithoutCarryingTheToken() throws Throwable {
        LineageJobStatusHook hook =
                new LineageJobStatusHook(
                        config("secret-token").withAuthToken(null),
                        Collections.singletonList(event()),
                        false);

        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        try (ObjectOutputStream out = new ObjectOutputStream(bytes)) {
            out.writeObject(hook);
        }
        Assertions.assertFalse(
                new String(bytes.toByteArray(), "ISO-8859-1").contains("secret-token"),
                "a serialized hook must not carry the receiver token");

        LineageJobStatusHook restored;
        try (ObjectInputStream in =
                new ObjectInputStream(new ByteArrayInputStream(bytes.toByteArray()))) {
            restored = (LineageJobStatusHook) in.readObject();
        }

        restored.onFinished(JOB_1);
        awaitEmissions();
        Assertions.assertEquals(1, RecordingLineageBackend.EVENTS.size());
        Assertions.assertEquals(
                LineageEventType.COMPLETE, RecordingLineageBackend.EVENTS.get(0).eventType());
        Assertions.assertNull(RecordingLineageBackend.CONFIGS.get(0).authToken());
    }

    @Test
    void keepsTheJobRunningWhenTheBackendCannotBeResolved() throws Throwable {
        Map<String, Object> options = new LinkedHashMap<>();
        options.put(LineageConfig.ENABLED, true);
        options.put(LineageConfig.TRANSPORT, "no-such-transport");
        options.put(LineageConfig.URL, "http://127.0.0.1:1");
        LineageConfig broken =
                LineageConfig.resolve(
                        options,
                        Collections.<String, Object>emptyMap(),
                        Collections.<String, Object>emptyMap());

        LineageJobStatusHook hook =
                new LineageJobStatusHook(broken, Collections.singletonList(event()), false);

        Assertions.assertDoesNotThrow(() -> hook.onFinished(JOB_1));
        awaitEmissions();
        Assertions.assertTrue(RecordingLineageBackend.EVENTS.isEmpty());
    }

    /**
     * A streaming job never reaches a terminal callback while it is healthy, so without this the
     * receiver would see nothing after the start event and eventually treat the run as abandoned.
     */
    @Test
    void emitsHeartbeatsWhileTheJobRunsAndStopsAtTheTerminalEvent() throws Throwable {
        LineageJobStatusHook hook =
                new LineageJobStatusHook(
                        config(null, 30L), Collections.singletonList(event()), false);

        hook.onCreated(JOB_1);

        long deadlineMs = System.currentTimeMillis() + 5000;
        while (RecordingLineageBackend.EVENTS.size() < 2
                && System.currentTimeMillis() < deadlineMs) {
            Thread.sleep(10);
        }
        Assertions.assertTrue(
                RecordingLineageBackend.EVENTS.size() >= 2,
                "onCreated must start a repeating heartbeat, saw "
                        + RecordingLineageBackend.EVENTS.size());
        for (LineageEvent emitted : RecordingLineageBackend.EVENTS) {
            Assertions.assertEquals(LineageEventType.RUNNING, emitted.eventType());
            Assertions.assertEquals(
                    JOB_1.toString(),
                    emitted.runProperties().get(LineageJobStatusHook.FLINK_JOB_ID_PROPERTY));
        }

        hook.onCanceled(JOB_1);
        awaitEmissions();
        int afterTerminal = RecordingLineageBackend.EVENTS.size();
        Assertions.assertEquals(
                LineageEventType.ABORT,
                RecordingLineageBackend.EVENTS.get(afterTerminal - 1).eventType());

        // Several heartbeat intervals: a RUNNING arriving after the terminal event would reopen a
        // run the receiver has already closed.
        Thread.sleep(300);
        Assertions.assertEquals(
                afterTerminal,
                RecordingLineageBackend.EVENTS.size(),
                "no heartbeat may follow the terminal event");
    }

    /** A heartbeat interval of zero disables the heartbeat rather than scheduling a busy loop. */
    @Test
    void doesNotScheduleAHeartbeatWhenTheIntervalIsZero() throws Throwable {
        LineageJobStatusHook hook =
                new LineageJobStatusHook(
                        config(null, 0L), Collections.singletonList(event()), false);

        hook.onCreated(JOB_1);
        Thread.sleep(150);
        awaitEmissions();

        Assertions.assertTrue(
                RecordingLineageBackend.EVENTS.isEmpty(),
                "a zero interval must not emit heartbeats");
    }

    /**
     * Flink runs the terminal callback inline on the job's own state transition, and one send
     * blocks for a full retry cycle per output dataset. Handing the event to the emitting thread is
     * what keeps an unreachable receiver from stalling the job's transition, and that thread sends
     * one event at a time, so the terminal event still leaves after the RUNNING that was already in
     * flight rather than being overtaken by it.
     */
    @Test
    void handsTheTerminalEventOffInsteadOfWaitingForTheSend() throws Throwable {
        RecordingLineageBackend.HEARTBEAT_ENTERED = new CountDownLatch(1);
        RecordingLineageBackend.HEARTBEAT_RELEASE = new CountDownLatch(1);
        LineageJobStatusHook hook =
                new LineageJobStatusHook(
                        config(null, 20L), Collections.singletonList(event()), false);

        hook.onCreated(JOB_1);
        Assertions.assertTrue(
                RecordingLineageBackend.HEARTBEAT_ENTERED.await(5, TimeUnit.SECONDS),
                "the heartbeat must have started sending");

        long startedAt = System.nanoTime();
        hook.onCanceled(JOB_1);
        long elapsedMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startedAt);

        Assertions.assertTrue(
                elapsedMs < 1000,
                "the terminal callback must not wait for a send, it took " + elapsedMs + "ms");
        Assertions.assertTrue(
                RecordingLineageBackend.EVENTS.isEmpty(),
                "nothing can have been sent while the receiver still holds the heartbeat");

        RecordingLineageBackend.HEARTBEAT_RELEASE.countDown();
        awaitEmissions();

        Assertions.assertEquals(
                LineageEventType.RUNNING, RecordingLineageBackend.EVENTS.get(0).eventType());
        Assertions.assertEquals(
                LineageEventType.ABORT,
                RecordingLineageBackend.EVENTS
                        .get(RecordingLineageBackend.EVENTS.size() - 1)
                        .eventType());

        // Heartbeats scheduled while the terminal event was being sent must find the run closed.
        int afterTerminal = RecordingLineageBackend.EVENTS.size();
        Thread.sleep(300);
        Assertions.assertEquals(
                afterTerminal,
                RecordingLineageBackend.EVENTS.size(),
                "no heartbeat may follow the terminal event");
    }
}
