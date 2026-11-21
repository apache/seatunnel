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

package org.apache.seatunnel.engine.server.event;

import org.apache.seatunnel.shade.com.fasterxml.jackson.core.type.TypeReference;

import org.apache.seatunnel.api.event.Event;
import org.apache.seatunnel.api.event.EventType;
import org.apache.seatunnel.common.utils.ReflectionUtils;
import org.apache.seatunnel.engine.common.utils.concurrent.CompletableFuture;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.Appender;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.Logger;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import com.hazelcast.config.Config;
import com.hazelcast.config.RingbufferConfig;
import com.hazelcast.config.RingbufferStoreConfig;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.HazelcastInstanceNotActiveException;
import com.hazelcast.ringbuffer.ReadResultSet;
import com.hazelcast.ringbuffer.Ringbuffer;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import okhttp3.Call;
import okhttp3.ConnectionSpec;
import okhttp3.OkHttpClient;
import okhttp3.TlsVersion;
import okhttp3.mockwebserver.MockResponse;
import okhttp3.mockwebserver.MockWebServer;
import okhttp3.mockwebserver.RecordedRequest;
import okhttp3.mockwebserver.SocketPolicy;
import okio.Buffer;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.awaitility.Awaitility.given;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@Slf4j
/** Covers buffering and shutdown behavior for HTTP-based job event reporting. */
public class JobEventHttpReportHandlerTest {
    private static final String ringBufferName = "test";
    private static final int capacity = 1000;
    private static HazelcastInstance hazelcast;
    private static MockWebServer mockWebServer;

    @BeforeAll
    public static void before() throws IOException {
        Config config = new Config();
        config.setRingbufferConfigs(
                Collections.singletonMap(
                        ringBufferName,
                        new RingbufferConfig(ringBufferName)
                                .setCapacity(capacity)
                                .setBackupCount(0)
                                .setAsyncBackupCount(1)
                                .setTimeToLiveSeconds(0)
                                .setRingbufferStoreConfig(
                                        new RingbufferStoreConfig().setEnabled(false))));
        hazelcast = Hazelcast.newHazelcastInstance(config);
        mockWebServer = new MockWebServer();
        mockWebServer.start();
        for (int i = 0; i < capacity; i++) {
            mockWebServer.enqueue(new MockResponse().setResponseCode(200));
        }
    }

    @AfterAll
    public static void after() throws IOException {
        hazelcast.shutdown();
        try {
            mockWebServer.shutdown();
        } catch (Exception e) {
            log.error("Failed to shutdown mockWebServer", e);
        }
    }

    @Test
    public void testInterruptedCloseCancelsInFlightRequest() throws Exception {
        try (MockWebServer blockedServer = new MockWebServer()) {
            blockedServer.enqueue(new MockResponse().setSocketPolicy(SocketPolicy.NO_RESPONSE));
            blockedServer.start();
            Ringbuffer ringbuffer = hazelcast.getRingbuffer("interrupted-close-test");
            ringbuffer.add(new TestEvent(1));
            JobEventHttpReportHandler handler =
                    new JobEventHttpReportHandler(
                            blockedServer.url("/api").toString(), Duration.ofDays(1), ringbuffer);
            OkHttpClient client =
                    (OkHttpClient) ReflectionUtils.getField(handler, "httpClient").get();
            boolean closed = false;
            try {
                Assertions.assertNotNull(blockedServer.takeRequest(10, TimeUnit.SECONDS));
                Call inFlight = client.dispatcher().runningCalls().get(0);
                Thread.currentThread().interrupt();
                handler.close();
                closed = true;
                Assertions.assertTrue(Thread.currentThread().isInterrupted());
                Assertions.assertTrue(
                        inFlight.isCanceled(), "Close must cancel the active request");
            } finally {
                Thread.interrupted();
                client.dispatcher().cancelAll();
                if (!closed) {
                    handler.close();
                }
            }
        }
    }

    @Test
    public void testReportEvent() throws IOException, InterruptedException {
        int maxEvents = 1000;
        String headerName = "X-SeaTunnel-Test";
        String headerValue = "event-report";
        Ringbuffer ringbuffer = hazelcast.getRingbuffer(ringBufferName);
        JobEventHttpReportHandler handler =
                new JobEventHttpReportHandler(
                        mockWebServer.url("/api").toString(),
                        Collections.singletonMap(headerName, headerValue),
                        Duration.ofSeconds(1),
                        ringbuffer);
        // Cursor initialization is intentionally asynchronous so handler construction cannot
        // block coordinator startup. Initialize it deterministically before filling the buffer.
        handler.report();
        for (int i = 0; i < maxEvents; i++) {
            handler.handle(new TestEvent(i));
        }
        given().ignoreExceptions()
                .await()
                .atMost(10, TimeUnit.SECONDS)
                .until(() -> mockWebServer.getRequestCount(), count -> count > 0);
        handler.report();
        handler.close();

        List<TestEvent> events = new ArrayList<>();
        for (int i = 0; i < mockWebServer.getRequestCount(); i++) {
            RecordedRequest request = mockWebServer.takeRequest(10, TimeUnit.SECONDS);
            Assertions.assertNotNull(request, "The event report should reach the HTTP server");
            Assertions.assertEquals("POST", request.getMethod());
            Assertions.assertEquals(headerValue, request.getHeader(headerName));
            Assertions.assertEquals(
                    "application/json; charset=utf-8", request.getHeader("Content-Type"));
            try (Buffer buffer = request.getBody()) {
                String body = buffer.readUtf8();
                List<TestEvent> data =
                        JobEventHttpReportHandler.JSON_MAPPER.readValue(
                                body, new TypeReference<List<TestEvent>>() {});
                events.addAll(data);
            }
        }

        Assertions.assertEquals(maxEvents, events.size());
        for (int i = 0; i < maxEvents; i++) {
            Assertions.assertEquals(String.valueOf(i), events.get(i).getJobId());
        }
    }

    @Test
    public void testRetryAfterHttpFailure() throws Exception {
        MockWebServer retryServer = new MockWebServer();
        retryServer.enqueue(new MockResponse().setResponseCode(500));
        retryServer.enqueue(new MockResponse().setResponseCode(200));
        retryServer.start();

        String retryRingBufferName = "retry-test";
        Ringbuffer ringbuffer = hazelcast.getRingbuffer(retryRingBufferName);
        ringbuffer.add(new TestEvent(1));
        JobEventHttpReportHandler handler =
                new JobEventHttpReportHandler(
                        retryServer.url("/api").toString(), Duration.ofDays(1), ringbuffer);
        try {
            handler.report();
            handler.report();

            RecordedRequest firstRequest = retryServer.takeRequest(10, TimeUnit.SECONDS);
            RecordedRequest retryRequest = retryServer.takeRequest(10, TimeUnit.SECONDS);
            Assertions.assertNotNull(firstRequest, "First event report was not received");
            Assertions.assertNotNull(retryRequest, "Retried event report was not received");
            try (Buffer firstBody = firstRequest.getBody();
                    Buffer retryBody = retryRequest.getBody()) {
                Assertions.assertEquals(firstBody.readUtf8(), retryBody.readUtf8());
            }
        } finally {
            handler.close();
            retryServer.shutdown();
        }
    }

    @Test
    public void testDoesNotFollowRedirects() throws Exception {
        MockWebServer redirectServer = new MockWebServer();
        MockWebServer redirectTarget = new MockWebServer();
        redirectServer.start();
        redirectTarget.start();
        for (int i = 0; i < 3; i++) {
            redirectServer.enqueue(
                    new MockResponse()
                            .setResponseCode(307)
                            .setHeader("Location", redirectTarget.url("/target")));
        }

        String redirectRingBufferName = "redirect-test";
        Ringbuffer ringbuffer = hazelcast.getRingbuffer(redirectRingBufferName);
        ringbuffer.add(new TestEvent(1));
        JobEventHttpReportHandler handler =
                new JobEventHttpReportHandler(
                        redirectServer.url("/api").toString(),
                        Collections.singletonMap("Authorization", "Bearer test-token"),
                        Duration.ofDays(1),
                        ringbuffer);
        try {
            handler.report();

            RecordedRequest redirectRequest = redirectServer.takeRequest(10, TimeUnit.SECONDS);
            Assertions.assertNotNull(redirectRequest, "Redirect response was not exercised");
            Assertions.assertEquals(
                    "Bearer test-token", redirectRequest.getHeader("Authorization"));
            Assertions.assertNull(
                    redirectTarget.takeRequest(1, TimeUnit.SECONDS),
                    "Event report followed a redirect to another endpoint");
        } finally {
            handler.close();
            redirectServer.shutdown();
            redirectTarget.shutdown();
        }
    }

    @Test
    public void testConstructorDoesNotWaitForRingbuffer() throws Exception {
        Ringbuffer ringbuffer = mock(Ringbuffer.class);
        ReadResultSet<Event> emptyResultSet = mock(ReadResultSet.class);
        CountDownLatch headSequenceCalled = new CountDownLatch(1);
        CountDownLatch releaseHeadSequence = new CountDownLatch(1);
        when(ringbuffer.headSequence())
                .thenAnswer(
                        invocation -> {
                            headSequenceCalled.countDown();
                            releaseHeadSequence.await();
                            return 0L;
                        });
        when(ringbuffer.readManyAsync(anyLong(), anyInt(), anyInt(), any()))
                .thenReturn(CompletableFuture.completedFuture(emptyResultSet));
        when(emptyResultSet.size()).thenReturn(0);

        ExecutorService constructorExecutor = Executors.newSingleThreadExecutor();
        Future<JobEventHttpReportHandler> handlerFuture =
                constructorExecutor.submit(
                        () ->
                                new JobEventHttpReportHandler(
                                        mockWebServer.url("/api").toString(),
                                        Duration.ofSeconds(1),
                                        ringbuffer));
        JobEventHttpReportHandler handler = null;
        try {
            handler = handlerFuture.get(5, TimeUnit.SECONDS);
            Assertions.assertTrue(headSequenceCalled.await(5, TimeUnit.SECONDS));
        } catch (TimeoutException e) {
            Assertions.fail("Handler construction waited for the distributed ringbuffer", e);
        } finally {
            releaseHeadSequence.countDown();
            if (handler == null) {
                handler = handlerFuture.get(5, TimeUnit.SECONDS);
            }
            handler.close();
            constructorExecutor.shutdownNow();
        }
    }

    @Test
    public void testInitialCursorAndOverflowRecovery() throws Exception {
        Ringbuffer ringbuffer = mock(Ringbuffer.class);
        ReadResultSet<Event> emptyResultSet = mock(ReadResultSet.class);
        CountDownLatch firstReadCompleted = new CountDownLatch(1);
        when(ringbuffer.headSequence()).thenReturn(5L, 7L);
        when(ringbuffer.readManyAsync(anyLong(), anyInt(), anyInt(), any()))
                .thenAnswer(
                        invocation -> {
                            firstReadCompleted.countDown();
                            return CompletableFuture.completedFuture(emptyResultSet);
                        });
        when(emptyResultSet.size()).thenReturn(0);

        JobEventHttpReportHandler handler =
                new JobEventHttpReportHandler(
                        mockWebServer.url("/api").toString(), Duration.ofDays(1), ringbuffer);
        try {
            Assertions.assertTrue(firstReadCompleted.await(5, TimeUnit.SECONDS));
            verify(ringbuffer).readManyAsync(eq(5L), anyInt(), anyInt(), any());

            handler.report();

            verify(ringbuffer).readManyAsync(eq(7L), anyInt(), anyInt(), any());
        } finally {
            handler.close();
        }
    }

    @Test
    public void testCloseWhenHazelcastNotActive() throws Exception {
        String closeTestRingBufferName = "close-test";
        Config config = new Config();
        config.setRingbufferConfigs(
                Collections.singletonMap(
                        closeTestRingBufferName,
                        new RingbufferConfig(closeTestRingBufferName)
                                .setCapacity(capacity)
                                .setBackupCount(0)
                                .setAsyncBackupCount(1)
                                .setTimeToLiveSeconds(0)
                                .setRingbufferStoreConfig(
                                        new RingbufferStoreConfig().setEnabled(false))));

        HazelcastInstance localHazelcast = Hazelcast.newHazelcastInstance(config);
        JobEventHttpReportHandler handler = null;
        try (MockWebServer closeServer = new MockWebServer()) {
            closeServer.enqueue(new MockResponse().setResponseCode(200));
            closeServer.start();
            Ringbuffer ringbuffer = localHazelcast.getRingbuffer(closeTestRingBufferName);
            handler =
                    new JobEventHttpReportHandler(
                            closeServer.url("/api").toString(), Duration.ofDays(1), ringbuffer);
            // Leave the event buffered until close, not delivered by the initial scheduler tick.
            stopScheduler(handler);
            handler.handle(new TestEvent(1));
            localHazelcast.shutdown();

            JobEventHttpReportHandler finalHandler = handler;
            Assertions.assertDoesNotThrow(finalHandler::close);
            assertBufferedEventDelivered(closeServer);
        } finally {
            localHazelcast.shutdown();
            if (handler != null) {
                handler.close();
            }
        }
    }

    @Test
    public void testCloseLogsInactiveHazelcastWithoutStackTrace() throws Exception {
        assertCloseAfterRingbufferFailure(new HazelcastInstanceNotActiveException(), Level.INFO);
    }

    @Test
    public void testCloseLogsWrappedInactiveHazelcastWithoutStackTrace() throws Exception {
        assertCloseAfterRingbufferFailure(
                new CompletionException(new HazelcastInstanceNotActiveException()), Level.INFO);
    }

    @Test
    public void testCloseLogsUnexpectedRingbufferFailure() throws Exception {
        assertCloseAfterRingbufferFailure(
                new IllegalStateException("Unexpected failure"), Level.ERROR);
    }

    @Test
    public void testClientKeepsModernTlsDefaults() throws Exception {
        Ringbuffer ringbuffer = mock(Ringbuffer.class);
        ReadResultSet<Event> emptyResultSet = mock(ReadResultSet.class);
        when(ringbuffer.readManyAsync(anyLong(), anyInt(), anyInt(), any()))
                .thenReturn(CompletableFuture.completedFuture(emptyResultSet));
        JobEventHttpReportHandler handler =
                new JobEventHttpReportHandler(
                        mockWebServer.url("/api").toString(), Duration.ofDays(1), ringbuffer);
        try {
            OkHttpClient client =
                    (OkHttpClient) ReflectionUtils.getField(handler, "httpClient").get();
            Assertions.assertEquals(
                    Arrays.asList(ConnectionSpec.MODERN_TLS, ConnectionSpec.CLEARTEXT),
                    client.connectionSpecs());
            Assertions.assertEquals(
                    Arrays.asList(TlsVersion.TLS_1_3, TlsVersion.TLS_1_2),
                    client.connectionSpecs().get(0).tlsVersions());
            Assertions.assertEquals(30000, client.connectTimeoutMillis());
            Assertions.assertEquals(10000, client.readTimeoutMillis());
            Assertions.assertEquals(10000, client.writeTimeoutMillis());
            Assertions.assertEquals(0, client.callTimeoutMillis());
        } finally {
            handler.close();
        }
    }

    private void assertCloseAfterRingbufferFailure(RuntimeException failure, Level expectedLevel)
            throws Exception {
        Ringbuffer ringbuffer = mock(Ringbuffer.class);
        ReadResultSet<Event> emptyResultSet = mock(ReadResultSet.class);
        when(ringbuffer.readManyAsync(anyLong(), anyInt(), anyInt(), any()))
                .thenReturn(CompletableFuture.completedFuture(emptyResultSet));
        Logger logger = (Logger) LogManager.getLogger(JobEventHttpReportHandler.class);
        Appender appender = mock(Appender.class);
        when(appender.getName()).thenReturn("event-close-test");
        when(appender.isStarted()).thenReturn(true);
        List<LogEvent> logEvents = new CopyOnWriteArrayList<>();
        doAnswer(
                        invocation -> {
                            logEvents.add(((LogEvent) invocation.getArgument(0)).toImmutable());
                            return null;
                        })
                .when(appender)
                .append(any(LogEvent.class));
        Level previousLevel = logger.getLevel();
        JobEventHttpReportHandler handler = null;
        try (MockWebServer closeServer = new MockWebServer()) {
            closeServer.enqueue(new MockResponse().setResponseCode(200));
            closeServer.start();
            handler =
                    new JobEventHttpReportHandler(
                            closeServer.url("/api").toString(), Duration.ofDays(1), ringbuffer);
            stopScheduler(handler);
            handler.handle(new TestEvent(1));
            when(ringbuffer.headSequence()).thenThrow(failure);
            logger.setLevel(Level.INFO);
            logger.addAppender(appender);

            JobEventHttpReportHandler finalHandler = handler;
            Assertions.assertDoesNotThrow(finalHandler::close);
            assertBufferedEventDelivered(closeServer);
            LogEvent flushLog =
                    logEvents.stream()
                            .filter(
                                    event ->
                                            event.getMessage()
                                                    .getFormattedMessage()
                                                    .contains("ringbuffer"))
                            .findFirst()
                            .orElseThrow(
                                    () ->
                                            new AssertionError(
                                                    "Missing ringbuffer shutdown diagnostic"));
            Assertions.assertEquals(expectedLevel, flushLog.getLevel());
            if (expectedLevel == Level.INFO) {
                Assertions.assertNull(flushLog.getThrown());
            } else {
                Assertions.assertSame(failure, flushLog.getThrown());
            }
        } finally {
            logger.removeAppender(appender);
            logger.setLevel(previousLevel);
            if (handler != null) {
                handler.close();
            }
        }
    }

    private void stopScheduler(JobEventHttpReportHandler handler) throws Exception {
        ScheduledExecutorService scheduler =
                (ScheduledExecutorService)
                        ReflectionUtils.getField(handler, "scheduledExecutorService").get();
        scheduler.shutdown();
        Assertions.assertTrue(scheduler.awaitTermination(5, TimeUnit.SECONDS));
    }

    private void assertBufferedEventDelivered(MockWebServer server) throws Exception {
        RecordedRequest request = server.takeRequest(10, TimeUnit.SECONDS);
        Assertions.assertNotNull(request, "Close must deliver the buffered event");
        try (Buffer body = request.getBody()) {
            List<TestEvent> events =
                    JobEventHttpReportHandler.JSON_MAPPER.readValue(
                            body.readUtf8(), new TypeReference<List<TestEvent>>() {});
            Assertions.assertEquals(1, events.size());
            Assertions.assertEquals("1", events.get(0).getJobId());
        }
        Assertions.assertEquals(1, server.getRequestCount());
    }

    @Getter
    @Setter
    @NoArgsConstructor
    @AllArgsConstructor
    /** Minimal event implementation used to exercise the handler's buffering logic. */
    static class TestEvent implements Event {
        private long createdTime;
        private String jobId;
        private EventType eventType;

        public TestEvent(long test) {
            this.createdTime = test;
            this.jobId = String.valueOf(test);
            this.eventType = EventType.SCHEMA_CHANGE_UPDATE_COLUMNS;
        }
    }
}
