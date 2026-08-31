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
import org.apache.seatunnel.engine.common.utils.concurrent.CompletableFuture;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import com.hazelcast.config.Config;
import com.hazelcast.config.RingbufferConfig;
import com.hazelcast.config.RingbufferStoreConfig;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.ringbuffer.ReadResultSet;
import com.hazelcast.ringbuffer.Ringbuffer;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import okhttp3.mockwebserver.MockResponse;
import okhttp3.mockwebserver.MockWebServer;
import okhttp3.mockwebserver.RecordedRequest;
import okio.Buffer;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.awaitility.Awaitility.given;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
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
            RecordedRequest request = mockWebServer.takeRequest();
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
    public void testCloseWhenHazelcastNotActive() {
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
        try {
            Ringbuffer ringbuffer = localHazelcast.getRingbuffer(closeTestRingBufferName);
            handler =
                    new JobEventHttpReportHandler(
                            mockWebServer.url("/api").toString(),
                            Duration.ofSeconds(1),
                            ringbuffer);
        } finally {
            localHazelcast.shutdown();
        }

        Assertions.assertNotNull(handler);
        JobEventHttpReportHandler finalHandler = handler;
        Assertions.assertDoesNotThrow(finalHandler::close);
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
