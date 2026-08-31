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

import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.seatunnel.shade.com.google.common.annotations.VisibleForTesting;
import org.apache.seatunnel.shade.com.google.common.util.concurrent.ThreadFactoryBuilder;

import org.apache.seatunnel.api.event.Event;
import org.apache.seatunnel.api.event.EventHandler;

import com.hazelcast.core.HazelcastInstanceNotActiveException;
import com.hazelcast.ringbuffer.OverflowPolicy;
import com.hazelcast.ringbuffer.ReadResultSet;
import com.hazelcast.ringbuffer.Ringbuffer;
import com.hazelcast.ringbuffer.impl.RingbufferProxy;
import lombok.extern.slf4j.Slf4j;
import okhttp3.ConnectionPool;
import okhttp3.MediaType;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.RequestBody;
import okhttp3.Response;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Deque;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Reports job events from the local ringbuffer to an external HTTP endpoint with local fallback.
 */
@Slf4j
public class JobEventHttpReportHandler implements EventHandler {
    public static final ObjectMapper JSON_MAPPER = new ObjectMapper();
    public static final Duration REPORT_INTERVAL = Duration.ofSeconds(10);
    private static final int LOCAL_EVENT_BUFFER_CAPACITY = 2000;
    private static final int MAX_IDLE_CONNECTIONS = 5;
    private static final long KEEP_ALIVE_DURATION_MINUTES = 5;

    private final String httpEndpoint;
    private final Map<String, String> httpHeaders;
    private final OkHttpClient httpClient;
    private final MediaType httpMediaType = MediaType.parse("application/json");
    private final Ringbuffer ringbuffer;
    private volatile long committedEventIndex;
    private final ScheduledExecutorService scheduledExecutorService;
    private final Object localBufferLock = new Object();
    private final Deque<Event> localBuffer = new ArrayDeque<>();
    private final AtomicBoolean closing = new AtomicBoolean(false);

    public JobEventHttpReportHandler(String httpEndpoint, Ringbuffer ringbuffer) {
        this(httpEndpoint, REPORT_INTERVAL, ringbuffer);
    }

    public JobEventHttpReportHandler(
            String httpEndpoint, Map<String, String> httpHeaders, Ringbuffer ringbuffer) {
        this(httpEndpoint, httpHeaders, REPORT_INTERVAL, ringbuffer);
    }

    public JobEventHttpReportHandler(
            String httpEndpoint, Duration reportInterval, Ringbuffer ringbuffer) {
        this(httpEndpoint, Collections.emptyMap(), reportInterval, ringbuffer);
    }

    public JobEventHttpReportHandler(
            String httpEndpoint,
            Map<String, String> httpHeaders,
            Duration reportInterval,
            Ringbuffer ringbuffer) {
        this.httpEndpoint = httpEndpoint;
        this.httpHeaders = httpHeaders;
        this.ringbuffer = ringbuffer;
        // Avoid a distributed operation on the coordinator initialization thread.
        // The reporting task initializes this index from the current ringbuffer head.
        this.committedEventIndex = -1;
        this.httpClient = createHttpClient();
        this.scheduledExecutorService =
                Executors.newSingleThreadScheduledExecutor(
                        new ThreadFactoryBuilder()
                                .setNameFormat("http-report-event-scheduler-%d")
                                .build());
        scheduledExecutorService.scheduleAtFixedRate(
                () -> {
                    try {
                        report();
                    } catch (Throwable e) {
                        log.error("Failed to report event", e);
                    }
                },
                0,
                reportInterval.getSeconds(),
                TimeUnit.SECONDS);
    }

    /** Buffers the event locally first and then mirrors it into the distributed ringbuffer. */
    @Override
    public void handle(Event event) {
        if (closing.get()) {
            return;
        }
        addToLocalBuffer(event);
        try {
            ringbuffer.addAsync(event, OverflowPolicy.OVERWRITE);
        } catch (HazelcastInstanceNotActiveException e) {
            // Hazelcast is shutting down, keep event in local buffer for best-effort flush.
            log.info("Skip writing event to ringbuffer because Hazelcast instance is not active");
        }
    }

    /**
     * Flushes one batch from the ringbuffer so tests and shutdown hooks can share the same logic.
     */
    @VisibleForTesting
    synchronized void report() throws IOException {
        reportFromRingbuffer();
    }

    private boolean reportFromRingbuffer() throws IOException {
        long headSequence = ringbuffer.headSequence();
        if (committedEventIndex < 0) {
            committedEventIndex = headSequence;
        } else if (headSequence > committedEventIndex) {
            log.warn(
                    "The head sequence {} is greater than the committed event index {}",
                    headSequence,
                    committedEventIndex);
            committedEventIndex = headSequence;
        }
        CompletionStage<ReadResultSet<Event>> completionStage =
                ringbuffer.readManyAsync(
                        committedEventIndex, 0, RingbufferProxy.MAX_BATCH_SIZE, null);
        ReadResultSet<Event> resultSet = completionStage.toCompletableFuture().join();
        if (resultSet.size() <= 0) {
            return false;
        }

        String events = JSON_MAPPER.writeValueAsString(resultSet.iterator());
        if (postEvents(events)) {
            committedEventIndex += resultSet.readCount();
            drainLocalBuffer(resultSet.readCount());
            return true;
        }
        return false;
    }

    private void reportFromLocalBuffer() throws IOException {
        List<Event> snapshot;
        synchronized (localBufferLock) {
            if (localBuffer.isEmpty()) {
                return;
            }
            snapshot = new ArrayList<>(localBuffer);
        }
        String events = JSON_MAPPER.writeValueAsString(snapshot);
        if (postEvents(events)) {
            synchronized (localBufferLock) {
                int toDrain = snapshot.size();
                while (toDrain-- > 0 && !localBuffer.isEmpty()) {
                    localBuffer.pollFirst();
                }
            }
        }
    }

    private boolean postEvents(String events) throws IOException {
        Request.Builder requestBuilder =
                new Request.Builder()
                        .url(httpEndpoint)
                        .post(RequestBody.create(events, httpMediaType));
        httpHeaders.forEach(requestBuilder::header);
        try (Response response = httpClient.newCall(requestBuilder.build()).execute()) {
            if (response.isSuccessful()) {
                return true;
            }
            log.error(
                    "Failed to report events, http status: {} {}",
                    response.code(),
                    response.message());
            return false;
        }
    }

    /**
     * Stops the scheduler and performs a best-effort final flush from both ringbuffer and buffer.
     */
    @Override
    public void close() {
        log.info("Close http report handler");
        closing.set(true);
        boolean interrupted = false;
        try {
            scheduledExecutorService.shutdown();
            boolean schedulerTerminated = false;
            try {
                schedulerTerminated =
                        scheduledExecutorService.awaitTermination(5, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                interrupted = true;
            }
            if (!schedulerTerminated) {
                scheduledExecutorService.shutdownNow();
                if (!interrupted) {
                    try {
                        schedulerTerminated =
                                scheduledExecutorService.awaitTermination(2, TimeUnit.SECONDS);
                    } catch (InterruptedException e) {
                        interrupted = true;
                    }
                }
            }
            if (schedulerTerminated && !interrupted) {
                try {
                    // Flush all remaining events before closing.
                    reportFromRingbuffer();
                } catch (Exception e) {
                    log.error("Failed to flush events from ringbuffer on close", e);
                }
                try {
                    reportFromLocalBuffer();
                } catch (Exception e) {
                    log.error("Failed to flush events from local buffer on close", e);
                }
            } else {
                log.warn("Skip final event flush because the http report scheduler did not stop");
            }
        } finally {
            httpClient.connectionPool().evictAll();
            if (interrupted) {
                Thread.currentThread().interrupt();
            }
        }
    }

    private OkHttpClient createHttpClient() {
        // Timeouts match the previous client. The okhttp3 pool is private to this handler and is
        // released on close; legacy http.keepAlive system properties do not configure it.
        return new OkHttpClient.Builder()
                .connectionPool(
                        new ConnectionPool(
                                MAX_IDLE_CONNECTIONS,
                                KEEP_ALIVE_DURATION_MINUTES,
                                TimeUnit.MINUTES))
                .connectTimeout(30, TimeUnit.SECONDS)
                .readTimeout(10, TimeUnit.SECONDS)
                .writeTimeout(10, TimeUnit.SECONDS)
                .followRedirects(false)
                .followSslRedirects(false)
                .build();
    }

    private void addToLocalBuffer(Event event) {
        synchronized (localBufferLock) {
            while (localBuffer.size() >= LOCAL_EVENT_BUFFER_CAPACITY) {
                localBuffer.pollFirst();
            }
            localBuffer.addLast(event);
        }
    }

    private void drainLocalBuffer(int count) {
        if (count <= 0) {
            return;
        }
        synchronized (localBufferLock) {
            int remaining = count;
            while (remaining > 0 && !localBuffer.isEmpty()) {
                localBuffer.pollFirst();
                remaining--;
            }
        }
    }
}
