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

import com.hazelcast.ringbuffer.OverflowPolicy;
import com.hazelcast.ringbuffer.ReadResultSet;
import com.hazelcast.ringbuffer.Ringbuffer;
import com.hazelcast.ringbuffer.impl.RingbufferProxy;
import com.squareup.okhttp.MediaType;
import com.squareup.okhttp.OkHttpClient;
import com.squareup.okhttp.Request;
import com.squareup.okhttp.RequestBody;
import com.squareup.okhttp.Response;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.time.Duration;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

@Slf4j
public class JobEventHttpReportHandler implements EventHandler {
    public static final ObjectMapper JSON_MAPPER = new ObjectMapper();
    public static final Duration REPORT_INTERVAL = Duration.ofSeconds(10);

    private final String httpEndpoint;
    private final Map<String, String> httpHeaders;
    private final OkHttpClient httpClient;
    private final MediaType httpMediaType = MediaType.parse("application/json");
    private final Ringbuffer ringbuffer;
    private volatile long committedEventIndex;
    private final ScheduledExecutorService scheduledExecutorService;

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
        this.committedEventIndex = ringbuffer.headSequence();
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

    @Override
    public void handle(Event event) {
        CompletionStage completionStage = ringbuffer.addAsync(event, OverflowPolicy.OVERWRITE);
        completionStage.toCompletableFuture().join();
    }

    @VisibleForTesting
    synchronized void report() throws IOException {
        long headSequence = ringbuffer.headSequence();
        if (headSequence > committedEventIndex) {
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
            return;
        }

        String events = JSON_MAPPER.writeValueAsString(resultSet.iterator());
        Request.Builder requestBuilder =
                new Request.Builder()
                        .url(httpEndpoint)
                        .post(RequestBody.create(httpMediaType, events));
        httpHeaders.forEach(requestBuilder::header);
        Response response = httpClient.newCall(requestBuilder.build()).execute();
        if (response.isSuccessful()) {
            committedEventIndex += resultSet.readCount();
        } else {
            log.error("Failed to request http server: {}", response);
        }
    }

    @Override
    public void close() {
        log.info("Close http report handler");
        scheduledExecutorService.shutdown();
    }

    private OkHttpClient createHttpClient() {
        OkHttpClient client = new OkHttpClient();
        client.setConnectTimeout(30, TimeUnit.SECONDS);
        client.setWriteTimeout(10, TimeUnit.SECONDS);
        return client;
    }
}
