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

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import com.hazelcast.config.Config;
import com.hazelcast.config.RingbufferConfig;
import com.hazelcast.config.RingbufferStoreConfig;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.ringbuffer.Ringbuffer;
import com.squareup.okhttp.mockwebserver.MockResponse;
import com.squareup.okhttp.mockwebserver.MockWebServer;
import com.squareup.okhttp.mockwebserver.RecordedRequest;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import okio.Buffer;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.awaitility.Awaitility.given;

@Slf4j
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
        Ringbuffer ringbuffer = hazelcast.getRingbuffer(ringBufferName);
        JobEventHttpReportHandler handler =
                new JobEventHttpReportHandler(
                        mockWebServer.url("/api").toString(), Duration.ofSeconds(1), ringbuffer);
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

    @Getter
    @Setter
    @NoArgsConstructor
    @AllArgsConstructor
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
