/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.seatunnel.connectors.seatunnel.typeform;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.connectors.seatunnel.typeform.config.TypeformSourceParameter;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import okhttp3.mockwebserver.MockResponse;
import okhttp3.mockwebserver.MockWebServer;
import okhttp3.mockwebserver.RecordedRequest;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

public class TypeformSourceReaderTest {

    private TypeformSourceReader newReader(MockWebServer server, int retry) {
        return newReader(server, retry, 10, 50, 0);
    }

    private TypeformSourceReader newReader(
            MockWebServer server,
            int retry,
            int backoffMillis,
            int maxBackoffMillis,
            long minRequestIntervalMillis) {
        Map<String, Object> configMap = new HashMap<>();
        configMap.put("base_url", server.url("/").toString());
        configMap.put("api_key", "test-token");
        configMap.put("form_id", "abc123");
        configMap.put("retry", retry);
        configMap.put("retry_backoff_multiplier_ms", backoffMillis);
        configMap.put("retry_backoff_max_ms", maxBackoffMillis);
        TypeformSourceParameter parameter = new TypeformSourceParameter();
        parameter.buildWithConfig(ReadonlyConfig.fromMap(configMap));
        TypeformSourceReader reader =
                new TypeformSourceReader(parameter, null, null, null, null, null);
        reader.setMinRequestIntervalMillis(minRequestIntervalMillis);
        return reader;
    }

    @Test
    public void testRetryOn429And5xx() throws Exception {
        try (MockWebServer server = new MockWebServer()) {
            server.enqueue(new MockResponse().setResponseCode(429));
            server.enqueue(new MockResponse().setResponseCode(503));
            server.enqueue(new MockResponse().setResponseCode(200).setBody("{\"items\":[]}"));
            server.start();
            TypeformSourceReader reader = newReader(server, 3);
            reader.open();
            try {
                Assertions.assertEquals(200, reader.executeRequest().getCode());
                Assertions.assertEquals(3, server.getRequestCount());
                RecordedRequest first = server.takeRequest();
                Assertions.assertEquals("Bearer test-token", first.getHeader("Authorization"));
                Assertions.assertTrue(first.getPath().startsWith("/forms/abc123/responses?"));
                Assertions.assertTrue(first.getPath().contains("page_size=100"));
                Assertions.assertFalse(first.getPath().contains("test-token"));
            } finally {
                reader.close();
            }
        }
    }

    @Test
    public void testNoRetryOn401() throws Exception {
        try (MockWebServer server = new MockWebServer()) {
            server.enqueue(
                    new MockResponse()
                            .setResponseCode(401)
                            .setBody("{\"code\":\"AUTHENTICATION_FAILED\"}"));
            server.start();
            TypeformSourceReader reader = newReader(server, 3);
            reader.open();
            try {
                Assertions.assertEquals(401, reader.executeRequest().getCode());
                Assertions.assertEquals(1, server.getRequestCount());
            } finally {
                reader.close();
            }
        }
    }

    @Test
    public void testGivesUpAfterRetries() throws Exception {
        try (MockWebServer server = new MockWebServer()) {
            for (int i = 0; i < 4; i++) {
                server.enqueue(new MockResponse().setResponseCode(500));
            }
            server.start();
            TypeformSourceReader reader = newReader(server, 3);
            reader.open();
            try {
                RuntimeException ex =
                        Assertions.assertThrows(
                                RuntimeException.class, () -> reader.executeRequest());
                Assertions.assertTrue(ex.getMessage().contains("HTTP 500"));
                Assertions.assertFalse(ex.getMessage().contains("test-token"));
                Assertions.assertEquals(4, server.getRequestCount());
            } finally {
                reader.close();
            }
        }
    }

    @Test
    public void testRequestsArePaced() throws Exception {
        try (MockWebServer server = new MockWebServer()) {
            server.enqueue(new MockResponse().setResponseCode(429));
            server.enqueue(new MockResponse().setResponseCode(200).setBody("{\"items\":[]}"));
            server.start();
            // A 1 ms backoff, so the gap between the two requests comes from the pacing.
            TypeformSourceReader reader = newReader(server, 3, 1, 50, 300);
            reader.open();
            try {
                long start = System.nanoTime();
                Assertions.assertEquals(200, reader.executeRequest().getCode());
                long elapsedMillis = (System.nanoTime() - start) / 1_000_000L;
                Assertions.assertEquals(2, server.getRequestCount());
                Assertions.assertTrue(
                        elapsedMillis >= 300,
                        "Second request must wait for the pacing interval, took " + elapsedMillis);
            } finally {
                reader.close();
            }
        }
    }

    @Test
    public void testDefaultPacingMatchesTypeformRateLimit() {
        Assertions.assertEquals(500, TypeformSourceReader.DEFAULT_MIN_REQUEST_INTERVAL_MILLIS);
    }

    @Test
    public void testInterruptedBackoffRestoresFlag() throws Exception {
        try (MockWebServer server = new MockWebServer()) {
            server.enqueue(new MockResponse().setResponseCode(429));
            server.start();
            // A long backoff keeps the worker in Thread.sleep while it is interrupted.
            TypeformSourceReader reader = newReader(server, 3, 20_000, 30_000, 0);
            reader.open();

            AtomicReference<Exception> thrown = new AtomicReference<>();
            AtomicBoolean flagRestored = new AtomicBoolean(false);
            Thread worker =
                    new Thread(
                            () -> {
                                try {
                                    reader.executeRequest();
                                } catch (Exception e) {
                                    thrown.set(e);
                                    flagRestored.set(Thread.currentThread().isInterrupted());
                                }
                            });
            try {
                worker.start();
                Assertions.assertNotNull(
                        server.takeRequest(10, TimeUnit.SECONDS), "429 request not received");
                // Wait until the worker has read the 429 and is sleeping in the backoff.
                long deadline = System.currentTimeMillis() + 10_000;
                while (worker.getState() != Thread.State.TIMED_WAITING
                        && System.currentTimeMillis() < deadline) {
                    Thread.sleep(5);
                }
                worker.interrupt();
                worker.join(10_000);

                Assertions.assertFalse(worker.isAlive(), "Worker must stop after interrupt");
                Exception e = thrown.get();
                Assertions.assertNotNull(e, "Expected exception due to thread interruption");
                String msg = String.valueOf(e.getMessage());
                Assertions.assertTrue(
                        msg.contains("Interrupted while backing off"),
                        "Exception must come from the backoff sleep, got: " + msg);
                Assertions.assertTrue(flagRestored.get(), "Interrupt flag must be restored");
                Assertions.assertFalse(
                        msg.contains("test-token"), "Message must not contain token");
                Assertions.assertFalse(
                        msg.contains("Authorization"), "Message must not contain Authorization");
                Assertions.assertEquals(1, server.getRequestCount());
            } finally {
                worker.interrupt();
                reader.close();
            }
        }
    }
}
