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

package org.apache.seatunnel.e2e.connector.file.s3;

import org.apache.seatunnel.e2e.common.TestSuiteBase;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import com.amazonaws.AmazonClientException;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.s3.model.AmazonS3Exception;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;

import java.io.Closeable;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Records actual SDK requests; no SeaTunnel jobs or object-content requests are used. */
@Timeout(30)
public class S3FileConnectDryRunWireIT extends TestSuiteBase {
    @Test
    void shouldUseOnlyObjectHeadForExactPath() throws Exception {
        try (MetadataServer server = new MetadataServer("object")) {
            S3FileConnectDryRunIT.validate(server.config());
            assertEquals(Arrays.asList("HEAD /dry-run-events/events"), server.requests());
        }
    }

    @Test
    void shouldListOnlyOnePrefixEntryWithoutFetchingContentsOrNextPage() throws Exception {
        try (MetadataServer server = new MetadataServer("prefix")) {
            assertDoesNotThrow(
                    () -> S3FileConnectDryRunIT.validate(server.config()),
                    () -> server.requests().toString());
            List<String> requests = server.requests();
            assertEquals(2, requests.size());
            assertEquals("HEAD /dry-run-events/events", requests.get(0));
            assertTrue(requests.get(1).startsWith("GET /dry-run-events/?"), requests.toString());
            assertTrue(requests.get(1).contains("max-keys=1"));
            assertTrue(requests.get(1).contains("prefix=events%2F"));
            assertTrue(requests.get(1).contains("delimiter=%2F"));
            assertFalse(requests.get(1).contains("continuation-token"));
        }
    }

    @Test
    void shouldPropagateForbiddenMetadataWithoutRetrying() throws Exception {
        try (MetadataServer server = new MetadataServer("forbidden")) {
            AmazonS3Exception failure =
                    assertThrows(
                            AmazonS3Exception.class,
                            () -> S3FileConnectDryRunIT.validate(server.config()));
            assertEquals(403, failure.getStatusCode());
            assertEquals(1, server.requests().size());
        }
    }

    @Test
    void shouldUseConfiguredSmallerSocketTimeoutAndCloseCredentialsOnFailure() throws Exception {
        try (MetadataServer server = new MetadataServer("stalled")) {
            Map<String, Object> config = server.config();
            Map<String, String> properties = new HashMap<>();
            properties.put("fs.s3a.path.style.access", "true");
            properties.put("fs.s3a.connection.timeout", "200");
            config.put("hadoop_s3_properties", properties);
            config.put(
                    "fs.s3a.aws.credentials.provider", TrackingCredentialsProvider.class.getName());
            int closedBefore = TrackingCredentialsProvider.CLOSED.get();
            AmazonClientException failure =
                    assertThrows(
                            AmazonClientException.class,
                            () -> S3FileConnectDryRunIT.validate(config));
            assertTrue(failure.getMessage().contains("Unable to execute HTTP request"));
            assertEquals(1, server.requests().size());
            assertEquals(closedBefore + 1, TrackingCredentialsProvider.CLOSED.get());
        }
    }

    @Test
    void shouldCloseCredentialsAfterSuccessfulMetadataRequest() throws Exception {
        try (MetadataServer server = new MetadataServer("object")) {
            Map<String, Object> config = server.config();
            config.put(
                    "fs.s3a.aws.credentials.provider", TrackingCredentialsProvider.class.getName());
            int closedBefore = TrackingCredentialsProvider.CLOSED.get();
            S3FileConnectDryRunIT.validate(config);
            assertEquals(closedBefore + 1, TrackingCredentialsProvider.CLOSED.get());
        }
    }

    public static class TrackingCredentialsProvider implements AWSCredentialsProvider, Closeable {
        private static final AtomicInteger CLOSED = new AtomicInteger();
        private static final AtomicInteger CREATED = new AtomicInteger();

        public TrackingCredentialsProvider() {
            CREATED.incrementAndGet();
        }

        @Override
        public AWSCredentials getCredentials() {
            return new BasicAWSCredentials("minioadmin", "minioadmin");
        }

        @Override
        public void refresh() {}

        @Override
        public void close() {
            CLOSED.incrementAndGet();
        }
    }

    @Test
    void shouldCloseCredentialsWhenEndpointInitializationFails() {
        Map<String, Object> config =
                S3FileConnectDryRunIT.sourceConfig("http://[", "dry-run-events", "/events");
        config.put("fs.s3a.aws.credentials.provider", TrackingCredentialsProvider.class.getName());
        int closedBefore = TrackingCredentialsProvider.CLOSED.get();
        IllegalArgumentException failure =
                assertThrows(
                        IllegalArgumentException.class,
                        () -> S3FileConnectDryRunIT.validate(config));
        assertTrue(failure.getMessage().contains("endpoint"));
        assertEquals(closedBefore + 1, TrackingCredentialsProvider.CLOSED.get());
    }

    @Test
    void shouldRejectInvalidClientSettingsBeforeCreatingCredentials() {
        Map<String, Object> config =
                S3FileConnectDryRunIT.sourceConfig("http://localhost", "dry-run-events", "/events");
        config.put("fs.s3a.aws.credentials.provider", TrackingCredentialsProvider.class.getName());
        Map<String, String> properties = new HashMap<>();
        properties.put("fs.s3a.connection.maximum", "0");
        config.put("hadoop_s3_properties", properties);
        int createdBefore = TrackingCredentialsProvider.CREATED.get();
        IllegalArgumentException failure =
                assertThrows(
                        IllegalArgumentException.class,
                        () -> S3FileConnectDryRunIT.validate(config));
        assertTrue(failure.getMessage().contains("fs.s3a.connection.maximum"));
        assertEquals(createdBefore, TrackingCredentialsProvider.CREATED.get());
    }

    private static final class MetadataServer implements AutoCloseable {
        private final HttpServer server;
        private final ExecutorService executor = Executors.newCachedThreadPool();
        private final ConcurrentLinkedQueue<String> requests = new ConcurrentLinkedQueue<>();
        private final CountDownLatch release = new CountDownLatch(1);

        private MetadataServer(String response) throws IOException {
            server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
            server.setExecutor(executor);
            server.createContext("/", exchange -> reply(exchange, response));
            server.start();
        }

        private void reply(HttpExchange exchange, String response) throws IOException {
            requests.add(exchange.getRequestMethod() + " " + exchange.getRequestURI());
            try {
                exchange.getResponseHeaders().add("Connection", "close");
                if ("stalled".equals(response)) {
                    try {
                        release.await();
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                    return;
                }
                if ("HEAD".equals(exchange.getRequestMethod())) {
                    int status =
                            "object".equals(response)
                                    ? 200
                                    : "forbidden".equals(response) ? 403 : 404;
                    exchange.getResponseHeaders().add("ETag", "\"event-etag\"");
                    exchange.getResponseHeaders()
                            .add("Last-Modified", "Tue, 08 Sep 2026 00:00:00 GMT");
                    exchange.sendResponseHeaders(status, -1);
                    return;
                }
                String xml =
                        "<ListBucketResult xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">"
                                + "<Name>dry-run-events</Name><Prefix>events/</Prefix><MaxKeys>1</MaxKeys>"
                                + "<IsTruncated>true</IsTruncated><NextContinuationToken>remaining-events</NextContinuationToken>"
                                + "<Contents><Key>events/data.json</Key><LastModified>2026-09-08T00:00:00.000Z</LastModified>"
                                + "<ETag>event-etag</ETag><Size>14</Size><StorageClass>STANDARD</StorageClass></Contents></ListBucketResult>";
                byte[] body = xml.getBytes(StandardCharsets.UTF_8);
                exchange.getResponseHeaders().add("Content-Type", "application/xml");
                exchange.sendResponseHeaders(200, body.length);
                exchange.getResponseBody().write(body);
            } finally {
                exchange.close();
            }
        }

        private Map<String, Object> config() {
            return S3FileConnectDryRunIT.sourceConfig(
                    "http://127.0.0.1:" + server.getAddress().getPort(),
                    "dry-run-events",
                    "/events");
        }

        private List<String> requests() {
            return new ArrayList<>(requests);
        }

        @Override
        public void close() {
            release.countDown();
            server.stop(0);
            executor.shutdownNow();
        }
    }
}
