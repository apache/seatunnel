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
package org.apache.seatunnel.e2e.connector.discourse;

import org.apache.seatunnel.e2e.common.TestResource;
import org.apache.seatunnel.e2e.common.TestSuiteBase;
import org.apache.seatunnel.e2e.common.container.EngineType;
import org.apache.seatunnel.e2e.common.container.TestContainer;

import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.TestTemplate;
import org.testcontainers.Testcontainers;
import org.testcontainers.containers.Container;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Verifies packaged plugin discovery and both bounded metadata resources across supported engines.
 * The anonymous local fixture checks engine execution, without claiming live-site authorization.
 */
public class DiscourseIT extends TestSuiteBase implements TestResource {
    private static HttpServer server;
    // The extra Zeta job holds its response until a real checkpoint completes, without a sleep
    // race.
    private static final CountDownLatch checkpointRequest = new CountDownLatch(1);
    private static final CountDownLatch releaseCheckpointResponse = new CountDownLatch(1);

    @BeforeAll
    @Override
    public void startUp() throws IOException {
        server = HttpServer.create(new InetSocketAddress(0), 0);
        server.createContext(
                "/forum/categories.json",
                exchange ->
                        respond(
                                exchange,
                                "include_subcategories=true",
                                "{\"category_list\":{\"categories\":[{\"id\":2,\"name\":\"General\","
                                        + "\"slug\":\"general\",\"subcategory_list\":[{\"id\":4,\"name\":\"Help\","
                                        + "\"slug\":\"help\",\"parent_category_id\":2}]}]}}"));
        server.createContext(
                "/forum/latest.json",
                exchange ->
                        respond(
                                exchange,
                                "per_page=2",
                                "{\"topic_list\":{\"more_topics_url\":\"/do-not-follow\",\"topics\":["
                                        + "{\"id\":123,\"title\":\"Pipeline Support\",\"slug\":\"pipeline-support\","
                                        + "\"category_id\":4,\"posts_count\":5,\"closed\":false}]}}"));
        server.createContext(
                "/checkpoint/latest.json",
                exchange -> {
                    checkpointRequest.countDown();
                    try {
                        if (!releaseCheckpointResponse.await(25, TimeUnit.SECONDS)) {
                            throw new IOException("Timed out waiting for the checkpoint assertion");
                        }
                        respond(
                                exchange,
                                "per_page=2",
                                "{\"topic_list\":{\"topics\":[{\"id\":123,\"title\":\"Pipeline Support\",\"slug\":\"pipeline-support\"}]}}");
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        throw new IOException("Checkpoint fixture was interrupted", e);
                    } finally {
                        exchange.close();
                    }
                });
        server.start();
        Testcontainers.exposeHostPorts(server.getAddress().getPort());
    }

    private static void respond(HttpExchange exchange, String query, String body)
            throws IOException {
        try {
            if (!"GET".equals(exchange.getRequestMethod())
                    || !query.equals(exchange.getRequestURI().getRawQuery())
                    || exchange.getRequestHeaders().containsKey("Api-Key")
                    || exchange.getRequestHeaders().containsKey("Api-Username")) {
                exchange.sendResponseHeaders(400, -1);
                return;
            }
            byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
            exchange.getResponseHeaders().set("Content-Type", "application/json");
            exchange.sendResponseHeaders(200, bytes.length);
            try (OutputStream out = exchange.getResponseBody()) {
                out.write(bytes);
            }
        } finally {
            exchange.close();
        }
    }

    @AfterAll
    @Override
    public void tearDown() {
        releaseCheckpointResponse.countDown();
        if (server != null) {
            server.stop(0);
        }
    }

    @TestTemplate
    public void readsBothDiscourseMetadataResources(TestContainer container)
            throws IOException, InterruptedException {
        verify(container, "/discourse_categories_to_assert.conf");
        verify(container, "/discourse_topics_to_assert.conf");
        if (container.identifier().getEngineType() == EngineType.SEATUNNEL) {
            verifyCheckpointDuringHttp(container);
        }
    }

    /*
     * Reuses the existing Zeta container only: completion must advance while the HTTP response is
     * held, then the same Assert rules must observe the fully decoded row with retries disabled.
     */
    private void verifyCheckpointDuringHttp(TestContainer container)
            throws IOException, InterruptedException {
        String jobId = Long.toString(ThreadLocalRandom.current().nextLong(1, Long.MAX_VALUE));
        ExecutorService executor = Executors.newSingleThreadExecutor();
        try {
            Future<Container.ExecResult> job =
                    executor.submit(
                            () ->
                                    container.executeJob(
                                            "/discourse_topics_checkpoint_to_assert.conf",
                                            jobId,
                                            "discourse.test.port="
                                                    + server.getAddress().getPort()));
            Assertions.assertTrue(checkpointRequest.await(15, TimeUnit.SECONDS));
            long completedBefore = container.getCompletedCheckpointCount(jobId);
            Awaitility.await()
                    .atMost(15, TimeUnit.SECONDS)
                    .until(() -> container.getCompletedCheckpointCount(jobId) > completedBefore);
            Assertions.assertFalse(
                    job.isDone(), "The response must still be held at checkpoint completion");
            releaseCheckpointResponse.countDown();
            Container.ExecResult result = job.get(30, TimeUnit.SECONDS);
            Assertions.assertEquals(0, result.getExitCode(), result.getStderr());
        } catch (ExecutionException | TimeoutException e) {
            throw new IOException("Checkpointed Discourse job did not complete", e);
        } finally {
            releaseCheckpointResponse.countDown();
            executor.shutdownNow();
        }
    }

    private void verify(TestContainer container, String config)
            throws IOException, InterruptedException {
        Container.ExecResult result =
                container.executeJob(
                        config,
                        Collections.singletonList(
                                "discourse.test.port=" + server.getAddress().getPort()));
        Assertions.assertEquals(0, result.getExitCode(), result.getStderr());
    }
}
