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

package org.apache.seatunnel.resource.e2e.kubernetes;

import org.apache.seatunnel.engine.common.runtime.ApplicationStatus;
import org.apache.seatunnel.resource.kubernetes.client.KubernetesApplicationClient;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.ValueSource;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import io.kubernetes.client.openapi.ApiClient;
import io.kubernetes.client.openapi.ApiException;
import io.kubernetes.client.openapi.apis.BatchV1Api;
import io.kubernetes.client.openapi.apis.CoreV1Api;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.apache.seatunnel.resource.e2e.kubernetes.AbstractKubernetesApplicationIT.livePodLogs;
import static org.apache.seatunnel.resource.e2e.kubernetes.AbstractKubernetesApplicationIT.recordApplicationDiagnostics;
import static org.apache.seatunnel.resource.e2e.kubernetes.AbstractKubernetesApplicationIT.retainedPodLogs;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/** Exercises the shared log polling helper with real Kubernetes SDK HTTP responses. */
public class KubernetesApplicationPodLogsIT {
    private final AtomicInteger requests = new AtomicInteger();
    private final AtomicInteger responseCode = new AtomicInteger(200);
    private final AtomicReference<String> responseBody = new AtomicReference<>();
    @TempDir private Path diagnosticsRoot;
    private CoreV1Api previousCore;
    private BatchV1Api previousBatch;
    private String previousNamespace;
    private ApiClient client;
    private HttpServer server;

    @BeforeEach
    void startLogEndpoint() throws Exception {
        previousCore = AbstractKubernetesApplicationIT.core;
        previousBatch = AbstractKubernetesApplicationIT.batch;
        previousNamespace = AbstractKubernetesApplicationIT.namespace;
        server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext(
                "/api/v1/namespaces/log-fixture/pods/worker/log",
                exchange -> {
                    requests.incrementAndGet();
                    respond(exchange, responseCode.get(), responseBody.get());
                });
        server.start();
        client = new ApiClient();
        client.setBasePath("http://127.0.0.1:" + server.getAddress().getPort());
        AbstractKubernetesApplicationIT.core = new CoreV1Api(client);
        AbstractKubernetesApplicationIT.batch = new BatchV1Api(client);
        AbstractKubernetesApplicationIT.namespace = "log-fixture";
    }

    @AfterEach
    void stopLogEndpoint() {
        AbstractKubernetesApplicationIT.core = previousCore;
        AbstractKubernetesApplicationIT.batch = previousBatch;
        AbstractKubernetesApplicationIT.namespace = previousNamespace;
        if (server != null) {
            server.stop(0);
        }
        if (client != null) {
            client.getHttpClient().dispatcher().executorService().shutdownNow();
            client.getHttpClient().connectionPool().evictAll();
        }
    }

    private static void respond(HttpExchange exchange, int status, String body) throws IOException {
        try {
            if (body == null) {
                exchange.sendResponseHeaders(status, -1);
            } else {
                byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
                exchange.sendResponseHeaders(status, bytes.length);
                exchange.getResponseBody().write(bytes);
            }
        } finally {
            exchange.close();
        }
    }

    private void jsonEndpoint(String path, String body) {
        server.createContext(
                path,
                exchange -> {
                    exchange.getResponseHeaders().set("Content-Type", "application/json");
                    respond(exchange, 200, body);
                });
    }

    @Test
    void diagnosticsContinueAfterEmptySuccessfulPodLogs() throws Exception {
        jsonEndpoint(
                "/apis/batch/v1/namespaces/log-fixture/jobs/application",
                "{\"metadata\":{\"name\":\"application\"},\"status\":{}}");
        jsonEndpoint("/api/v1/namespaces/log-fixture/events", "{\"items\":[]}");
        jsonEndpoint(
                "/api/v1/namespaces/log-fixture/pods",
                "{\"items\":[{\"metadata\":{\"name\":\"worker\"}},{\"metadata\":{\"name\":\"second-worker\"}}]}");
        AtomicReference<String> secondPodQuery = new AtomicReference<>();
        server.createContext(
                "/api/v1/namespaces/log-fixture/pods/second-worker/log",
                exchange -> {
                    secondPodQuery.set(exchange.getRequestURI().getQuery());
                    respond(exchange, 200, "second pod marker\n");
                });
        KubernetesApplicationClient application = mock(KubernetesApplicationClient.class);
        when(application.getClusterId()).thenReturn("application");
        when(application.getStatus()).thenReturn(ApplicationStatus.RUNNING);
        String previousBase = System.getProperty("basedir");
        try {
            System.setProperty("basedir", diagnosticsRoot.toString());
            recordApplicationDiagnostics(application);
            Path reports = diagnosticsRoot.resolve("target/failsafe-reports");
            assertEquals("", Files.readString(reports.resolve("worker.log")));
            assertEquals(
                    "second pod marker\n", Files.readString(reports.resolve("second-worker.log")));
            assertTrue(Arrays.asList(secondPodQuery.get().split("&")).contains("tailLines=200"));
        } finally {
            if (previousBase == null) {
                System.clearProperty("basedir");
            } else {
                System.setProperty("basedir", previousBase);
            }
        }
    }

    @Test
    void emptySuccessfulLogsRemainRetryableUntilTheMarkerAppears() throws Exception {
        assertFalse(retainedPodLogs("worker").contains("checkpoint marker"));
        responseBody.set("checkpoint marker\n");
        assertEquals("checkpoint marker\n", retainedPodLogs("worker"));
        assertEquals(2, requests.get());
    }

    @ParameterizedTest
    @ValueSource(ints = {400, 404})
    void unavailablePodLogsRemainRetryable(int status) throws Exception {
        responseCode.set(status);
        responseBody.set("container logs not available yet");
        assertEquals("", retainedPodLogs("worker"));
    }

    @ParameterizedTest
    @ValueSource(ints = {401, 403, 500})
    void otherApiFailuresRemainVisible(int status) {
        responseCode.set(status);
        responseBody.set("pod log failure");
        ApiException failure = assertThrows(ApiException.class, () -> retainedPodLogs("worker"));
        assertEquals(status, failure.getCode());
        assertEquals("pod log failure", failure.getResponseBody());
    }

    @ParameterizedTest
    @EnumSource(
            value = ApplicationStatus.class,
            names = {"FAILED", "SUCCEEDED", "CANCELED", "UNKNOWN"})
    void stoppedApplicationsCannotBePolled(ApplicationStatus status) throws Exception {
        KubernetesApplicationClient application = mock(KubernetesApplicationClient.class);
        when(application.getStatus()).thenReturn(status);
        assertThrows(IllegalStateException.class, () -> livePodLogs(application, "worker"));
        assertEquals(0, requests.get());
    }

    @ParameterizedTest
    @EnumSource(
            value = ApplicationStatus.class,
            names = {"DEPLOYING", "RUNNING"})
    void liveApplicationsCanWaitForFirstLogs(ApplicationStatus status) throws Exception {
        KubernetesApplicationClient application = mock(KubernetesApplicationClient.class);
        when(application.getStatus()).thenReturn(status);
        assertFalse(livePodLogs(application, "worker").contains("checkpoint marker"));
        responseBody.set("checkpoint marker\n");
        assertEquals("checkpoint marker\n", livePodLogs(application, "worker"));
        assertEquals(2, requests.get());
    }
}
