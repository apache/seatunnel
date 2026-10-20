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

package org.apache.seatunnel.engine.e2e;

import org.apache.seatunnel.engine.common.config.ConfigProvider;
import org.apache.seatunnel.engine.common.config.SeaTunnelConfig;
import org.apache.seatunnel.engine.server.SeaTunnelServerStarter;
import org.apache.seatunnel.engine.server.rest.RestConstant;

import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import com.hazelcast.instance.impl.HazelcastInstanceImpl;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.concurrent.TimeUnit;

import static io.restassured.RestAssured.given;
import static org.hamcrest.Matchers.notNullValue;

/**
 * E2E test for the Web UI entry point that the Zeta HTTP service serves.
 *
 * <p>The Web UI keeps its route in the URL fragment, so a deep link looks like {@code
 * http://localhost:8080/#/overview}. A client that percent-encodes a whole URL - a link opened from
 * a chat, a wiki or a markdown preview does that - requests {@code /%23/overview} instead, and the
 * HTTP service used to answer with a bare Jetty "HTTP ERROR 404" page. The cases below pin down
 * that the encoded fragment is restored by a redirect, that following it opens the Web UI, and that
 * REST paths are not affected.
 *
 * <p>The Web UI assertions need the packaged UI resources, so this test belongs to the engine E2E
 * lane that builds the UI module.
 */
public class WebUiEntryPointIT {

    private static final String UI_ENTRY_PAGE_TITLE = "<title>Seatunnel Engine UI</title>";

    /** The reported URL, {@code /#/overview}, as a client that percent-encoded it sends it. */
    private static final String ENCODED_OVERVIEW_ROUTE = "/%23/overview";

    private static HazelcastInstanceImpl node;
    private static String baseUrl;
    private static HttpClient client;

    @BeforeAll
    static void startEngine() throws IOException {
        Assertions.assertNotNull(
                WebUiEntryPointIT.class.getClassLoader().getResource("ui"),
                "This test needs the packaged Web UI resources; run it without -Dskip.ui=true");
        int httpPort = getAvailablePort();
        node = startNode(httpPort, "");
        baseUrl = "http://localhost:" + httpPort;
        // A fragment the client encoded is sent as a path, so the raw URL has to reach the wire
        // exactly as written here; Redirect.NEVER keeps the redirect itself observable.
        client = httpClient(HttpClient.Redirect.NEVER);
        Awaitility.await()
                .atMost(2, TimeUnit.MINUTES)
                .until(() -> get(baseUrl, RestConstant.REST_URL_OVERVIEW).statusCode() == 200);
    }

    @AfterAll
    static void stopEngine() {
        if (node != null) {
            node.shutdown();
            node = null;
        }
    }

    @Test
    void testEncodedFragmentIsRedirectedToTheWebUiRoute() throws Exception {
        HttpResponse<String> response = get(baseUrl, ENCODED_OVERVIEW_ROUTE);

        Assertions.assertEquals(302, response.statusCode());
        Assertions.assertEquals(
                "/#/overview",
                response.headers().firstValue("Location").orElse(null),
                "The percent-encoded fragment must come back as a URL fragment");
    }

    @Test
    void testEncodedFragmentDeepLinkKeepsItsRoute() throws Exception {
        HttpResponse<String> response = get(baseUrl, "/%23/jobs/1001");

        Assertions.assertEquals(302, response.statusCode());
        Assertions.assertEquals(
                "/#/jobs/1001", response.headers().firstValue("Location").orElse(null));
    }

    @Test
    void testFollowingTheRedirectOpensTheWebUi() throws Exception {
        HttpResponse<String> response =
                httpClient(HttpClient.Redirect.NORMAL)
                        .send(
                                request(baseUrl, ENCODED_OVERVIEW_ROUTE),
                                HttpResponse.BodyHandlers.ofString());

        Assertions.assertEquals(200, response.statusCode());
        Assertions.assertTrue(
                response.headers().firstValue("Content-Type").orElse("").contains("text/html"),
                "Following the redirect must serve the Web UI entry page");
        Assertions.assertTrue(
                response.body().contains(UI_ENTRY_PAGE_TITLE),
                "Following the redirect must serve the Web UI entry page");
    }

    @Test
    void testWebUiIsServedAtTheRootPath() throws Exception {
        HttpResponse<String> response = get(baseUrl, "/");

        Assertions.assertEquals(200, response.statusCode());
        Assertions.assertTrue(response.body().contains(UI_ENTRY_PAGE_TITLE));
    }

    @Test
    void testRestPathIsNotRedirectedToTheWebUi() {
        given().get(baseUrl + RestConstant.REST_URL_OVERVIEW)
                .then()
                .statusCode(200)
                .body("projectVersion", notNullValue());
    }

    @Test
    void testUnknownPathIsNotRedirectedToTheWebUi() throws Exception {
        Assertions.assertEquals(404, get(baseUrl, "/not-a-web-ui-route").statusCode());
    }

    /**
     * A configured {@code context-path} moves the Web UI route and the REST endpoints under the
     * prefix, and the restored fragment has to keep that prefix. This needs its own node, so it is
     * the one case that does not use the node from {@link #startEngine()}.
     */
    @Test
    void testEncodedFragmentRedirectKeepsTheConfiguredContextPath() throws Exception {
        int httpPort = getAvailablePort();
        String prefixedBaseUrl = "http://localhost:" + httpPort;
        String contextPath = "/seatunnel";
        HazelcastInstanceImpl prefixedNode = null;
        try {
            prefixedNode = startNode(httpPort, contextPath);
            Awaitility.await()
                    .atMost(2, TimeUnit.MINUTES)
                    .until(
                            () ->
                                    get(
                                                            prefixedBaseUrl,
                                                            contextPath
                                                                    + RestConstant
                                                                            .REST_URL_OVERVIEW)
                                                    .statusCode()
                                            == 200);

            HttpResponse<String> response =
                    client.send(
                            request(prefixedBaseUrl, contextPath + ENCODED_OVERVIEW_ROUTE),
                            HttpResponse.BodyHandlers.ofString());

            Assertions.assertEquals(302, response.statusCode());
            Assertions.assertEquals(
                    contextPath + "/#/overview",
                    response.headers().firstValue("Location").orElse(null));
        } finally {
            if (prefixedNode != null) {
                prefixedNode.shutdown();
            }
        }
    }

    private static HttpResponse<String> get(String targetBaseUrl, String path) throws Exception {
        return client.send(request(targetBaseUrl, path), HttpResponse.BodyHandlers.ofString());
    }

    private static HttpRequest request(String targetBaseUrl, String path) {
        return HttpRequest.newBuilder(URI.create(targetBaseUrl + path)).GET().build();
    }

    private static HazelcastInstanceImpl startNode(int httpPort, String contextPath) {
        SeaTunnelConfig config = ConfigProvider.locateAndGetSeaTunnelConfig();
        // Each node needs its own cluster: nodes in one JVM and with the same name would join each
        // other over TCP-IP discovery, and the redirect assertions do not need that.
        config.getHazelcastConfig()
                .setClusterName(
                        TestUtils.getClusterName(
                                "WebUiEntryPointIT" + contextPath.replace('/', '_')));
        config.getEngineConfig().getHttpConfig().setEnabled(true);
        config.getEngineConfig().getHttpConfig().setEnableDynamicPort(false);
        config.getEngineConfig().getHttpConfig().setPort(httpPort);
        // Other ITs in the same JVM may leave another context path behind.
        config.getEngineConfig().getHttpConfig().setContextPath(contextPath);
        return SeaTunnelServerStarter.createHazelcastInstance(config);
    }

    private static HttpClient httpClient(HttpClient.Redirect redirectPolicy) {
        return HttpClient.newBuilder()
                .followRedirects(redirectPolicy)
                .connectTimeout(Duration.ofSeconds(10))
                .build();
    }

    private static int getAvailablePort() {
        try (ServerSocket socket = new ServerSocket(0)) {
            return socket.getLocalPort();
        } catch (IOException e) {
            throw new IllegalStateException("No available port for the Web UI entry point IT", e);
        }
    }
}
