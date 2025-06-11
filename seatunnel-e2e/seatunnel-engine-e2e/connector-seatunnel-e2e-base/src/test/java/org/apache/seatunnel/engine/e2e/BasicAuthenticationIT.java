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

import org.apache.seatunnel.engine.server.rest.RestConstant;

import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.GenericContainer;

import io.restassured.http.ContentType;
import io.restassured.response.Response;

import java.io.IOException;
import java.util.Base64;
import java.util.concurrent.TimeUnit;

import static io.restassured.RestAssured.given;
import static org.apache.seatunnel.e2e.common.util.ContainerUtil.PROJECT_ROOT_PATH;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.notNullValue;

/** Integration test for basic authentication in SeaTunnel Engine. */
public class BasicAuthenticationIT extends SeaTunnelEngineContainer {

    private static final String HTTP = "http://";
    private static final String COLON = ":";
    private static final String USERNAME = "testuser";
    private static final String PASSWORD = "testpassword";
    private static final String BASIC_AUTH_HEADER = "Authorization";
    private static final String BASIC_AUTH_PREFIX = "Basic ";

    @Override
    @BeforeEach
    public void startUp() throws Exception {
        // Create server with basic authentication enabled

        server = createSeaTunnelContainerWithBasicAuth();
        // Wait for server to be ready
        Awaitility.await()
                .atMost(2, TimeUnit.MINUTES)
                .until(
                        () -> {
                            try {
                                // Try to access with correct credentials
                                String credentials = USERNAME + ":" + PASSWORD;
                                String encodedCredentials =
                                        Base64.getEncoder().encodeToString(credentials.getBytes());

                                given().header(
                                                BASIC_AUTH_HEADER,
                                                BASIC_AUTH_PREFIX + encodedCredentials)
                                        .get(
                                                HTTP
                                                        + server.getHost()
                                                        + COLON
                                                        + server.getMappedPort(8080)
                                                        + "/")
                                        .then()
                                        .statusCode(200);
                                return true;
                            } catch (Exception e) {
                                return false;
                            }
                        });
    }

    @Override
    @AfterEach
    public void tearDown() throws Exception {
        super.tearDown();
    }

    /**
     * Test that accessing the web UI without authentication credentials returns 401 Unauthorized.
     */
    @Test
    public void testAccessWithoutCredentials() {
        given().get(HTTP + server.getHost() + COLON + server.getMappedPort(8080) + "/")
                .then()
                .statusCode(401);
    }

    /** Test that accessing the web UI with incorrect credentials returns 401 Unauthorized. */
    @Test
    public void testAccessWithIncorrectCredentials() {
        String credentials = "wronguser:wrongpassword";
        String encodedCredentials = Base64.getEncoder().encodeToString(credentials.getBytes());

        given().header(BASIC_AUTH_HEADER, BASIC_AUTH_PREFIX + encodedCredentials)
                .get(HTTP + server.getHost() + COLON + server.getMappedPort(8080) + "/")
                .then()
                .statusCode(401);
    }

    /** Test that accessing the web UI with correct credentials returns 200 OK. */
    @Test
    public void testAccessWithCorrectCredentials() {
        String credentials = USERNAME + ":" + PASSWORD;
        String encodedCredentials = Base64.getEncoder().encodeToString(credentials.getBytes());

        given().header(BASIC_AUTH_HEADER, BASIC_AUTH_PREFIX + encodedCredentials)
                .get(HTTP + server.getHost() + COLON + server.getMappedPort(8080) + "/")
                .then()
                .statusCode(200)
                .contentType(containsString("text/html"))
                .body(containsString("<title>Seatunnel Engine UI</title>"));
    }

    /** Test that accessing the REST API with correct credentials returns 200 OK. */
    @Test
    public void testRestApiAccessWithCorrectCredentials() {
        String credentials = USERNAME + ":" + PASSWORD;
        String encodedCredentials = Base64.getEncoder().encodeToString(credentials.getBytes());

        given().header(BASIC_AUTH_HEADER, BASIC_AUTH_PREFIX + encodedCredentials)
                .get(
                        HTTP
                                + server.getHost()
                                + COLON
                                + server.getMappedPort(8080)
                                + RestConstant.REST_URL_OVERVIEW)
                .then()
                .statusCode(200)
                .body("projectVersion", notNullValue());
    }

    /** Test that accessing the REST API with Incorrect credentials returns 200 OK. */
    @Test
    public void testRestApiAccessWithIncorrectCredentials() {
        String credentials = "wronguser:wrongpassword";
        String encodedCredentials = Base64.getEncoder().encodeToString(credentials.getBytes());

        given().header(BASIC_AUTH_HEADER, BASIC_AUTH_PREFIX + encodedCredentials)
                .get(
                        HTTP
                                + server.getHost()
                                + COLON
                                + server.getMappedPort(8080)
                                + RestConstant.REST_URL_OVERVIEW)
                .then()
                .statusCode(401);
    }

    /** Test submitting a job via REST API with correct credentials. */
    @Test
    public void testSubmitJobWithCorrectCredentials() {
        String credentials = USERNAME + ":" + PASSWORD;
        String encodedCredentials = Base64.getEncoder().encodeToString(credentials.getBytes());

        // Simple batch job configuration
        String jobConfig =
                "{\n"
                        + "    \"env\": {\n"
                        + "        \"job.mode\": \"batch\"\n"
                        + "    },\n"
                        + "    \"source\": [\n"
                        + "        {\n"
                        + "            \"plugin_name\": \"FakeSource\",\n"
                        + "            \"plugin_output\": \"fake\",\n"
                        + "            \"row.num\": 100,\n"
                        + "            \"schema\": {\n"
                        + "                \"fields\": {\n"
                        + "                    \"name\": \"string\",\n"
                        + "                    \"age\": \"int\",\n"
                        + "                    \"card\": \"int\"\n"
                        + "                }\n"
                        + "            }\n"
                        + "        }\n"
                        + "    ],\n"
                        + "    \"transform\": [\n"
                        + "    ],\n"
                        + "    \"sink\": [\n"
                        + "        {\n"
                        + "            \"plugin_name\": \"InMemory\",\n"
                        + "            \"plugin_input\": \"fake\",\n"
                        + "            \"throw_exception\": true\n"
                        + "        }\n"
                        + "    ]\n"
                        + "}";

        Response response =
                given().header(BASIC_AUTH_HEADER, BASIC_AUTH_PREFIX + encodedCredentials)
                        .contentType(ContentType.JSON)
                        .body(jobConfig)
                        .post(
                                HTTP
                                        + server.getHost()
                                        + COLON
                                        + server.getMappedPort(8080)
                                        + RestConstant.REST_URL_SUBMIT_JOB);

        response.then().statusCode(200).body("jobId", notNullValue());
    }

    /** Test submitting a job via REST API with incorrect credentials. */
    @Test
    public void testSubmitJobWithIncorrectCredentials() {
        String credentials = "wronguser:wrongpassword";
        String encodedCredentials = Base64.getEncoder().encodeToString(credentials.getBytes());

        // Simple batch job configuration
        String jobConfig =
                "{\n"
                        + "  \"env\": {\n"
                        + "    \"job.mode\": \"BATCH\"\n"
                        + "  },\n"
                        + "  \"source\": {\n"
                        + "    \"FakeSource\": {\n"
                        + "      \"plugin_output\": \"fake\",\n"
                        + "      \"row.num\": 100,\n"
                        + "      \"schema\": {\n"
                        + "        \"fields\": {\n"
                        + "          \"id\": \"int\",\n"
                        + "          \"name\": \"string\"\n"
                        + "        }\n"
                        + "      }\n"
                        + "    }\n"
                        + "  },\n"
                        + "  \"sink\": {\n"
                        + "    \"Console\": {\n"
                        + "      \"plugin_input\": \"fake\"\n"
                        + "    }\n"
                        + "  }\n"
                        + "}";

        given().header(BASIC_AUTH_HEADER, BASIC_AUTH_PREFIX + encodedCredentials)
                .contentType(ContentType.JSON)
                .body(jobConfig)
                .post(
                        HTTP
                                + server.getHost()
                                + COLON
                                + server.getMappedPort(8080)
                                + RestConstant.REST_URL_SUBMIT_JOB)
                .then()
                .statusCode(401);
    }

    /** Create a SeaTunnel container with basic authentication enabled. */
    private GenericContainer<?> createSeaTunnelContainerWithBasicAuth()
            throws IOException, InterruptedException {
        String configPath =
                PROJECT_ROOT_PATH
                        + "/seatunnel-e2e/seatunnel-engine-e2e/connector-seatunnel-e2e-base/src/test/resources/basic-auth/seatunnel.yaml";

        return createSeaTunnelContainerWithFakeSourceAndInMemorySink(configPath);
    }
}
