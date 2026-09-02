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

package org.apache.seatunnel.e2e.connector.firebase;

import org.apache.seatunnel.e2e.common.TestResource;
import org.apache.seatunnel.e2e.common.TestSuiteBase;
import org.apache.seatunnel.e2e.common.container.TestContainer;

import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.TestTemplate;
import org.testcontainers.Testcontainers;
import org.testcontainers.containers.Container;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.DockerImageName;

import lombok.extern.slf4j.Slf4j;

import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URI;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;

@Slf4j
public class FirebaseSourceIT extends TestSuiteBase implements TestResource {
    private static final String FIREBASE_EMULATOR_IMAGE = "andreysenov/firebase-tools:latest";
    private static final int REALTIME_DATABASE_EMULATOR_PORT = 9000;

    private GenericContainer<?> firebaseEmulator;
    private String emulatorHostUrl;
    private int mappedPort;

    @BeforeAll
    @Override
    public void startUp() throws Exception {
        log.info("Starting Firebase Realtime Database Emulator container...");

        // Define inline firebase.json configuration to bind database emulator to 0.0.0.0
        String firebaseJson =
                "{"
                        + "  \"emulators\": {"
                        + "    \"database\": {"
                        + "      \"host\": \"0.0.0.0\","
                        + "      \"port\": 9000"
                        + "    }"
                        + "  }"
                        + "}";

        // Rule 1: Dynamic Port Assignment via Testcontainers
        firebaseEmulator =
                new GenericContainer<>(DockerImageName.parse(FIREBASE_EMULATOR_IMAGE))
                        .withExposedPorts(REALTIME_DATABASE_EMULATOR_PORT)
                        // Write firebase.json on startup and run the emulator
                        .withCommand(
                                "sh",
                                "-c",
                                "echo '"
                                        + firebaseJson
                                        + "' > firebase.json && firebase emulators:start --only database --project test-project")
                        .waitingFor(
                                Wait.forHttp("/.json")
                                        .forStatusCode(200)
                                        .withStartupTimeout(Duration.ofSeconds(60)))
                        .withLogConsumer(new Slf4jLogConsumer(log));

        firebaseEmulator.start();

        String host = firebaseEmulator.getHost();
        this.mappedPort = firebaseEmulator.getMappedPort(REALTIME_DATABASE_EMULATOR_PORT);

        Testcontainers.exposeHostPorts(mappedPort);
        emulatorHostUrl = String.format("http://%s:%d", host, mappedPort);

        log.info(
                "Firebase Emulator initialized successfully at dynamic endpoint: {}",
                emulatorHostUrl);

        seedTestData();
    }

    private void seedTestData() throws Exception {
        log.info("Seeding structured test dataset to Firebase Emulator REST API...");

        String jsonPayload =
                "{"
                        + "  \"user_1\": {\"id\": 101, \"name\": \"Any Name\", \"score\": 98.5, \"is_active\": true, \"timestamp\": 1700000000000},"
                        + "  \"user_2\": {\"id\": 102, \"name\": \"Apache SeaTunnel\", \"score\": 100.0, \"is_active\": false, \"timestamp\": 1700000005000}"
                        + "}";

        String putEndpointUrl = emulatorHostUrl + "/users.json?ns=test-project";

        // Wait until PUT returns 200 OK
        Awaitility.await()
                .atMost(30, TimeUnit.SECONDS)
                .pollInterval(1, TimeUnit.SECONDS)
                .until(() -> executePut(putEndpointUrl, jsonPayload) == 200);

        log.info("Successfully seeded test records into /users.json?ns=test-project");
    }

    private int executePut(String urlStr, String jsonBody) {
        HttpURLConnection connection = null;
        try {
            URL url = URI.create(urlStr).toURL();
            connection = (HttpURLConnection) url.openConnection();
            connection.setRequestMethod("PUT");
            connection.setDoOutput(true);
            connection.setRequestProperty("Content-Type", "application/json");
            connection.setConnectTimeout(5000);
            connection.setReadTimeout(5000);

            try (OutputStream os = connection.getOutputStream()) {
                byte[] input = jsonBody.getBytes(StandardCharsets.UTF_8);
                os.write(input, 0, input.length);
            }
            return connection.getResponseCode();
        } catch (Exception e) {
            log.warn("Failed to seed data via PUT request to {}: {}", urlStr, e.getMessage());
            return -1;
        } finally {
            if (connection != null) {
                connection.disconnect();
            }
        }
    }

    @TestTemplate
    public void testFirebaseSourceToAssertSink(TestContainer container) throws Exception {
        log.info("Executing Firebase Source E2E Job execution on container engine...");
        String targetUrl = String.format("http://host.testcontainers.internal:%d", mappedPort);

        List<String> variables = Collections.singletonList("URL=" + targetUrl);

        Container.ExecResult execResult =
                container.executeJob("/firebase_source_to_assert.conf", variables);

        Assertions.assertEquals(
                0,
                execResult.getExitCode(),
                "SeaTunnel job failed to execute. Error output: " + execResult.getStderr());

        log.info("Firebase Source E2E integration test executed successfully.");
    }

    @AfterAll
    @Override
    public void tearDown() throws Exception {
        if (firebaseEmulator != null) {
            log.info("Stopping Firebase Emulator container and freeing bound host ports...");
            firebaseEmulator.stop();
            firebaseEmulator = null;
        }
    }
}
