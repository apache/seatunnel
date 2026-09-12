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

import org.apache.seatunnel.shade.com.fasterxml.jackson.core.type.TypeReference;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.ObjectMapper;

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
import org.testcontainers.utility.DockerImageName;

import lombok.extern.slf4j.Slf4j;

import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URI;
import java.net.URL;
import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

@Slf4j
public class FirebaseSinkIT extends TestSuiteBase implements TestResource {

    private static final String FIREBASE_EMULATOR_IMAGE = "andreysenov/firebase-tools:latest";
    private static final int REALTIME_DATABASE_EMULATOR_PORT = 9000;
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    private GenericContainer<?> firebaseEmulator;
    private String emulatorHostUrl;
    private int mappedPort;

    @BeforeAll
    @Override
    public void startUp() throws Exception {
        log.info("Starting Firebase Realtime Database Emulator container for Sink IT...");

        String firebaseJson =
                "{"
                        + "  \"emulators\": {"
                        + "    \"database\": {"
                        + "      \"host\": \"0.0.0.0\","
                        + "      \"port\": 9000"
                        + "    }"
                        + "  }"
                        + "}";

        firebaseEmulator =
                new GenericContainer<>(DockerImageName.parse(FIREBASE_EMULATOR_IMAGE))
                        .withExposedPorts(REALTIME_DATABASE_EMULATOR_PORT)
                        .withCommand(
                                "sh",
                                "-c",
                                "echo '"
                                        + firebaseJson
                                        + "' > firebase.json && firebase emulators:start --only database --project test-project")
                        .waitingFor(
                                org.testcontainers.containers.wait.strategy.Wait.forHttp("/.json")
                                        .forStatusCode(200)
                                        .withStartupTimeout(Duration.ofSeconds(60)))
                        .withLogConsumer(new Slf4jLogConsumer(log));

        firebaseEmulator.start();

        String host = firebaseEmulator.getHost();
        this.mappedPort = firebaseEmulator.getMappedPort(REALTIME_DATABASE_EMULATOR_PORT);

        Testcontainers.exposeHostPorts(mappedPort);
        emulatorHostUrl = String.format("http://%s:%d", host, mappedPort);

        log.info(
                "Firebase Emulator initialized successfully for Sink IT at endpoint: {}",
                emulatorHostUrl);
    }

    @TestTemplate
    public void testFirebaseSink(TestContainer container) throws Exception {
        log.info("Executing Firebase Sink E2E Job execution on container engine...");

        // Include ?ns=test-project so HTTP requests from the sink append or target the correct
        // emulator namespace
        String targetUrl =
                String.format("http://host.testcontainers.internal:%d?ns=test-project", mappedPort);

        List<String> variables = Collections.singletonList("URL=" + targetUrl);

        Container.ExecResult execResult =
                container.executeJob("/fake_source_to_firebase.conf", variables);

        Assertions.assertEquals(
                0,
                execResult.getExitCode(),
                "SeaTunnel job failed to execute. Error output: " + execResult.getStderr());

        // Verify data was written correctly to Firebase Realtime Database
        verifySinkData();

        log.info("Firebase Sink E2E integration test executed and verified successfully.");
    }

    private void verifySinkData() {
        String queryEndpointUrl = emulatorHostUrl + "/users.json?ns=test-project";

        // Wait until records are available in the emulator
        Awaitility.await()
                .atMost(30, TimeUnit.SECONDS)
                .pollInterval(1, TimeUnit.SECONDS)
                .until(
                        () -> {
                            Map<String, Object> usersMap = fetchFirebaseData(queryEndpointUrl);
                            return usersMap != null && usersMap.size() >= 2;
                        });

        Map<String, Object> users = fetchFirebaseData(queryEndpointUrl);
        Assertions.assertNotNull(users, "Data read from Firebase should not be null");

        // Verify specific key node creation (users/101 and users/102)
        Assertions.assertTrue(users.containsKey("101"), "Firebase should contain key '101'");
        Assertions.assertTrue(users.containsKey("102"), "Firebase should contain key '102'");

        @SuppressWarnings("unchecked")
        Map<String, Object> user101 = (Map<String, Object>) users.get("101");
        Assertions.assertEquals(101, user101.get("id"));
        Assertions.assertEquals("Alice", user101.get("name"));
        Assertions.assertEquals(true, user101.get("is_active"));

        log.info("Verified record contents for node 101 successfully.");
    }

    private Map<String, Object> fetchFirebaseData(String urlStr) {
        HttpURLConnection connection = null;
        try {
            URL url = URI.create(urlStr).toURL();
            connection = (HttpURLConnection) url.openConnection();
            connection.setRequestMethod("GET");
            connection.setRequestProperty("Accept", "application/json");
            connection.setConnectTimeout(5000);
            connection.setReadTimeout(5000);

            if (connection.getResponseCode() == 200) {
                try (InputStream is = connection.getInputStream()) {
                    return OBJECT_MAPPER.readValue(is, new TypeReference<Map<String, Object>>() {});
                }
            }
            return null;
        } catch (Exception e) {
            log.warn(
                    "Failed to fetch data from Firebase Emulator at {}: {}",
                    urlStr,
                    e.getMessage());
            return null;
        } finally {
            if (connection != null) {
                connection.disconnect();
            }
        }
    }

    @AfterAll
    @Override
    public void tearDown() throws Exception {
        if (firebaseEmulator != null) {
            log.info("Stopping Firebase Emulator container...");
            firebaseEmulator.stop();
            firebaseEmulator = null;
        }
    }
}
