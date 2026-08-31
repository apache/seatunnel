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

package org.apache.seatunnel.e2e.connector.splunk;

import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.JsonNode;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.ObjectMapper;

import org.apache.seatunnel.e2e.common.TestResource;
import org.apache.seatunnel.e2e.common.TestSuiteBase;
import org.apache.seatunnel.e2e.common.container.TestContainer;

import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.TestTemplate;
import org.testcontainers.containers.Container;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.lifecycle.Startables;
import org.testcontainers.utility.DockerImageName;
import org.testcontainers.utility.DockerLoggerFactory;

import lombok.extern.slf4j.Slf4j;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

/**
 * Verifies that a FakeSource to Splunk sink job actually lands its events in the target index,
 * including the configured HEC metadata mapping.
 */
@Slf4j
public class SplunkIT extends TestSuiteBase implements TestResource {

    private static final String SPLUNK_DOCKER_IMAGE = "splunk/splunk:9.3.2";
    private static final String SPLUNK_HOST = "splunk-e2e";
    private static final int HEC_PORT = 8088;
    private static final int MANAGEMENT_PORT = 8089;

    private static final String SPLUNK_PASSWORD = "SeaTunnel@2026";
    /** Must match the token configured in fake_to_splunk.conf. */
    private static final String HEC_TOKEN = "00000000-0000-0000-0000-0000000000ff";

    private static final String INDEX = "main";
    private static final String SOURCE = "seatunnel";
    private static final String SOURCE_TYPE = "seatunnel_e2e";

    /** Number of rows produced by fake_to_splunk.conf. */
    private static final int EXPECTED_EVENT_COUNT = 5;

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    private GenericContainer<?> splunkContainer;

    @BeforeAll
    @Override
    public void startUp() throws Exception {
        splunkContainer =
                new GenericContainer<>(DockerImageName.parse(SPLUNK_DOCKER_IMAGE))
                        .withNetwork(NETWORK)
                        .withNetworkAliases(SPLUNK_HOST)
                        .withExposedPorts(HEC_PORT, MANAGEMENT_PORT)
                        .withEnv("SPLUNK_START_ARGS", "--accept-license")
                        .withEnv("SPLUNK_PASSWORD", SPLUNK_PASSWORD)
                        .withEnv("SPLUNK_HEC_TOKEN", HEC_TOKEN)
                        .withStartupAttempts(2)
                        .withStartupTimeout(Duration.ofMinutes(10))
                        // The collector runs behind the self-signed certificate generated at
                        // install time, which is exactly the deployment shape the sink's
                        // tls_verify_certificate option exists for.
                        .waitingFor(
                                Wait.forHttp("/services/collector/health")
                                        .forPort(HEC_PORT)
                                        .usingTls()
                                        .allowInsecure()
                                        .forStatusCode(200)
                                        .withStartupTimeout(Duration.ofMinutes(10)))
                        .withLogConsumer(
                                new Slf4jLogConsumer(
                                        DockerLoggerFactory.getLogger(SPLUNK_DOCKER_IMAGE)));
        Startables.deepStart(Stream.of(splunkContainer)).join();
        log.info("Splunk container started");
    }

    @AfterAll
    @Override
    public void tearDown() {
        if (splunkContainer != null) {
            splunkContainer.close();
        }
    }

    @TestTemplate
    public void testFakeSourceToSplunkSink(TestContainer container) throws Exception {
        // The suite runs once per engine container against the same index, so the assertion is on
        // the delta rather than on an absolute count.
        int countBefore = searchEventCount();

        Container.ExecResult execResult = container.executeJob("/fake_to_splunk.conf");
        Assertions.assertEquals(0, execResult.getExitCode(), execResult.getStderr());

        Awaitility.await()
                .atMost(120, TimeUnit.SECONDS)
                .pollInterval(2, TimeUnit.SECONDS)
                .untilAsserted(
                        () ->
                                Assertions.assertEquals(
                                        countBefore + EXPECTED_EVENT_COUNT, searchEventCount()));

        assertEventContentAndMetadata();
    }

    /** Asserts the HEC metadata mapping and the event body written by the sink. */
    private void assertEventContentAndMetadata() throws Exception {
        List<JsonNode> results =
                runSearch(
                        String.format(
                                "search index=%s sourcetype=%s | eval epoch=_time "
                                        + "| table id, message, host, source, sourcetype, epoch",
                                INDEX, SOURCE_TYPE));
        Assertions.assertFalse(results.isEmpty(), "no events were returned by the Splunk search");

        Set<String> messages = new HashSet<>();
        Set<String> hosts = new HashSet<>();
        Set<String> epochs = new HashSet<>();
        for (JsonNode result : results) {
            // sourcetype and source come from the static sink options.
            Assertions.assertEquals(SOURCE_TYPE, result.get("sourcetype").asText());
            Assertions.assertEquals(SOURCE, result.get("source").asText());
            messages.add(result.get("message").asText());
            hosts.add(result.get("host").asText());
            // Splunk reports _time in whole seconds here; the sink sends millisecond precision.
            epochs.add(result.get("epoch").asText().split("\\.")[0]);
        }

        // The event body carries the upstream row fields.
        Assertions.assertTrue(messages.contains("seatunnel event one"), messages.toString());
        Assertions.assertTrue(messages.contains("seatunnel event five"), messages.toString());

        // host comes from the 'hostname' row field via host_field.
        Assertions.assertTrue(hosts.contains("web-01"), hosts.toString());
        Assertions.assertTrue(hosts.contains("web-02"), hosts.toString());
        Assertions.assertTrue(hosts.contains("web-03"), hosts.toString());

        // _time comes from the 'event_time' row field via time_field, read as UTC.
        Assertions.assertTrue(epochs.contains("1786969845"), epochs.toString());
        Assertions.assertTrue(epochs.contains("1786969849"), epochs.toString());
    }

    private int searchEventCount() throws Exception {
        List<JsonNode> results =
                runSearch(
                        String.format(
                                "search index=%s sourcetype=%s | stats count", INDEX, SOURCE_TYPE));
        if (results.isEmpty()) {
            return 0;
        }
        return results.get(0).get("count").asInt();
    }

    /**
     * Runs a Splunk search through the management API from inside the container, avoiding a
     * dependency on a TLS-tolerant HTTP client in the test classpath.
     */
    private List<JsonNode> runSearch(String search) throws Exception {
        Container.ExecResult result =
                splunkContainer.execInContainer(
                        "curl",
                        "-sk",
                        "-u",
                        "admin:" + SPLUNK_PASSWORD,
                        "https://localhost:" + MANAGEMENT_PORT + "/services/search/jobs/export",
                        "--data-urlencode",
                        "search=" + search,
                        "-d",
                        "output_mode=json");
        Assertions.assertEquals(
                0, result.getExitCode(), "splunk search failed: " + result.getStderr());

        // The export endpoint streams one JSON object per line; only lines carrying a result
        // matter.
        List<JsonNode> results = new ArrayList<>();
        for (String line : result.getStdout().split("\n")) {
            if (line.trim().isEmpty()) {
                continue;
            }
            JsonNode node = OBJECT_MAPPER.readTree(line);
            if (node.has("result")) {
                results.add(node.get("result"));
            }
        }
        return results;
    }
}
