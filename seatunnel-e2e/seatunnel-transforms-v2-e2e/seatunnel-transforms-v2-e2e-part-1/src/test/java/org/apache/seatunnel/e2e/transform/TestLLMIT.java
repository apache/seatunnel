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

package org.apache.seatunnel.e2e.transform;

import org.apache.seatunnel.e2e.common.TestResource;
import org.apache.seatunnel.e2e.common.container.EngineType;
import org.apache.seatunnel.e2e.common.container.TestContainer;
import org.apache.seatunnel.e2e.common.junit.DisabledOnContainer;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.TestTemplate;
import org.testcontainers.containers.Container;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.containers.wait.strategy.HttpWaitStrategy;
import org.testcontainers.lifecycle.Startables;
import org.testcontainers.utility.DockerImageName;
import org.testcontainers.utility.DockerLoggerFactory;
import org.testcontainers.utility.MountableFile;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.util.Arrays;
import java.util.Optional;
import java.util.stream.Stream;

public class TestLLMIT extends TestSuiteBase implements TestResource {
    private static final String TMP_DIR = "/tmp";
    private GenericContainer<?> mockserverContainer;
    private static final String IMAGE = "mockserver/mockserver:5.14.0";
    private static final String HTTP_RESPONSE_MARKER = "LLM_HTTP_RESPONSE_SECRET_503";
    private static final String MALFORMED_RESPONSE_MARKER = "LLM_PARSE_RESPONSE_SECRET_200";

    @BeforeAll
    @Override
    public void startUp() {
        Optional<URL> resource =
                Optional.ofNullable(TestLLMIT.class.getResource("/mockserver-config.json"));
        this.mockserverContainer =
                new GenericContainer<>(DockerImageName.parse(IMAGE))
                        .withNetwork(NETWORK)
                        .withNetworkAliases("mockserver")
                        .withExposedPorts(1080)
                        .withCopyFileToContainer(
                                MountableFile.forHostPath(
                                        new File(
                                                        resource.orElseThrow(
                                                                        () ->
                                                                                new IllegalArgumentException(
                                                                                        "Can not get config file of mockServer"))
                                                                .getPath())
                                                .getAbsolutePath()),
                                TMP_DIR + "/mockserver-config.json")
                        .withEnv(
                                "MOCKSERVER_INITIALIZATION_JSON_PATH",
                                TMP_DIR + "/mockserver-config.json")
                        .withEnv("MOCKSERVER_LOG_LEVEL", "WARN")
                        .withLogConsumer(new Slf4jLogConsumer(DockerLoggerFactory.getLogger(IMAGE)))
                        .waitingFor(new HttpWaitStrategy().forPath("/").forStatusCode(404));
        Startables.deepStart(Stream.of(mockserverContainer)).join();
    }

    @AfterAll
    @Override
    public void tearDown() throws Exception {
        if (mockserverContainer != null) {
            mockserverContainer.stop();
        }
    }

    @TestTemplate
    public void testLLMWithOpenAI(TestContainer container)
            throws IOException, InterruptedException {
        Container.ExecResult execResult = container.executeJob("/llm_openai_transform.conf");
        Assertions.assertEquals(0, execResult.getExitCode());
    }

    @TestTemplate
    public void testLLMWithOpenAIMultiTable(TestContainer container)
            throws IOException, InterruptedException {
        Container.ExecResult execResult =
                container.executeJob("/llm_openai_transform_multi_table.conf");
        Assertions.assertEquals(0, execResult.getExitCode());
    }

    @TestTemplate
    public void testLLMWithMicrosoft(TestContainer container)
            throws IOException, InterruptedException {
        Container.ExecResult execResult = container.executeJob("/llm_microsoft_transform.conf");
        Assertions.assertEquals(0, execResult.getExitCode());
    }

    @TestTemplate
    public void testLLMWithOpenAIBoolean(TestContainer container)
            throws IOException, InterruptedException {
        Container.ExecResult execResult =
                container.executeJob("/llm_openai_transform_boolean.conf");
        Assertions.assertEquals(0, execResult.getExitCode());
    }

    @TestTemplate
    public void testLLMWithOpenAIStrictBoolean(TestContainer container)
            throws IOException, InterruptedException {
        Container.ExecResult execResult =
                container.executeJob("/llm_openai_transform_boolean_strict.conf");
        Assertions.assertEquals(0, execResult.getExitCode());
    }

    @TestTemplate
    public void testLLMWithOpenAIStrictBooleanRejectsInvalidOutput(TestContainer container)
            throws IOException, InterruptedException {
        Container.ExecResult execResult =
                container.executeJob("/llm_openai_transform_boolean_strict_invalid.conf");
        Assertions.assertNotEquals(0, execResult.getExitCode());
        String output = execResult.getStdout() + execResult.getStderr();
        Assertions.assertTrue(output.contains("strict_boolean_output"), output);
        Assertions.assertTrue(output.contains("exactly one non-null true or false"), output);
    }

    @TestTemplate
    public void testLLMWithOpenAIColumns(TestContainer container)
            throws IOException, InterruptedException {
        Container.ExecResult execResult =
                container.executeJob("/llm_openai_transform_columns.conf");
        Assertions.assertEquals(0, execResult.getExitCode());
    }

    @TestTemplate
    public void testLLMWithOpenAIOutputColumnName(TestContainer container)
            throws IOException, InterruptedException {
        Container.ExecResult execResult =
                container.executeJob("/llm_openai_transform_custom_output_name.conf");
        Assertions.assertEquals(0, execResult.getExitCode());
    }

    @TestTemplate
    public void testLLMWithCustomModel(TestContainer container)
            throws IOException, InterruptedException {
        Container.ExecResult execResult = container.executeJob("/llm_transform_custom.conf");
        Assertions.assertEquals(0, execResult.getExitCode());
    }

    @TestTemplate
    public void testLLMWithKimiAI(TestContainer container)
            throws IOException, InterruptedException {
        Container.ExecResult execResult = container.executeJob("/llm_kimiai_transform.conf");
        Assertions.assertEquals(0, execResult.getExitCode());
    }

    @TestTemplate
    @DisabledOnContainer(
            value = {},
            type = {EngineType.FLINK, EngineType.SPARK},
            disabledReason = "Verifies Zeta job failure diagnostics")
    public void testLLMWithOpenAIHttpErrorDiagnostics(TestContainer container)
            throws IOException, InterruptedException {
        assertSafeFailure(container, "OPENAI", "http-error", "TEMPORARY_REMOTE_ERROR", 503);
    }

    @TestTemplate
    @DisabledOnContainer(
            value = {},
            type = {EngineType.FLINK, EngineType.SPARK},
            disabledReason = "Verifies Zeta job failure diagnostics")
    public void testLLMWithCustomHttpErrorDiagnostics(TestContainer container)
            throws IOException, InterruptedException {
        assertSafeFailure(container, "CUSTOM", "http-error", "TEMPORARY_REMOTE_ERROR", 503);
    }

    @TestTemplate
    @DisabledOnContainer(
            value = {},
            type = {EngineType.FLINK, EngineType.SPARK},
            disabledReason = "Verifies Zeta job failure diagnostics")
    public void testLLMWithOpenAIMalformedResponseDiagnostics(TestContainer container)
            throws IOException, InterruptedException {
        assertSafeFailure(container, "OPENAI", "malformed", "RESPONSE_PARSE_ERROR", 200);
    }

    @TestTemplate
    @DisabledOnContainer(
            value = {},
            type = {EngineType.FLINK, EngineType.SPARK},
            disabledReason = "Verifies Zeta job failure diagnostics")
    public void testLLMWithCustomMalformedResponseDiagnostics(TestContainer container)
            throws IOException, InterruptedException {
        assertSafeFailure(container, "CUSTOM", "malformed", "RESPONSE_PARSE_ERROR", 200);
    }

    private void assertSafeFailure(
            TestContainer container,
            String provider,
            String endpoint,
            String errorType,
            Integer httpStatus)
            throws IOException, InterruptedException {
        Container.ExecResult execResult =
                container.executeJob(
                        "/llm_safe_diagnostics.conf",
                        Arrays.asList(
                                "diagnostic_provider=" + provider,
                                "diagnostic_api_path=http://mockserver:1080/llm-diagnostics/"
                                        + provider
                                        + "/"
                                        + endpoint));
        Assertions.assertNotEquals(0, execResult.getExitCode(), "The LLM job must fail");
        String output = execResult.getStdout() + execResult.getStderr();
        // These markers occur only in mock responses, never in the logged job configuration.
        Assertions.assertFalse(
                output.contains(HTTP_RESPONSE_MARKER), "HTTP response leaked into job output");
        Assertions.assertFalse(
                output.contains(MALFORMED_RESPONSE_MARKER),
                "Malformed response leaked into job output");
        Assertions.assertTrue(
                output.contains("LLM inference failed"), "Missing LLM failure diagnostic boundary");
        Assertions.assertTrue(
                output.contains(
                        "provider=" + provider + ", model=test-model, errorType=" + errorType),
                "Missing safe provider, model, and error type metadata");
        if (httpStatus != null) {
            Assertions.assertTrue(
                    output.contains("httpStatus=" + httpStatus), "Missing HTTP status metadata");
        }
    }
}
