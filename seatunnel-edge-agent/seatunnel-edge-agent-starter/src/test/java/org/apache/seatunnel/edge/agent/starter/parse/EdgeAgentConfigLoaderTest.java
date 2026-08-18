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

package org.apache.seatunnel.edge.agent.starter.parse;

import org.apache.seatunnel.edge.agent.connector.config.EdgeInputOptions;
import org.apache.seatunnel.edge.agent.connector.config.FileCollectOptions;
import org.apache.seatunnel.edge.agent.starter.config.AgentRuntimeConfig;
import org.apache.seatunnel.edge.agent.starter.config.EdgeAgentRuntimeOptions;
import org.apache.seatunnel.edge.agent.starter.config.EdgeDeliveryGuarantee;
import org.apache.seatunnel.edge.agent.transport.config.EdgeOutputOptions;
import org.apache.seatunnel.edge.agent.transport.config.EdgeTransportConfig;
import org.apache.seatunnel.edge.agent.transport.config.EdgeTransportOptions;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;

public class EdgeAgentConfigLoaderTest {

    @TempDir Path tempDir;

    @Test
    void loadAppliesDefaultInputAndOutputTypes() throws Exception {
        Path yamlPath = writeYaml(minimalYaml(null, null, "target/test-wal.db"));

        EdgeAgentResolvedConfig resolved = EdgeAgentConfigLoader.load(yamlPath, tempDir);

        Assertions.assertEquals("file", resolved.getInputType());
        Assertions.assertEquals("console", resolved.getOutputType());
        Assertions.assertEquals("file", resolved.getInputConfig().get(EdgeInputOptions.TYPE));
        Assertions.assertEquals("console", resolved.getOutputConfig().get(EdgeOutputOptions.TYPE));
        Assertions.assertEquals("target/test-wal.db", resolved.getRuntimeConfig().getSqlitePath());
        Assertions.assertEquals(
                EdgeDeliveryGuarantee.BEST_EFFORT,
                resolved.getRuntimeConfig().getDeliveryGuarantee());
    }

    @Test
    void loadNormalizesExplicitTypes() throws Exception {
        Path yamlPath = writeYaml(minimalYaml("FILE", "TRANSPORT", "target/test-wal.db"));

        EdgeAgentResolvedConfig resolved = EdgeAgentConfigLoader.load(yamlPath, tempDir);

        Assertions.assertEquals("file", resolved.getInputType());
        Assertions.assertEquals("transport", resolved.getOutputType());
    }

    @Test
    void loadMapsInputOutputAndQueueSlices() throws Exception {
        String yaml =
                "agent:\n"
                        + "  id: edge-agent-001\n"
                        + "  bulk-max-size: 4\n"
                        + "input:\n"
                        + "  id: file-in\n"
                        + "  type: file\n"
                        + "  paths:\n"
                        + "    - /var/log/*.log\n"
                        + "queue:\n"
                        + "  sqlite-path: data/wal.db\n"
                        + "output:\n"
                        + "  type: transport\n"
                        + "  endpoint: localhost:10001\n"
                        + "  token: tok\n";
        EdgeAgentResolvedConfig resolved = EdgeAgentConfigLoader.load(writeYaml(yaml), tempDir);

        Assertions.assertEquals("file-in", resolved.getInputConfig().get(FileCollectOptions.ID));
        Assertions.assertFalse(resolved.getInputConfig().getSourceMap().containsKey("queue"));
        Assertions.assertFalse(resolved.getInputConfig().getSourceMap().containsValue(null));
        Assertions.assertEquals(
                "transport", resolved.getOutputConfig().get(EdgeOutputOptions.TYPE));
        Assertions.assertEquals(
                "localhost:10001", resolved.getOutputConfig().get(EdgeTransportOptions.ENDPOINT));
        Assertions.assertEquals("data/wal.db", resolved.getRuntimeConfig().getSqlitePath());
        Assertions.assertEquals(4, resolved.getRuntimeConfig().getBatchBulkMaxSize());
        Assertions.assertEquals("edge-agent-001", resolved.getAgentId());
    }

    @Test
    void loadUsesDefaultDeliveryGuaranteeWhenOmitted() throws Exception {
        Path yamlPath = writeYaml(minimalYaml(null, null, "target/test-wal.db"));
        EdgeAgentResolvedConfig resolved = EdgeAgentConfigLoader.load(yamlPath, tempDir);
        Assertions.assertEquals(
                EdgeDeliveryGuarantee.BEST_EFFORT,
                resolved.getRuntimeConfig().getDeliveryGuarantee());

        String yamlWithAgentIdOnly =
                "agent:\n  id: edge-agent-001\n"
                        + minimalYamlBodyWithoutAgent("target/test-wal-2.db");
        EdgeAgentResolvedConfig withAgentId =
                EdgeAgentConfigLoader.load(writeYaml(yamlWithAgentIdOnly), tempDir);
        Assertions.assertEquals(
                EdgeDeliveryGuarantee.BEST_EFFORT,
                withAgentId.getRuntimeConfig().getDeliveryGuarantee());
        Assertions.assertEquals("edge-agent-001", withAgentId.getAgentId());
    }

    @Test
    void loadAcceptsDeliveryGuaranteeAliases() throws Exception {
        for (String guarantee : new String[] {"best-effort", "best_effort", "BEST_EFFORT"}) {
            String yaml =
                    "agent:\n"
                            + "  id: agent-1\n"
                            + "  delivery-guarantee: "
                            + guarantee
                            + "\n"
                            + minimalYamlBodyWithoutAgent("target/test-wal.db");
            EdgeAgentResolvedConfig resolved = EdgeAgentConfigLoader.load(writeYaml(yaml), tempDir);
            Assertions.assertEquals(
                    EdgeDeliveryGuarantee.BEST_EFFORT,
                    resolved.getRuntimeConfig().getDeliveryGuarantee());
        }
    }

    @Test
    void loadRejectsUnsupportedDeliveryGuaranteeValues() throws Exception {
        for (String guarantee : new String[] {"UNKNOWN_MODE", "exactly-once"}) {
            String yaml =
                    "agent:\n"
                            + "  id: agent-1\n"
                            + "  delivery-guarantee: "
                            + guarantee
                            + "\n"
                            + minimalYamlBodyWithoutAgent("target/test-wal.db");
            Path yamlPath = writeYaml(yaml);

            IllegalArgumentException exception =
                    Assertions.assertThrows(
                            IllegalArgumentException.class,
                            () -> EdgeAgentConfigLoader.load(yamlPath, tempDir));

            Assertions.assertTrue(
                    exception.getMessage().contains("Unsupported agent.delivery-guarantee"));
        }
    }

    @Test
    void loadRejectsMissingInput() throws Exception {
        Path yamlPath = writeYaml("queue:\n  sqlite-path: data/wal.db\noutput:\n  type: console\n");

        IllegalArgumentException exception =
                Assertions.assertThrows(
                        IllegalArgumentException.class,
                        () -> EdgeAgentConfigLoader.load(yamlPath, tempDir));

        Assertions.assertTrue(exception.getMessage().contains("input must be defined"));
    }

    @Test
    void loadAppliesDefaultSqlitePathWhenOmitted() throws Exception {
        String yaml =
                "input:\n"
                        + "  id: in-1\n"
                        + "  paths: [\"/tmp/a.log\"]\n"
                        + "output:\n"
                        + "  type: console\n";
        EdgeAgentResolvedConfig withoutQueue = EdgeAgentConfigLoader.load(writeYaml(yaml), tempDir);
        Assertions.assertEquals("data/wal.db", withoutQueue.getRuntimeConfig().getSqlitePath());

        String yamlEmptyQueue =
                "input:\n"
                        + "  id: in-1\n"
                        + "  paths: [\"/tmp/a.log\"]\n"
                        + "queue: {}\n"
                        + "output:\n"
                        + "  type: console\n";
        EdgeAgentResolvedConfig emptyQueue =
                EdgeAgentConfigLoader.load(writeYaml(yamlEmptyQueue), tempDir);
        Assertions.assertEquals("data/wal.db", emptyQueue.getRuntimeConfig().getSqlitePath());
    }

    @Test
    void loadRejectsLegacyInputQueue() throws Exception {
        String yaml =
                "input:\n"
                        + "  id: in-1\n"
                        + "  paths: [\"/tmp/a.log\"]\n"
                        + "  queue:\n"
                        + "    sqlite-path: data/wal.db\n";
        Path yamlPath = writeYaml(yaml);

        IllegalArgumentException exception =
                Assertions.assertThrows(
                        IllegalArgumentException.class,
                        () -> EdgeAgentConfigLoader.load(yamlPath, tempDir));

        Assertions.assertTrue(exception.getMessage().contains("input.queue"));
        Assertions.assertTrue(exception.getMessage().contains("top-level queue"));
    }

    @Test
    void loadAutoGeneratesStableIdsWhenOmitted() throws Exception {
        String yaml =
                "input:\n" + "  paths: [\"/tmp/a.log\"]\n" + "output:\n" + "  type: console\n";
        Path yamlPath = writeYaml(yaml);

        EdgeAgentResolvedConfig first = EdgeAgentConfigLoader.load(yamlPath, tempDir);
        EdgeAgentResolvedConfig second = EdgeAgentConfigLoader.load(yamlPath, tempDir);

        Assertions.assertNotNull(first.getAgentId());
        Assertions.assertNotNull(first.getInputId());
        Assertions.assertNotNull(first.getOutputId());
        Assertions.assertEquals(first.getAgentId(), second.getAgentId());
        Assertions.assertEquals(first.getInputId(), second.getInputId());
        Assertions.assertEquals(first.getOutputId(), second.getOutputId());
        Assertions.assertEquals(
                first.getInputId(), first.getInputConfig().get(FileCollectOptions.ID));

        Path idFile = tempDir.resolve(EdgeAgentIdResolver.ID_FILE_NAME);
        Assertions.assertTrue(Files.isRegularFile(idFile));
        String content = new String(Files.readAllBytes(idFile), StandardCharsets.UTF_8);
        Assertions.assertTrue(content.contains(EdgeAgentIdResolver.KEY_AGENT_ID + '='));
        Assertions.assertTrue(content.contains(EdgeAgentIdResolver.KEY_INPUT_ID + '='));
        Assertions.assertTrue(content.contains(EdgeAgentIdResolver.KEY_OUTPUT_ID + '='));
    }

    @Test
    void loadPreservesExplicitIdsWithoutOverwritingIdFile() throws Exception {
        Path idFile = tempDir.resolve(EdgeAgentIdResolver.ID_FILE_NAME);
        Files.write(
                idFile,
                (EdgeAgentIdResolver.KEY_AGENT_ID
                                + "=file-agent\n"
                                + EdgeAgentIdResolver.KEY_INPUT_ID
                                + "=file-input\n"
                                + EdgeAgentIdResolver.KEY_OUTPUT_ID
                                + "=file-output\n")
                        .getBytes(StandardCharsets.UTF_8));

        String yaml =
                "agent:\n"
                        + "  id: agent-explicit\n"
                        + "input:\n"
                        + "  id: input-explicit\n"
                        + "  paths: [\"/tmp/a.log\"]\n"
                        + "output:\n"
                        + "  id: output-explicit\n"
                        + "  type: console\n";
        Path yamlPath = writeYaml(yaml);

        EdgeAgentResolvedConfig resolved = EdgeAgentConfigLoader.load(yamlPath, tempDir);

        Assertions.assertEquals("agent-explicit", resolved.getAgentId());
        Assertions.assertEquals("input-explicit", resolved.getInputId());
        Assertions.assertEquals("output-explicit", resolved.getOutputId());

        String persisted = new String(Files.readAllBytes(idFile), StandardCharsets.UTF_8);
        Assertions.assertTrue(persisted.contains("agent.id=agent-explicit"));
        Assertions.assertTrue(persisted.contains("input.id=input-explicit"));
        Assertions.assertTrue(persisted.contains("output.id=output-explicit"));
    }

    @Test
    void loadAutoGeneratesOutputIdWhenOmitted() throws Exception {
        String yaml =
                "agent:\n"
                        + "  id: a1\n"
                        + "input:\n"
                        + "  id: i1\n"
                        + "  paths: [\"/tmp/a.log\"]\n"
                        + "output:\n"
                        + "  type: console\n";
        EdgeAgentResolvedConfig resolved = EdgeAgentConfigLoader.load(writeYaml(yaml), tempDir);

        Assertions.assertNotNull(resolved.getOutputId());
        Assertions.assertFalse(resolved.getOutputId().trim().isEmpty());
    }

    @Test
    void loadUsesDefaultsWhenQueueAndRetryOptionsOmitted() throws Exception {
        String yaml =
                "agent:\n"
                        + "  id: agent-1\n"
                        + "input:\n"
                        + "  id: in-1\n"
                        + "  paths: [\"/tmp/a.log\"]\n"
                        + "queue:\n"
                        + "  sqlite-path: target/minimal-wal.db\n"
                        + "output:\n"
                        + "  type: console\n";
        AgentRuntimeConfig runtime =
                EdgeAgentConfigLoader.load(writeYaml(yaml), tempDir).getRuntimeConfig();

        Assertions.assertEquals(
                EdgeAgentRuntimeOptions.QUEUE_POLL_BATCH_SIZE.defaultValue(),
                runtime.getMaxPollRecords());
        Assertions.assertEquals(
                EdgeAgentRuntimeOptions.RETRY_MAX_ATTEMPTS.defaultValue(),
                runtime.getRetryMaxAttempts());
        Assertions.assertEquals(
                EdgeAgentRuntimeOptions.RETRY_BACKOFF_MS.defaultValue(),
                runtime.getRetryBackoffMs());
        Assertions.assertEquals(
                EdgeAgentRuntimeOptions.RETRY_BACKOFF_MAX_MS.defaultValue(),
                runtime.getRetryBackoffMaxMs());
    }

    @Test
    void loadRejectsTransportWithoutEndpoint() throws Exception {
        String yaml =
                "input:\n"
                        + "  id: in-1\n"
                        + "  paths: [\"/tmp/a.log\"]\n"
                        + "output:\n"
                        + "  type: transport\n"
                        + "  token: tok\n";
        Path yamlPath = writeYaml(yaml);

        Assertions.assertThrows(
                Exception.class, () -> EdgeAgentConfigLoader.load(yamlPath, tempDir));
    }

    @Test
    void loadRejectsTransportWithBlankEndpoint() throws Exception {
        String yaml =
                "input:\n"
                        + "  id: in-1\n"
                        + "  paths: [\"/tmp/a.log\"]\n"
                        + "output:\n"
                        + "  type: transport\n"
                        + "  endpoint: \"\"\n"
                        + "  token: tok\n";
        EdgeAgentResolvedConfig resolved = EdgeAgentConfigLoader.load(writeYaml(yaml), tempDir);

        IllegalArgumentException exception =
                Assertions.assertThrows(
                        IllegalArgumentException.class,
                        () -> EdgeTransportConfig.from(resolved.getOutputConfig()));

        Assertions.assertTrue(exception.getMessage().contains("transport.endpoint"));
    }

    @Test
    void loadRejectsTransportWithoutToken() throws Exception {
        String yaml =
                "input:\n"
                        + "  id: in-1\n"
                        + "  paths: [\"/tmp/a.log\"]\n"
                        + "output:\n"
                        + "  type: transport\n"
                        + "  endpoint: localhost:10001\n";
        Path yamlPath = writeYaml(yaml);

        Assertions.assertThrows(
                Exception.class, () -> EdgeAgentConfigLoader.load(yamlPath, tempDir));
    }

    @Test
    void loadAllowsConsoleWithoutEndpoint() throws Exception {
        String yaml =
                "input:\n"
                        + "  id: in-1\n"
                        + "  paths: [\"/tmp/a.log\"]\n"
                        + "output:\n"
                        + "  type: console\n";
        EdgeAgentResolvedConfig resolved = EdgeAgentConfigLoader.load(writeYaml(yaml), tempDir);
        Assertions.assertEquals("console", resolved.getOutputType());
    }

    @Test
    void runtimeConfigUsesOptionDefaultsFromYaml() throws Exception {
        String yaml =
                "agent:\n"
                        + "  id: edge-agent-001\n"
                        + "  delivery-guarantee: BEST_EFFORT\n"
                        + minimalYamlBodyWithoutAgent("target/test-wal.db");
        AgentRuntimeConfig runtime =
                EdgeAgentConfigLoader.load(writeYaml(yaml), tempDir).getRuntimeConfig();

        Assertions.assertEquals("target/test-wal.db", runtime.getSqlitePath());
        Assertions.assertEquals(
                EdgeAgentRuntimeOptions.QUEUE_RESURRECT_BATCH_SIZE.defaultValue(),
                runtime.getResurrectBatchSize());
    }

    private Path writeYaml(String content) throws Exception {
        Path yamlPath = tempDir.resolve("agent.yaml");
        Files.write(yamlPath, content.getBytes(StandardCharsets.UTF_8));
        return yamlPath;
    }

    private static String minimalYaml(String inputType, String outputType, String sqlitePath) {
        StringBuilder yaml = new StringBuilder();
        yaml.append("agent:\n  id: agent-1\n");
        yaml.append(minimalYamlBodyWithoutAgent(sqlitePath));
        if (inputType != null) {
            int inputLine = yaml.indexOf("input:\n");
            yaml.insert(inputLine + "input:\n".length(), "  type: " + inputType + "\n");
        }
        if (outputType != null) {
            yaml.append("output:\n  type: ").append(outputType).append('\n');
            if ("TRANSPORT".equalsIgnoreCase(outputType)) {
                yaml.append("  endpoint: localhost:10001\n");
                yaml.append("  token: test-token\n");
            }
        }
        return yaml.toString();
    }

    private static String minimalYamlBodyWithoutAgent(String sqlitePath) {
        return "input:\n"
                + "  id: in-1\n"
                + "  paths: [\"/tmp/a.log\"]\n"
                + "queue:\n"
                + "  sqlite-path: "
                + sqlitePath
                + "\n";
    }
}
