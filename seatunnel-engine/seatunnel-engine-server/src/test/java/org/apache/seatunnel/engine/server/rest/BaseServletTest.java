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

package org.apache.seatunnel.engine.server.rest;

import org.apache.seatunnel.config.sql.SqlConfigBuilder;
import org.apache.seatunnel.engine.common.config.SeaTunnelConfig;
import org.apache.seatunnel.engine.common.config.server.HttpConfig;
import org.apache.seatunnel.engine.common.runtime.ExecutionMode;
import org.apache.seatunnel.engine.common.utils.PassiveCompletableFuture;
import org.apache.seatunnel.engine.core.dag.logical.LogicalDag;
import org.apache.seatunnel.engine.core.job.JobImmutableInformation;
import org.apache.seatunnel.engine.server.AbstractSeaTunnelServerTest;
import org.apache.seatunnel.engine.server.SeaTunnelServer;
import org.apache.seatunnel.engine.server.SeaTunnelServerStarter;
import org.apache.seatunnel.engine.server.TestUtils;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import com.hazelcast.config.Config;
import com.hazelcast.internal.serialization.Data;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.util.Collections;

class BaseServletTest extends AbstractSeaTunnelServerTest {

    private static final int HTTP_PORT = 18080;

    private static final Long JOB_1 = System.currentTimeMillis() + 1L;

    @BeforeAll
    void setUp() {
        String name = this.getClass().getName();
        Config hazelcastConfig = Config.loadFromString(getHazelcastConfig());
        hazelcastConfig.setClusterName(TestUtils.getClusterName("RestApiServletTest_" + name));
        SeaTunnelConfig seaTunnelConfig = loadSeaTunnelConfig();
        seaTunnelConfig.setHazelcastConfig(hazelcastConfig);
        seaTunnelConfig.getEngineConfig().setMode(ExecutionMode.LOCAL);

        HttpConfig httpConfig = seaTunnelConfig.getEngineConfig().getHttpConfig();
        httpConfig.setEnabled(true);
        httpConfig.setPort(HTTP_PORT);

        instance = SeaTunnelServerStarter.createHazelcastInstance(seaTunnelConfig);
        nodeEngine = instance.node.nodeEngine;
        server = nodeEngine.getService(SeaTunnelServer.SERVICE_NAME);
        LOGGER = nodeEngine.getLogger(AbstractSeaTunnelServerTest.class);
    }

    @Test
    void testWriteJsonWithObject() throws IOException {
        startJob(JOB_1, "fake_to_console.conf");
        testLogRestApiResponse("html");
        testLogRestApiResponse("JSON");
    }

    @Test
    void testSqlConfigParsing() throws Exception {
        String sqlContent =
                "/* config\n"
                        + "env {\n"
                        + "  parallelism = 1\n"
                        + "  job.mode = \"BATCH\"\n"
                        + "}\n"
                        + "*/\n"
                        + "\n"
                        + "CREATE TABLE test_source (\n"
                        + "    id INT,\n"
                        + "    name STRING\n"
                        + ") WITH (\n"
                        + "    'connector' = 'FakeSource',\n"
                        + "    'rows' = '[{ fields = [1, \"test\"], kind = INSERT }]',\n"
                        + "    'schema' = '{ fields { id = \"int\", name = \"string\" } }',\n"
                        + "    'type' = 'source'\n"
                        + ");\n"
                        + "\n"
                        + "CREATE TABLE test_sink (\n"
                        + "    id INT,\n"
                        + "    name STRING\n"
                        + ") WITH (\n"
                        + "    'connector' = 'Console',\n"
                        + "    'type' = 'sink'\n"
                        + ");\n"
                        + "\n"
                        + "INSERT INTO test_sink SELECT * FROM test_source;";

        org.apache.seatunnel.shade.com.typesafe.config.Config config =
                SqlConfigBuilder.of(sqlContent);

        Assertions.assertNotNull(config);
        Assertions.assertTrue(config.hasPath("source"));
        Assertions.assertTrue(config.hasPath("transform"));
        Assertions.assertTrue(config.hasPath("sink"));

        // SQL with INSERT INTO ... SELECT FROM ... will create a transform step
        Assertions.assertTrue(
                config.hasPath("transform"),
                "Transform should be created for INSERT INTO ... SELECT FROM ... statement");

        // Verify source configuration
        org.apache.seatunnel.shade.com.typesafe.config.Config sourceConfig =
                config.getConfigList("source").get(0);
        Assertions.assertEquals("FakeSource", sourceConfig.getString("plugin_name"));
        Assertions.assertEquals("test_source", sourceConfig.getString("plugin_output"));

        // Verify transform configuration (created by INSERT statement)
        org.apache.seatunnel.shade.com.typesafe.config.Config transformConfig =
                config.getConfigList("transform").get(0);
        Assertions.assertEquals("test_source", transformConfig.getString("plugin_input"));
        Assertions.assertTrue(
                transformConfig.getString("plugin_output").startsWith("test_source__temp"));
        Assertions.assertEquals("SELECT * FROM test_source", transformConfig.getString("query"));

        // Verify sink configuration
        org.apache.seatunnel.shade.com.typesafe.config.Config sinkConfig =
                config.getConfigList("sink").get(0);
        Assertions.assertEquals("Console", sinkConfig.getString("plugin_name"));
        Assertions.assertEquals(
                transformConfig.getString("plugin_output"), sinkConfig.getString("plugin_input"));
    }

    public void testLogRestApiResponse(String format) throws IOException {
        HttpURLConnection conn = null;
        try {
            java.net.URL url =
                    new java.net.URL("http://localhost:" + HTTP_PORT + "/logs?format=" + format);
            conn = (HttpURLConnection) url.openConnection();

            Assertions.assertEquals(200, conn.getResponseCode());
            Assertions.assertTrue(
                    conn.getHeaderFields()
                            .get("Content-Type")
                            .toString()
                            .contains("charset=utf-8"));
        } finally {
            if (conn != null) {
                conn.disconnect();
            }
        }
    }

    private void startJob(Long jobId, String path) {
        LogicalDag testLogicalDag = TestUtils.createTestLogicalPlan(path, jobId.toString(), jobId);

        JobImmutableInformation jobImmutableInformation =
                new JobImmutableInformation(
                        jobId,
                        "Test",
                        nodeEngine.getSerializationService(),
                        testLogicalDag,
                        Collections.emptyList(),
                        Collections.emptyList());

        Data data = nodeEngine.getSerializationService().toData(jobImmutableInformation);

        PassiveCompletableFuture<Void> voidPassiveCompletableFuture =
                server.getCoordinatorService()
                        .submitJob(jobId, data, jobImmutableInformation.isStartWithSavePoint());
        voidPassiveCompletableFuture.join();
    }
}
