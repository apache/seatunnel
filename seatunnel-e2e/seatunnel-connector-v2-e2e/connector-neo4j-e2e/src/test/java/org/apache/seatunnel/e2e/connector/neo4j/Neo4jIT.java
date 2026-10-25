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

package org.apache.seatunnel.e2e.connector.neo4j;

import org.apache.seatunnel.shade.com.typesafe.config.ConfigFactory;
import org.apache.seatunnel.shade.com.typesafe.config.ConfigRenderOptions;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.table.factory.FactoryUtil;
import org.apache.seatunnel.api.table.factory.SupportSourceDryRunValidation;
import org.apache.seatunnel.api.table.factory.TableSourceFactory;
import org.apache.seatunnel.api.table.factory.TableSourceFactoryContext;
import org.apache.seatunnel.core.starter.seatunnel.args.ClientCommandArgs;
import org.apache.seatunnel.core.starter.seatunnel.command.SeaTunnelConfValidateCommand;
import org.apache.seatunnel.core.starter.utils.CommandLineUtils;
import org.apache.seatunnel.e2e.common.TestResource;
import org.apache.seatunnel.e2e.common.TestSuiteBase;
import org.apache.seatunnel.e2e.common.container.TestContainer;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.io.TempDir;
import org.neo4j.driver.AuthTokens;
import org.neo4j.driver.Driver;
import org.neo4j.driver.GraphDatabase;
import org.neo4j.driver.Record;
import org.neo4j.driver.Result;
import org.neo4j.driver.Session;
import org.neo4j.driver.SessionConfig;
import org.neo4j.driver.Value;
import org.neo4j.driver.types.Node;
import org.testcontainers.containers.Container;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.lifecycle.Startables;
import org.testcontainers.shaded.org.awaitility.Awaitility;
import org.testcontainers.utility.DockerImageName;
import org.testcontainers.utility.DockerLoggerFactory;

import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.driver.Values.parameters;

@Slf4j
public class Neo4jIT extends TestSuiteBase implements TestResource {

    private static final int FAKE_ROW_NUM = 1000;

    private static final String CONTAINER_IMAGE = "neo4j:5.6.0";
    private static final String CONTAINER_HOST = "neo4j-host";
    private static final int HTTP_PORT = 7474;
    private static final int BOLT_PORT = 7687;
    private static final String CONTAINER_NEO4J_USERNAME = "neo4j";
    private static final String CONTAINER_NEO4J_PASSWORD = "Test@12343";
    private GenericContainer<?> container;
    private Driver neo4jDriver;
    private Session neo4jSession;

    @BeforeAll
    @Override
    public void startUp() throws Exception {
        DockerImageName imageName = DockerImageName.parse(CONTAINER_IMAGE);
        container =
                new GenericContainer<>(imageName)
                        .withNetwork(NETWORK)
                        .withNetworkAliases(CONTAINER_HOST)
                        .withExposedPorts(HTTP_PORT, BOLT_PORT)
                        .withEnv(
                                "NEO4J_AUTH",
                                CONTAINER_NEO4J_USERNAME + "/" + CONTAINER_NEO4J_PASSWORD)
                        .withLogConsumer(
                                new Slf4jLogConsumer(
                                        DockerLoggerFactory.getLogger(CONTAINER_IMAGE)));
        Startables.deepStart(Stream.of(container)).join();
        log.info("container started");
        Awaitility.given()
                .ignoreExceptions()
                .await()
                .atMost(30, TimeUnit.SECONDS)
                .untilAsserted(this::initConnection);
    }

    private void initConnection() {
        neo4jDriver =
                GraphDatabase.driver(
                        URI.create(
                                String.format(
                                        "bolt://%s:%s",
                                        container.getHost(), container.getMappedPort(BOLT_PORT))),
                        AuthTokens.basic(CONTAINER_NEO4J_USERNAME, CONTAINER_NEO4J_PASSWORD));
        neo4jSession = neo4jDriver.session(SessionConfig.forDatabase("neo4j"));
    }

    @TestTemplate
    public void test(TestContainer container) throws IOException, InterruptedException {
        // clean test data before test
        final Result checkExists = neo4jSession.run("MATCH (tt:TestTest) RETURN tt");
        if (checkExists.hasNext()) {
            neo4jSession.run("MATCH (tt:TestTest) delete tt");
        }

        final Result checkExistsT = neo4jSession.run("MATCH (t:Test) RETURN t");
        if (checkExistsT.hasNext()) {
            neo4jSession.run("MATCH (t:Test) delete t");
        }

        // given
        neo4jSession.run(
                "CREATE (t:Test {string:'foo', boolean:true, long:2147483648, double:1.7976931348623157E308, "
                        + "byteArray:$byteArray, date:date('2022-10-07'), localTime:localtime('20:04:00'), localDateTime:localdatetime('2022-10-07T20:04:00'), "
                        + "list:[0, 1], int:2147483647, float:$float})",
                parameters("byteArray", new byte[] {(byte) 1}, "float", Float.MAX_VALUE));
        // when
        Container.ExecResult execResult = container.executeJob("/neo4j/neo4j_to_neo4j.conf");
        // then
        Assertions.assertEquals(0, execResult.getExitCode());

        final Result result = neo4jSession.run("MATCH (tt:TestTest) RETURN tt");
        final Node tt = result.single().get("tt").asNode();

        assertEquals("foo", tt.get("string").asString());
        assertTrue(tt.get("boolean").asBoolean());
        assertEquals(2147483648L, tt.get("long").asLong());
        assertEquals(Double.MAX_VALUE, tt.get("double").asDouble());
        assertArrayEquals(new byte[] {(byte) 1}, tt.get("byteArray").asByteArray());
        assertEquals(LocalDate.parse("2022-10-07"), tt.get("date").asLocalDate());
        assertEquals(
                LocalDateTime.parse("2022-10-07T20:04:00"),
                tt.get("localDateTime").asLocalDateTime());
        final ArrayList<Integer> expectedList = new ArrayList<>();
        expectedList.add(0);
        expectedList.add(1);
        assertTrue(tt.get("list").asList(Value::asInt).containsAll(expectedList));
        assertEquals(2147483647, tt.get("int").asInt());
        assertEquals(2147483647, tt.get("mapValue").asInt());
        assertEquals(Float.MAX_VALUE, tt.get("float").asFloat());
    }

    @TestTemplate
    public void testBatchWrite(TestContainer container) throws IOException, InterruptedException {
        // clean test data before test
        final Result checkExists = neo4jSession.run("MATCH (n:BatchLabel) RETURN n limit 1");
        if (checkExists.hasNext()) {
            neo4jSession.run("MATCH (n:BatchLabel) delete n");
        }

        // unwind $batch as row create(n:BatchLabel) set n.name = row.name,n.age = row.age
        Container.ExecResult execResult =
                container.executeJob("/neo4j/fake_to_neo4j_batch_write.conf");
        // then
        Assertions.assertEquals(0, execResult.getExitCode());
        final Result result = neo4jSession.run("MATCH (n:BatchLabel) RETURN n");
        // nodes
        assertTrue(result.hasNext());
        int cnt = 0;
        // verify the attributes of the node
        while (result.hasNext()) {
            // don`t remove import org.neo4j.driver.Record;This can cause code not to compile in
            // java14+
            Record r = result.next();
            String name = r.get("n").get("name").asString();
            assertNotNull(name);
            Object age = r.get("n").get("age").asObject();
            assertNotNull(age);
            cnt++;
        }
        assertEquals(FAKE_ROW_NUM, cnt);
    }

    @TestTemplate
    public void testMultiTableSource(TestContainer container)
            throws IOException, InterruptedException {
        neo4jSession.run("MATCH (n) WHERE n:MultiPerson OR n:MultiCompany DELETE n");
        neo4jSession.run("CREATE (:MultiPerson {name:'Alice'})");
        neo4jSession.run("CREATE (:MultiCompany {name:'Acme'})");

        Container.ExecResult execResult =
                container.executeJob("/neo4j/neo4j_multi_table_source.conf");

        Assertions.assertEquals(0, execResult.getExitCode());
    }

    @Test
    public void testDryRunDoesNotExecuteConfiguredCypher() throws Exception {
        neo4jSession.run("MATCH (n:DryRunSentinel) DELETE n").consume();
        neo4jSession.run("CREATE (:DryRunSentinel {name:'unchanged'})").consume();
        Map<String, Object> options = dryRunOptions();
        options.put("query", "MATCH (n:DryRunSentinel) DELETE n");
        validateDryRun(options);
        Assertions.assertEquals(
                "unchanged",
                neo4jSession
                        .run("MATCH (n:DryRunSentinel) RETURN n.name AS name")
                        .single()
                        .get("name")
                        .asString());
        options.put("query", "THIS IS DELIBERATELY INVALID CYPHER");
        validateDryRun(options);
    }

    @Test
    public void testDryRunMultiTableSchemaWithoutQueryExecution() throws Exception {
        Map<String, Object> options = dryRunOptions();
        options.remove("query");
        options.remove("schema");
        Map<String, Object> first = new HashMap<>();
        first.put("query", "CREATE (:DryRunMustNotExist)");
        Map<String, Object> schema = new HashMap<>();
        schema.put("table", "first");
        schema.put("fields", Collections.singletonMap("name", "string"));
        first.put("schema", schema);
        Map<String, Object> second = new HashMap<>();
        second.put("query", "INVALID CYPHER");
        Map<String, Object> secondSchema = new HashMap<>(schema);
        secondSchema.put("table", "second");
        second.put("schema", secondSchema);
        options.put("tables_configs", Arrays.asList(first, second));
        validateDryRun(options);
        Assertions.assertEquals(
                0,
                neo4jSession
                        .run("MATCH (n:DryRunMustNotExist) RETURN count(n) AS count")
                        .single()
                        .get("count")
                        .asInt());
    }

    @Test
    public void testDryRunRejectsWrongPasswordWithoutEchoingIt() {
        Map<String, Object> options = dryRunOptions();
        options.put("password", "dry-run-private-password");
        Exception error = Assertions.assertThrows(IOException.class, () -> validateDryRun(options));
        Assertions.assertFalse(error.toString().contains("dry-run-private-password"));
        Assertions.assertNull(error.getCause());
    }

    @Test
    public void testDryRunDoesNotClaimDatabaseValidation() throws Exception {
        Map<String, Object> options = dryRunOptions();
        options.put("database", "missing-preflight-database");
        // Driver connectivity does not select a database or prove query permissions.
        validateDryRun(options);
    }

    private Map<String, Object> dryRunOptions() {
        Map<String, Object> options = new HashMap<>();
        options.put(
                "uri",
                String.format(
                        "bolt://%s:%s", container.getHost(), container.getMappedPort(BOLT_PORT)));
        options.put("database", "neo4j");
        options.put("username", CONTAINER_NEO4J_USERNAME);
        options.put("password", CONTAINER_NEO4J_PASSWORD);
        options.put("query", "RETURN 'unused' AS name");
        options.put(
                "schema",
                Collections.singletonMap("fields", Collections.singletonMap("name", "string")));
        return options;
    }

    @Test
    public void testDryRunCommand(@TempDir Path directory) throws Exception {
        Map<String, Object> options = dryRunOptions();
        options.put("query", "CREATE (:DryRunCommandMustNotExist)");
        checkDryRunCommand(options, directory);
        Assertions.assertEquals(
                0,
                neo4jSession
                        .run("MATCH (n:DryRunCommandMustNotExist) RETURN count(n) AS count")
                        .single()
                        .get("count")
                        .asInt());
    }

    private void checkDryRunCommand(Map<String, Object> options, Path directory) throws Exception {
        options.put("plugin_name", "Neo4j");
        options.put("plugin_output", "preflight");
        Map<String, Object> sink = new HashMap<>();
        sink.put("plugin_name", "Console");
        sink.put("plugin_input", "preflight");
        Map<String, Object> job = new HashMap<>();
        job.put("source", Collections.singletonList(options));
        job.put("sink", Collections.singletonList(sink));
        Path file = directory.resolve("dry-run.json");
        Files.write(
                file,
                ConfigFactory.parseMap(job)
                        .root()
                        .render(ConfigRenderOptions.concise())
                        .getBytes(StandardCharsets.UTF_8));
        ClientCommandArgs args =
                CommandLineUtils.parse(
                        new String[] {"-c", file.toString(), "--dry-run", "connect"},
                        new ClientCommandArgs(),
                        "seatunnel.sh",
                        true);
        new SeaTunnelConfValidateCommand(args).execute();
    }

    private void validateDryRun(Map<String, Object> options) throws Exception {
        ClassLoader loader = getClass().getClassLoader();
        TableSourceFactory factory =
                FactoryUtil.discoverFactory(loader, TableSourceFactory.class, "Neo4j");
        Assertions.assertTrue(factory instanceof SupportSourceDryRunValidation);
        SupportSourceDryRunValidation validator = (SupportSourceDryRunValidation) factory;
        TableSourceFactoryContext context =
                new TableSourceFactoryContext(ReadonlyConfig.fromMap(options), loader);
        validator.validateConnectionForDryRun(context, validator.inferSchemaForDryRun(context));
    }

    @AfterAll
    @Override
    public void tearDown() {
        if (neo4jSession != null) {
            neo4jSession.close();
        }
        if (neo4jDriver != null) {
            neo4jDriver.close();
        }
        if (container != null) {
            container.close();
        }
    }
}
