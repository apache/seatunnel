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

import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.JsonNode;
import org.apache.seatunnel.shade.com.typesafe.config.Config;
import org.apache.seatunnel.shade.com.typesafe.config.ConfigException;
import org.apache.seatunnel.shade.com.typesafe.config.ConfigFactory;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.config.sql.SqlConfigBuilder;
import org.apache.seatunnel.core.starter.command.ParameterSplitter;
import org.apache.seatunnel.core.starter.utils.ConfigBuilder;
import org.apache.seatunnel.core.starter.utils.ConfigShadeUtils;
import org.apache.seatunnel.engine.server.utils.RestUtil;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import com.sun.net.httpserver.HttpServer;

import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

/** Variables on REST job submission must behave like {@code -i} on the command line. */
public class RestSubmitVariablesTest {

    // quotes keep the comma inside one value, and values may contain '=' and non-ASCII text
    private static final String VARIABLES =
            "table=\"orders, archived\",region=Zürich-東京,filter=a=b=c,rows=5,fields=[1,2]";

    private static final String HOCON =
            "env {\n"
                    + "  job.mode = \"BATCH\"\n"
                    + "  job.name = \"sync_${region}\"\n"
                    + "}\n"
                    + "source {\n"
                    + "  FakeSource {\n"
                    + "    row.num = ${rows}\n"
                    + "    table = ${table}\n"
                    + "    where = \"${filter}\"\n"
                    + "    ids = ${fields}\n"
                    + "    tags = [\"${region}\", \"fixed\"]\n"
                    + "    missing = \"keep_${not_given}\"\n"
                    + "    defaulted = \"${not_given:fallback}\"\n"
                    + "  }\n"
                    + "}\n"
                    + "sink { Console {} }\n";

    private static final String SQL =
            "/* config\n"
                    + "env {\n"
                    + "  job.mode = \"BATCH\"\n"
                    + "  job.name = \"sync_${region}\"\n"
                    + "}\n"
                    + "*/\n"
                    + "CREATE TABLE source_table WITH (\n"
                    + "  'connector'='FakeSource',\n"
                    + "  'type'='source',\n"
                    + "  'row.num'='${rows}',\n"
                    + "  'where'='${filter}',\n"
                    + "  'schema'='{ fields { id = \"int\", name = \"string\" } }'\n"
                    + ");\n"
                    + "CREATE TABLE sink_table WITH (\n"
                    + "  'connector'='Console',\n"
                    + "  'type'='sink'\n"
                    + ");\n"
                    + "INSERT INTO sink_table SELECT * FROM source_table;";

    private static final String JSON =
            "{\n"
                    + "  \"env\": {\"job.mode\": \"BATCH\", \"job.name\": \"sync_${region}\"},\n"
                    + "  \"source\": [{\"plugin_name\": \"FakeSource\", \"row.num\": \"${rows}\",\n"
                    + "              \"where\": \"${filter}\", \"tags\": [\"${region}\"]}],\n"
                    + "  \"sink\": [{\"plugin_name\": \"Console\"}]\n"
                    + "}";

    @TempDir Path tempDir;

    @AfterEach
    void clearProperty() {
        System.clearProperty("seatunnel.rest.test.secret");
    }

    @Test
    void hoconBodyResolvesLikeCli() throws Exception {
        Config cli = ConfigBuilder.of(write("job.conf", HOCON), cliVariables(VARIABLES));
        Config rest = restHocon(HOCON, VARIABLES);

        Assertions.assertEquals(cli.root().unwrapped(), rest.root().unwrapped());

        Config source = rest.getConfigList("source").get(0);
        Assertions.assertEquals("sync_Zürich-東京", jobName(rest));
        Assertions.assertEquals("orders, archived", source.getString("table"));
        Assertions.assertEquals("a=b=c", source.getString("where"));
        Assertions.assertEquals("5", option(source, "row.num"));
        Assertions.assertEquals(2, source.getList("ids").size());
        Assertions.assertEquals("Zürich-東京", source.getStringList("tags").get(0));
        Assertions.assertEquals("keep_${not_given}", source.getString("missing"));
        Assertions.assertEquals("fallback", source.getString("defaulted"));
    }

    @Test
    void sqlBodyResolvesLikeCli() throws Exception {
        Config cli = ConfigBuilder.of(write("job.sql", SQL), cliVariables(VARIABLES));
        Config rest =
                ConfigShadeUtils.decryptConfig(
                        RestUtil.applyVariables(
                                SqlConfigBuilder.of(SQL), RestUtil.parseVariables(VARIABLES)));

        Assertions.assertEquals(cli.root().unwrapped(), rest.root().unwrapped());
        Assertions.assertEquals("sync_Zürich-東京", jobName(rest));
        Assertions.assertEquals("a=b=c", rest.getConfigList("source").get(0).getString("where"));
    }

    @Test
    void jsonBodyResolvesLikeCliWithQueryOrBodyVariables() throws Exception {
        Config cli = ConfigBuilder.of(write("job.json", JSON), cliVariables(VARIABLES));

        Config fromQuery =
                ConfigShadeUtils.decryptConfig(RestUtil.buildConfig(json(JSON), VARIABLES));
        Assertions.assertEquals(cli.root().unwrapped(), fromQuery.root().unwrapped());

        String jsonWithVariables =
                JSON.replaceFirst(
                        "\\{",
                        "{\"variables\": {\"table\": \"orders, archived\","
                                + " \"region\": \"Zürich-東京\", \"filter\": \"a=b=c\","
                                + " \"rows\": 5, \"fields\": \"[1,2]\"},");
        Config fromBody =
                ConfigShadeUtils.decryptConfig(RestUtil.buildConfig(json(jsonWithVariables), null));
        Assertions.assertEquals(cli.root().unwrapped(), fromBody.root().unwrapped());
        Assertions.assertFalse(fromBody.hasPath("variables"));
    }

    @Test
    void missingUnquotedVariableFailsLikeCli() throws Exception {
        String body = HOCON.replace("${rows}", "${rows_not_given}");

        Assertions.assertThrows(
                ConfigException.NotResolved.class,
                () -> ConfigBuilder.of(write("job.conf", body), cliVariables(VARIABLES)));
        // the same failure, reported as a bad request
        IllegalArgumentException e =
                Assertions.assertThrows(
                        IllegalArgumentException.class, () -> restHocon(body, VARIABLES));
        assertNamesOnly(e, "variables", "Zürich", "orders, archived");
    }

    @Test
    void unresolvedConfigNeverEchoesOtherValues() {
        // Typesafe renders the partly substituted concatenation in its message
        String body =
                "env { job.mode = \"BATCH\" }\n"
                        + "source { FakeSource {\n"
                        + "  url = \"jdbc:x://h/\"${password}\"?a=\"${not_given}\n"
                        + "} }\n"
                        + "sink { Console {} }\n";
        IllegalArgumentException unresolved =
                Assertions.assertThrows(
                        IllegalArgumentException.class,
                        () -> restHocon(body, "password=pw-concat-xyz"));
        assertNamesOnly(unresolved, "variables", "pw-concat-xyz");

        IllegalArgumentException wrongType =
                Assertions.assertThrows(
                        IllegalArgumentException.class,
                        () ->
                                restHocon(
                                        body.replace(
                                                "\"jdbc:x://h/\"${password}\"?a=\"${not_given}",
                                                "${password} {a = 1}"),
                                        "password=pw-type-xyz"));
        assertNamesOnly(wrongType, "variables", "pw-type-xyz");

        IllegalArgumentException conflicting =
                Assertions.assertThrows(
                        IllegalArgumentException.class,
                        () -> restHocon(HOCON, "a=one-xyz,a.b=two-xyz"));
        assertNamesOnly(conflicting, "variables", "one-xyz", "two-xyz");
    }

    @Test
    void sinkTablePlaceholdersAreLeftForTheSink() {
        String body =
                HOCON.replace(
                        "sink { Console {} }",
                        "sink { Console {\n"
                                + "  table = \"${table_name:default_table}\"\n"
                                + "  path = \"/data/${database_name}/${region}\"\n"
                                + "} }");

        Config sink = restHocon(body, VARIABLES).getConfigList("sink").get(0);
        Assertions.assertEquals("${table_name:default_table}", sink.getString("table"));
        Assertions.assertEquals("/data/${database_name}/Zürich-東京", sink.getString("path"));
    }

    @Test
    void encryptConfigBodyIsNotTreatedAsASubmission() throws Exception {
        // /encrypt-config uses the one-argument overload and must keep the body as it is
        Config config = RestUtil.buildConfig(json(withBodyVariables("{\"region\": \"eu\"}")));
        Assertions.assertTrue(config.hasPath("variables"));
        Assertions.assertEquals("sync_${region}", jobName(config));
    }

    @Test
    void engineSystemPropertiesAndEnvironmentAreNeverUsed() {
        System.setProperty("seatunnel.rest.test.secret", "property-value-xyz");
        String body =
                "env { job.mode = \"BATCH\" }\n"
                        + "source { FakeSource {\n"
                        + "  a = \"${seatunnel.rest.test.secret}\"\n"
                        + "  b = \"${PATH}\"\n"
                        + "  c = ${?PATH}\n"
                        + "  d = ${region}\n"
                        + "} }\n"
                        + "sink { Console {} }\n";

        Config rest = restHocon(body, "region=eu");
        Config source = rest.getConfigList("source").get(0);
        Assertions.assertEquals("${seatunnel.rest.test.secret}", source.getString("a"));
        Assertions.assertEquals("${PATH}", source.getString("b"));
        Assertions.assertFalse(source.hasPath("c"));
        Assertions.assertEquals("eu", source.getString("d"));

        String unquoted =
                body.replace("\"${seatunnel.rest.test.secret}\"", "${seatunnel.rest.test.secret}");
        IllegalArgumentException e =
                Assertions.assertThrows(
                        IllegalArgumentException.class, () -> restHocon(unquoted, "region=eu"));
        assertNamesOnly(e, "variables", "property-value-xyz");
    }

    @Test
    void variableValuesCannotIncludeFilesOrUrls() throws Exception {
        Path secret = write("secret.conf", "leaked = \"file-content-xyz\"\n");
        IllegalArgumentException fileInclude =
                Assertions.assertThrows(
                        IllegalArgumentException.class,
                        () -> restHocon(HOCON, "fields={include file(\"" + secret + "\")}"));
        assertNamesOnly(fileInclude, "fields", secret.toString(), "file-content-xyz");

        AtomicInteger hits = new AtomicInteger();
        HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext(
                "/",
                exchange -> {
                    hits.incrementAndGet();
                    byte[] reply = "leaked = 1".getBytes(StandardCharsets.UTF_8);
                    exchange.sendResponseHeaders(200, reply.length);
                    exchange.getResponseBody().write(reply);
                    exchange.close();
                });
        server.start();
        try {
            String url = "http://127.0.0.1:" + server.getAddress().getPort() + "/x.conf";
            IllegalArgumentException urlInclude =
                    Assertions.assertThrows(
                            IllegalArgumentException.class,
                            () -> restHocon(HOCON, "fields={include url(\"" + url + "\")}"));
            assertNamesOnly(urlInclude, "fields", url);
            Assertions.assertEquals(0, hits.get());
        } finally {
            server.stop(0);
        }
    }

    @Test
    void invalidStructuredValueNamesTheKeyOnly() {
        IllegalArgumentException e =
                Assertions.assertThrows(
                        IllegalArgumentException.class,
                        () -> restHocon(HOCON, "fields={ secret-xyz = = }"));
        assertNamesOnly(e, "fields", "secret-xyz");
    }

    @Test
    void malformedVariablesAreRejectedWithoutTheirValues() {
        IllegalArgumentException noEquals =
                Assertions.assertThrows(
                        IllegalArgumentException.class,
                        () -> RestUtil.parseVariables("region=eu,password-xyz"));
        assertNamesOnly(noEquals, "variables", "password-xyz");

        IllegalArgumentException emptyKey =
                Assertions.assertThrows(
                        IllegalArgumentException.class,
                        () -> RestUtil.parseVariables(" =value-xyz"));
        assertNamesOnly(emptyKey, "variables", "value-xyz");

        IllegalArgumentException duplicate =
                Assertions.assertThrows(
                        IllegalArgumentException.class,
                        () -> RestUtil.parseVariables("token=first-xyz, token=second-xyz"));
        assertNamesOnly(duplicate, "token", "first-xyz", "second-xyz");

        IllegalArgumentException reserved =
                Assertions.assertThrows(
                        IllegalArgumentException.class,
                        () -> RestUtil.parseVariables("table_name=value-xyz"));
        assertNamesOnly(reserved, "table_name", "value-xyz");
    }

    @Test
    void jsonVariablesMustBeAnObjectOfScalars() {
        IllegalArgumentException notObject =
                Assertions.assertThrows(
                        IllegalArgumentException.class,
                        () ->
                                RestUtil.buildConfig(
                                        json(withBodyVariables("\"a=value-xyz\"")), null));
        assertNamesOnly(notObject, "variables", "value-xyz");

        IllegalArgumentException nested =
                Assertions.assertThrows(
                        IllegalArgumentException.class,
                        () ->
                                RestUtil.buildConfig(
                                        json(
                                                withBodyVariables(
                                                        "{\"region\": {\"x\": \"value-xyz\"}}")),
                                        null));
        assertNamesOnly(nested, "region", "value-xyz");

        IllegalArgumentException duplicate =
                Assertions.assertThrows(
                        IllegalArgumentException.class,
                        () ->
                                RestUtil.buildConfig(
                                        json(withBodyVariables("{\"region\": \"body-xyz\"}")),
                                        "region=query-xyz"));
        assertNamesOnly(duplicate, "region", "body-xyz", "query-xyz");
    }

    @Test
    void configIsUntouchedWithoutVariables() {
        Config parsed = ConfigFactory.parseString(HOCON);
        Assertions.assertSame(
                parsed, RestUtil.applyVariables(parsed, RestUtil.parseVariables(null)));
        Assertions.assertSame(parsed, RestUtil.applyVariables(parsed, RestUtil.parseVariables("")));
    }

    @Test
    void submitJobsReadsVariablesPerJob() throws Exception {
        String body =
                "["
                        + JSON.replaceFirst(
                                "\\{",
                                "{\"params\": {\"jobId\": \"1\", \"variables\": \"region=eu,rows=1\"},")
                        + ","
                        + JSON.replaceFirst(
                                "\\{", "{\"variables\": {\"region\": \"us\", \"rows\": \"2\"},")
                        + "]";

        List<scala.Tuple2<Map<String, String>, Config>> jobs = RestUtil.buildConfigList(json(body));

        Assertions.assertEquals(2, jobs.size());
        Assertions.assertEquals("1", jobs.get(0)._1.get("jobId"));
        Assertions.assertFalse(jobs.get(0)._1.containsKey(RestConstant.VARIABLES));
        Assertions.assertEquals("sync_eu", jobName(jobs.get(0)._2));
        Assertions.assertEquals("sync_us", jobName(jobs.get(1)._2));
        Assertions.assertTrue(jobs.get(1)._1.isEmpty());
        Assertions.assertFalse(jobs.get(1)._2.hasPath("variables"));
    }

    private static Config restHocon(String body, String variables) {
        // the same steps JobInfoService takes for a HOCON body
        return ConfigShadeUtils.decryptConfig(
                RestUtil.applyVariables(
                        ConfigFactory.parseString(body), RestUtil.parseVariables(variables)));
    }

    // job options such as job.name are read the way the engine reads them
    private static String jobName(Config config) {
        return option(config.getConfig("env"), "job.name");
    }

    private static String option(Config config, String key) {
        return ReadonlyConfig.fromConfig(config).toMap().get(key);
    }

    private static List<String> cliVariables(String variables) {
        return new ParameterSplitter().split(variables);
    }

    private static String withBodyVariables(String variables) {
        return JSON.replaceFirst("\\{", "{\"variables\": " + variables + ",");
    }

    private static JsonNode json(String body) throws Exception {
        return RestUtil.convertByteToJsonNode(body.getBytes(StandardCharsets.UTF_8));
    }

    private Path write(String name, String content) throws Exception {
        Path file = tempDir.resolve(name);
        Files.write(file, content.getBytes(StandardCharsets.UTF_8));
        return file;
    }

    private static void assertNamesOnly(Exception e, String expected, String... hidden) {
        String message = e.getMessage();
        Assertions.assertTrue(message.contains(expected), message);
        for (String value : hidden) {
            Assertions.assertFalse(message.contains(value), message);
        }
        Assertions.assertNull(e.getCause(), "the cause may carry the value");
    }
}
