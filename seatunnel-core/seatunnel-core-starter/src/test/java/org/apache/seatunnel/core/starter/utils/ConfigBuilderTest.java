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

package org.apache.seatunnel.core.starter.utils;

import org.apache.seatunnel.shade.com.typesafe.config.Config;
import org.apache.seatunnel.shade.com.typesafe.config.ConfigFactory;

import org.apache.seatunnel.core.starter.exception.ConfigCheckException;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class ConfigBuilderTest {

    /**
     * Every starter loads a job config through {@code ConfigBuilder.of(path, variables)}, which
     * rebuilds the config after substituting user variables. That rebuild must keep option keys
     * literal: a regex key such as {@code ^t_nova_.*$}, used by {@code
     * multi_table_config.primary_keys}, is not a valid HOCON path, so rebuilding the map with
     * {@code ConfigFactory.parseMap} rejected the whole config with {@code ConfigException.BadPath}
     * (see #12491).
     */
    @Test
    public void testOfPreservesLiteralRegexOptionKeys(@TempDir Path tempDir) throws IOException {
        Path conf = writeMultiTableConfigConf(tempDir);

        // The starters pass null when no -i variable is given.
        assertLoadedConfig(conf, ConfigBuilder.of(conf, null), "jdbc:mysql://${db_host}/sink");

        // A -i variable is substituted, and must not disturb the shape of the config.
        assertLoadedConfig(
                conf,
                ConfigBuilder.of(conf, Arrays.asList("db_host=localhost:3306")),
                "jdbc:mysql://localhost:3306/sink");
    }

    private static void assertLoadedConfig(Path conf, Config config, String expectedUrl) {
        Assertions.assertEquals(expectedUrl, config.getConfigList("sink").get(0).getString("url"));

        // Ordinary dotted keys keep the shape the file parser produced, so nothing changes for the
        // options that already worked.
        Assertions.assertEquals(
                ConfigFactory.parseFile(conf.toFile()).getConfig("env").root().unwrapped(),
                config.getConfig("env").root().unwrapped());

        // The regex key survives as one literal key, instead of being rejected as an invalid path
        // expression.
        Map<?, ?> primaryKeys =
                config.getConfigList("sink")
                        .get(0)
                        .getConfig("multi_table_config")
                        .getConfig("primary_keys")
                        .root()
                        .unwrapped();
        Assertions.assertEquals(
                Arrays.asList("${primary_key}", "DATA_SOURCE"), primaryKeys.get("^t_nova_.*$"));
    }

    private static Path writeMultiTableConfigConf(Path tempDir) throws IOException {
        Path conf = tempDir.resolve("multi_table_config.conf");
        Files.write(
                conf,
                Arrays.asList(
                        "env {",
                        "  job.mode = \"BATCH\"",
                        "}",
                        "source {",
                        "  FakeSource {",
                        "    plugin_output = \"fake\"",
                        "  }",
                        "}",
                        "sink {",
                        "  Jdbc {",
                        "    url = \"jdbc:mysql://${db_host}/sink\"",
                        "    multi_table_config {",
                        "      primary_keys {",
                        "        \"^t_nova_.*$\" = [\"${primary_key}\", \"DATA_SOURCE\"]",
                        "      }",
                        "    }",
                        "  }",
                        "}"));
        return conf;
    }

    /**
     * The REST submit-job API parses its JSON body into a map and passes it to {@code
     * ConfigBuilder#of(Map)}. That entry point must keep option keys literal as well, otherwise a
     * JSON body carrying {@code multi_table_config.primary_keys} regex keys is rejected with {@code
     * ConfigException.BadPath} before the connector is created.
     */
    @Test
    public void testOfMapPreservesLiteralRegexOptionKeys() {
        Map<String, Object> primaryKeys = new LinkedHashMap<>();
        primaryKeys.put("^t_nova_.*$", Arrays.asList("${primary_key}", "DATA_SOURCE"));
        Map<String, Object> multiTableConfig = new LinkedHashMap<>();
        multiTableConfig.put("primary_keys", primaryKeys);
        Map<String, Object> jdbcOptions = new LinkedHashMap<>();
        jdbcOptions.put("plugin_name", "Jdbc");
        jdbcOptions.put("url", "jdbc:mysql://localhost:3306/sink");
        jdbcOptions.put("multi_table_config", multiTableConfig);
        Map<String, Object> fakeSource = new LinkedHashMap<>();
        fakeSource.put("plugin_name", "FakeSource");
        fakeSource.put("plugin_output", "fake");
        Map<String, Object> env = new LinkedHashMap<>();
        env.put("job.mode", "BATCH");
        Map<String, Object> config = new LinkedHashMap<>();
        config.put("env", env);
        config.put("source", Collections.singletonList(fakeSource));
        config.put("sink", Collections.singletonList(jdbcOptions));

        Config parsed = ConfigBuilder.of(config);
        // The REST path continues with the shade rebuild in JobInfoService.
        Config loaded = ConfigShadeUtils.decryptConfig(parsed);

        Map<?, ?> resolvedPrimaryKeys =
                loaded.getConfigList("sink")
                        .get(0)
                        .getConfig("multi_table_config")
                        .getConfig("primary_keys")
                        .root()
                        .unwrapped();
        Assertions.assertEquals(
                Arrays.asList("${primary_key}", "DATA_SOURCE"),
                resolvedPrimaryKeys.get("^t_nova_.*$"));
        // Dotted option keys keep the literal shape they had before this change.
        Assertions.assertEquals(
                Collections.singletonMap("job.mode", "BATCH"),
                loaded.getConfig("env").root().unwrapped());
    }

    @Test
    public void testConfigDesensitizationSort() {
        Map<String, Object> config = new LinkedHashMap<>();
        config.put("a", "1");
        config.put("b", "1");
        config.put("c", "1");
        config.put("d", "1");
        config.put("e", "1");
        config.put("f", "1");

        Map<String, Object> desensitizationConfig =
                ConfigBuilder.configDesensitization(
                        config, ConfigShadeUtils.getSensitiveOptions(null));
        List<String> keys = new ArrayList<>(desensitizationConfig.keySet());
        Assertions.assertIterableEquals(Arrays.asList("a", "b", "c", "d", "e", "f"), keys);
    }

    @Test
    public void testConfigDesensitizationMasksJdbcUrls() {
        Map<String, Object> source = new LinkedHashMap<>();
        source.put(
                "url",
                "jdbc:mysql://alice:secret-password@db.example.com:3306/orders?token=secret-token");
        source.put("metadata_url", "https://catalog.example.com/tables");

        Map<String, Object> config = new LinkedHashMap<>();
        config.put("source", Arrays.asList(source));

        Map<String, Object> desensitized =
                ConfigBuilder.configDesensitization(
                        config, ConfigShadeUtils.getLogDesensitizationOptions(null));
        List<?> sources = (List<?>) desensitized.get("source");
        Map<?, ?> desensitizedSource = (Map<?, ?>) sources.get(0);

        Assertions.assertEquals("******", desensitizedSource.get("url"));
        Assertions.assertEquals(
                "https://catalog.example.com/tables", desensitizedSource.get("metadata_url"));
    }

    @Test
    public void testConfigDesensitizationMasksKafkaJaasConfig() {
        Map<String, Object> jaasConfig = new LinkedHashMap<>();
        jaasConfig.put(
                "config",
                "org.apache.kafka.common.security.scram.ScramLoginModule required "
                        + "username=\"alice\" password=\"secret\";");

        Map<String, Object> saslConfig = new LinkedHashMap<>();
        saslConfig.put("jaas", jaasConfig);

        Map<String, Object> kafkaConfig = new LinkedHashMap<>();
        kafkaConfig.put("bootstrap.servers", "localhost:9092");
        kafkaConfig.put("sasl", saslConfig);

        Map<String, Object> source = new LinkedHashMap<>();
        source.put("kafka.config", kafkaConfig);

        Map<String, Object> config = new LinkedHashMap<>();
        config.put("source", Arrays.asList(source));

        Map<String, Object> desensitized =
                ConfigBuilder.configDesensitization(
                        config, ConfigShadeUtils.getLogDesensitizationOptions(null));
        List<?> sources = (List<?>) desensitized.get("source");
        Map<?, ?> desensitizedSource = (Map<?, ?>) sources.get(0);
        Map<?, ?> desensitizedKafkaConfig = (Map<?, ?>) desensitizedSource.get("kafka.config");
        Map<?, ?> desensitizedSaslConfig = (Map<?, ?>) desensitizedKafkaConfig.get("sasl");
        Map<?, ?> desensitizedJaasConfig = (Map<?, ?>) desensitizedSaslConfig.get("jaas");

        Assertions.assertEquals("******", desensitizedJaasConfig.get("config"));
        Assertions.assertEquals("localhost:9092", desensitizedKafkaConfig.get("bootstrap.servers"));
    }

    @Test
    public void testConfigDesensitizationMasksConnectionStrings() {
        Map<String, Object> source = new LinkedHashMap<>();
        source.put(
                "connection_string",
                "Endpoint=sb://example/;SharedAccessKeyName=name;SharedAccessKey=secret;");
        source.put("event_hub_name", "events");

        Map<String, Object> config = new LinkedHashMap<>();
        config.put("source", Arrays.asList(source));

        Map<String, Object> desensitized =
                ConfigBuilder.configDesensitization(
                        config, ConfigShadeUtils.getLogDesensitizationOptions(null));
        List<?> sources = (List<?>) desensitized.get("source");
        Map<?, ?> desensitizedSource = (Map<?, ?>) sources.get(0);

        Assertions.assertEquals("******", desensitizedSource.get("connection_string"));
        Assertions.assertEquals("events", desensitizedSource.get("event_hub_name"));
        Assertions.assertFalse(
                ConfigShadeUtils.getSensitiveOptions(null).contains("connection_string"));
    }

    @Test
    public void testConfigDesensitizationMasksSnmpCommunity() {
        Map<String, Object> sink = new LinkedHashMap<>();
        sink.put("host", "127.0.0.1");
        sink.put("community", "private-community");

        Map<String, Object> config = new LinkedHashMap<>();
        config.put("sink", Arrays.asList(sink));

        Map<String, Object> desensitized =
                ConfigBuilder.configDesensitization(
                        config, ConfigShadeUtils.getLogDesensitizationOptions(null));
        List<?> sinks = (List<?>) desensitized.get("sink");
        Map<?, ?> desensitizedSink = (Map<?, ?>) sinks.get(0);

        Assertions.assertEquals("******", desensitizedSink.get("community"));
        Assertions.assertEquals("127.0.0.1", desensitizedSink.get("host"));
    }

    @Test
    public void testConfigDesensitizationMasksSalesforceSecretsWithoutAddingEncryptionKeys() {
        Map<String, Object> connector = new LinkedHashMap<>();
        connector.put("client_secret", "private-secret");
        connector.put("security_token", "private-token");
        connector.put("object_name", "Account");
        Map<String, Object> config = new LinkedHashMap<>();
        config.put("source", Arrays.asList(connector));
        config.put("sink", Arrays.asList(connector));
        Map<String, Object> masked =
                ConfigBuilder.configDesensitization(
                        config, ConfigShadeUtils.getLogDesensitizationOptions(null));
        for (String role : Arrays.asList("source", "sink")) {
            Map<?, ?> entry = (Map<?, ?>) ((List<?>) masked.get(role)).get(0);
            Assertions.assertEquals("******", entry.get("client_secret"));
            Assertions.assertEquals("******", entry.get("security_token"));
            Assertions.assertEquals("Account", entry.get("object_name"));
        }
        Assertions.assertFalse(
                ConfigShadeUtils.getSensitiveOptions(null).contains("client_secret"));
        Assertions.assertFalse(
                ConfigShadeUtils.getSensitiveOptions(null).contains("security_token"));
    }

    @Test
    public void testConfigDesensitizationMasksS3CredentialOptions() {
        Map<String, Object> accessKeyConfig = new LinkedHashMap<>();
        accessKeyConfig.put("key", "access-key");

        Map<String, Object> s3aConfig = new LinkedHashMap<>();
        s3aConfig.put("endpoint", "http://localhost:9000");
        s3aConfig.put("path.style.access", "true");
        s3aConfig.put("aws.credentials.provider", "SimpleAWSCredentialsProvider");
        s3aConfig.put("access", accessKeyConfig);

        Map<String, Object> fsConfig = new LinkedHashMap<>();
        fsConfig.put("s3a", s3aConfig);

        Map<String, Object> checkpointConfig = new LinkedHashMap<>();
        checkpointConfig.put("fs", fsConfig);
        checkpointConfig.put("fs.s3a.access-key", "access-key");
        checkpointConfig.put("fs.s3a.secret.key", "secret-key");

        Map<String, Object> config = new LinkedHashMap<>();
        config.put("checkpoint", checkpointConfig);

        Map<String, Object> desensitized =
                ConfigBuilder.configDesensitization(
                        config, ConfigShadeUtils.getSensitiveOptions(null));
        Map<?, ?> desensitizedCheckpoint = (Map<?, ?>) desensitized.get("checkpoint");
        Map<?, ?> desensitizedFsConfig = (Map<?, ?>) desensitizedCheckpoint.get("fs");
        Map<?, ?> desensitizedS3aConfig = (Map<?, ?>) desensitizedFsConfig.get("s3a");
        Map<?, ?> desensitizedAccessConfig = (Map<?, ?>) desensitizedS3aConfig.get("access");

        Assertions.assertEquals("******", desensitizedAccessConfig.get("key"));
        Assertions.assertEquals("******", desensitizedCheckpoint.get("fs.s3a.access-key"));
        Assertions.assertEquals("******", desensitizedCheckpoint.get("fs.s3a.secret.key"));
        Assertions.assertEquals("http://localhost:9000", desensitizedS3aConfig.get("endpoint"));
        Assertions.assertEquals("true", desensitizedS3aConfig.get("path.style.access"));
        Assertions.assertEquals(
                "SimpleAWSCredentialsProvider",
                desensitizedS3aConfig.get("aws.credentials.provider"));
    }

    @Test
    public void testConfigDesensitizationKeepsSchemaFieldsReadable() {
        Map<String, Object> fields = new LinkedHashMap<>();
        fields.put("access_token", "string");
        fields.put("user-password", "string");

        Map<String, Object> schema = new LinkedHashMap<>();
        schema.put("fields", fields);

        Map<String, Object> source = new LinkedHashMap<>();
        source.put("schema", schema);

        Map<String, Object> config = new LinkedHashMap<>();
        config.put("source", Arrays.asList(source));

        Map<String, Object> desensitized =
                ConfigBuilder.configDesensitization(
                        config, ConfigShadeUtils.getLogDesensitizationOptions(null));
        List<?> sources = (List<?>) desensitized.get("source");
        Map<?, ?> desensitizedSource = (Map<?, ?>) sources.get(0);
        Map<?, ?> desensitizedSchema = (Map<?, ?>) desensitizedSource.get("schema");
        Map<?, ?> desensitizedFields = (Map<?, ?>) desensitizedSchema.get("fields");

        Assertions.assertEquals("string", desensitizedFields.get("access_token"));
        Assertions.assertEquals("string", desensitizedFields.get("user-password"));
    }

    @Test
    void testDuplicateKeyThrowsException() {
        List<String> variables = Arrays.asList("jobName=value1", "jobName=value2");

        Assertions.assertThrows(
                ConfigCheckException.class, () -> ConfigBuilder.extractUserVariables(variables));
    }

    @Test
    void testUserKeyValueTrimmed() {
        List<String> variables = Arrays.asList("k1= , k2=v2".split(","));
        Map<String, String> userConfigMap = ConfigBuilder.extractUserVariables(variables);

        Assertions.assertTrue(userConfigMap.get("k1").isEmpty());
        Assertions.assertTrue(userConfigMap.containsKey("k2"));
        Assertions.assertFalse(userConfigMap.containsKey(" k2"));
    }

    @Test
    void testUserEmptyKeyThrowsConfigCheckException() {
        List<String> variables = Arrays.asList(" =v1 , k2=v2".split(","));

        Assertions.assertThrows(
                ConfigCheckException.class, () -> ConfigBuilder.extractUserVariables(variables));
    }
}
