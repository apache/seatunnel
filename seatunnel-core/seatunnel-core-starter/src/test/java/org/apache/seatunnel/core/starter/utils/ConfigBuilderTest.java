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
import org.apache.seatunnel.shade.com.typesafe.config.ConfigUtil;

import org.apache.seatunnel.api.configuration.ConfigAdapter;
import org.apache.seatunnel.core.starter.exception.ConfigCheckException;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class ConfigBuilderTest {

    @Test
    public void testInvalidPathKeysSurviveConfigShadeRoundTrip() {
        Map<String, Object> fields = new LinkedHashMap<>();
        fields.put("^t_nova_.*$", "string");
        fields.put("a:b", "long");
        fields.put("a\"b\\c", "string");
        fields.put("${FOO}", "boolean");
        fields.put("", "int");
        Map<String, Object> schema = new LinkedHashMap<>();
        schema.put("fields", fields);
        Map<String, Object> source = new LinkedHashMap<>();
        source.put("schema", schema);
        Map<String, Object> configMap = new LinkedHashMap<>();
        configMap.put("source", Arrays.asList(source));
        configMap.put("sink", Arrays.asList(new LinkedHashMap<>()));

        Config config = ConfigShadeUtils.decryptConfig(ConfigBuilder.of(configMap));
        Map<?, ?> actualFields =
                config.getConfigList("source")
                        .get(0)
                        .getConfig("schema")
                        .getConfig("fields")
                        .root()
                        .unwrapped();
        Assertions.assertEquals(fields, actualFields);
    }

    @Test
    public void testWhitespacePaddedKeysTrimmedUnlessGuardQuotesWholeKey() {
        Map<String, Object> fields = new LinkedHashMap<>();
        // Parsable keys keep path semantics: surrounding unquoted whitespace is trimmed.
        fields.put("  padded  ", "trimmed");
        fields.put("  job.mode  ", "dotted");
        // Unparsable keys: the guard quotes each whole key, so padding stays literal.
        fields.put("  ^t_nova_.*$  ", "regex");
        fields.put("  ${FOO}  ", "literal");
        Map<String, Object> schema = new LinkedHashMap<>();
        schema.put("fields", fields);
        Map<String, Object> source = new LinkedHashMap<>();
        source.put("schema", schema);
        Map<String, Object> configMap = new LinkedHashMap<>();
        configMap.put("source", Arrays.asList(source));
        configMap.put("sink", Arrays.asList(new LinkedHashMap<>()));

        assertWhitespacePaddedKeys(ConfigBuilder.of(configMap));
        assertWhitespacePaddedKeys(ConfigShadeUtils.decryptConfig(ConfigBuilder.of(configMap)));
    }

    @Test
    public void testQuotedLiteralKeysSurviveConfigFileLoad(@TempDir Path tempDir)
            throws IOException {
        Config config = ConfigBuilder.of(writeLiteralKeyConfigFile(tempDir, false));

        Assertions.assertEquals("BATCH", config.getConfig("env").getString("job.mode"));
        assertLiteralSchemaFields(config);
    }

    @Test
    public void testQuotedLiteralKeysSurviveConfigFileLoadWithUserVariables(@TempDir Path tempDir)
            throws IOException {
        Config config =
                ConfigBuilder.of(
                        writeLiteralKeyConfigFile(tempDir, true),
                        Arrays.asList("jobName=quoted_key_job"));

        Assertions.assertEquals("quoted_key_job", config.getConfig("env").getString("job.name"));
        assertLiteralSchemaFields(config);
    }

    /** The distinct fallback value exposes an unintended fallback after a valid adapter result. */
    @Test
    public void testConfigAdapterPathKeepsLiteralKeysAndValues(@TempDir Path tempDir)
            throws IOException {
        Path configFile = tempDir.resolve("fallback.conf");
        Files.write(
                configFile,
                "origin = \"fallback\"\nsource { FakeSource {} }\nsink { Console {} }\n"
                        .getBytes(StandardCharsets.UTF_8));

        Map<String, Object> adapterConfig = new LinkedHashMap<>();
        adapterConfig.put("origin", "adapter");
        adapterConfig.put(
                "source",
                Arrays.asList(
                        Collections.singletonMap(
                                "schema",
                                Collections.singletonMap("fields", literalSchemaFields()))));
        adapterConfig.put(
                "sink", Arrays.asList(Collections.singletonMap("plugin_name", "Console")));

        ConfigAdapter adapter = mock(ConfigAdapter.class);
        when(adapter.loadConfig(configFile)).thenReturn(adapterConfig);

        Config config = ConfigBuilder.of(adapter, configFile, Collections.<String>emptyList());

        Assertions.assertEquals("adapter", config.getString("origin"));
        assertLiteralSchemaFields(config);
    }

    /** Quoted values remain literal for ConfigBuilder's variable substitution. */
    @Test
    public void testQuotedPlaceholderIsSubstitutedFromUserVariables(@TempDir Path tempDir)
            throws IOException {
        String configText =
                "env { job.name = \"prefix-${jobName$}\" }\nsource { FakeSource {} }\nsink { Console {} }\n";

        // HOCON leaves quoted placeholders for ConfigBuilder to resolve.
        Assertions.assertEquals(
                "prefix-${jobName$}",
                ConfigFactory.parseString(configText).getConfig("env").getString("job.name"));

        Path configFile = tempDir.resolve("variables.conf");
        Files.write(configFile, configText.getBytes(StandardCharsets.UTF_8));

        Config config = ConfigBuilder.of(configFile, Arrays.asList("jobName$=quoted_key_job"));

        Assertions.assertEquals(
                "prefix-quoted_key_job", config.getConfig("env").getString("job.name"));
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

    private static void assertWhitespacePaddedKeys(Config config) {
        Map<?, ?> fields =
                config.getConfigList("source")
                        .get(0)
                        .getConfig("schema")
                        .getConfig("fields")
                        .root()
                        .unwrapped();

        Assertions.assertEquals(4, fields.size());
        Assertions.assertEquals("trimmed", fields.get("padded"));
        Assertions.assertFalse(fields.containsKey("  padded  "));
        Assertions.assertEquals("dotted", fields.get("job.mode"));
        Assertions.assertEquals("regex", fields.get("  ^t_nova_.*$  "));
        Assertions.assertFalse(fields.containsKey("^t_nova_.*$"));
        Assertions.assertEquals("literal", fields.get("  ${FOO}  "));
        Assertions.assertFalse(fields.containsKey("${FOO}"));
    }

    private static void assertLiteralSchemaFields(Config config) {
        Assertions.assertEquals(
                literalSchemaFields(),
                config.getConfigList("source")
                        .get(0)
                        .getConfig("schema")
                        .getConfig("fields")
                        .root()
                        .unwrapped());
    }

    private static Map<String, Object> literalSchemaFields() {
        Map<String, Object> fields = new LinkedHashMap<>();
        fields.put("^t_nova_.*$", "string");
        fields.put("a:b", "long");
        fields.put("${FOO}", "boolean");
        fields.put("a\"b\\c", "int");
        return fields;
    }

    /** Quotes literal field keys while leaving the optional value substitution active. */
    private static Path writeLiteralKeyConfigFile(Path tempDir, boolean withJobNamePlaceholder)
            throws IOException {
        StringBuilder content = new StringBuilder();
        content.append("env {\n");
        content.append("  job.mode = BATCH\n");
        if (withJobNamePlaceholder) {
            content.append("  job.name = ${jobName}\n");
        }
        content.append("}\n");
        content.append("source {\n  FakeSource {\n    schema {\n");
        content.append("  fields {\n");
        for (Map.Entry<String, Object> field : literalSchemaFields().entrySet()) {
            content.append(literalKeyLine(field.getKey(), (String) field.getValue()));
        }
        content.append("  }\n");
        content.append("    }\n  }\n}\nsink { Console {} }\n");

        Path configFile = tempDir.resolve("literal_path_keys.conf");
        Files.write(configFile, content.toString().getBytes(StandardCharsets.UTF_8));
        return configFile;
    }

    private static String literalKeyLine(String key, String type) {
        return "    " + ConfigUtil.quoteString(key) + " = " + type + "\n";
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
