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

package org.apache.seatunnel.connectors.seatunnel.paimon.catalog;

import org.apache.seatunnel.shade.com.typesafe.config.ConfigFactory;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.common.utils.SeaTunnelException;
import org.apache.seatunnel.connectors.seatunnel.paimon.config.PaimonBaseOptions;
import org.apache.seatunnel.connectors.seatunnel.paimon.config.PaimonConfig;
import org.apache.seatunnel.connectors.seatunnel.paimon.security.PaimonSecurityContext;

import org.apache.hadoop.conf.Configuration;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.Appender;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.Logger;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.catalog.CatalogFactory;
import org.apache.paimon.options.Options;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.ArgumentCaptor;
import org.mockito.MockedStatic;

import java.io.Writer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.CALLS_REAL_METHODS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class PaimonCatalogLoaderTest {
    @TempDir private Path directory;

    @Test
    void testRestForwardsOnlyExplicitStorageOptions() {
        Map<String, Object> config = config("rest");
        Map<String, String> storage = new HashMap<>();
        storage.put("fs.s3a.endpoint", "storage.example.com");
        storage.put("dfs.nameservices", "managed-storage");
        config.put("paimon.hadoop.conf", storage);
        assertEquals(expectedRestOptions(storage), catalogOptions(config));
        // A managed warehouse is an opaque name, not an instruction to load Hadoop defaults.
        config.put("warehouse", "s3a://managed-warehouse");
        Map<String, String> expected = expectedRestOptions(storage);
        expected.put("warehouse", "s3a://managed-warehouse");
        assertEquals(expected, catalogOptions(config));
    }

    @Test
    void testRestRejectsCatalogAndAuthOptionsInStorageMap() {
        for (String key :
                new String[] {
                    "uri",
                    "warehouse",
                    "metastore",
                    "token",
                    "token.provider",
                    "token.refresh-time",
                    "dlf.token-path",
                    "dlf.token-loader",
                    "dlf.token-ecs-endpoint",
                    "dlf.security-token",
                    "hadoop.uri",
                    "hadoop.token",
                    "hadoop.dlf.token-path",
                    "hadoop.hadoop.dlf.token-loader"
                }) {
            Map<String, Object> config = config("rest");
            config.put(
                    "paimon.hadoop.conf",
                    Collections.singletonMap(key, "private-value\nsecret-tail"));
            RuntimeException error =
                    assertThrows(RuntimeException.class, () -> catalogOptions(config), key);
            assertTrue(error.getMessage().contains(key));
            assertFalse(error.getMessage().contains("private-value"));
            assertFalse(error.getMessage().contains("secret-tail"));
            assertNull(error.getCause());
        }
    }

    @Test
    void testRestRejectsCatalogAndAuthOptionsInStorageFiles() throws Exception {
        for (String option :
                new String[] {"hdfs_site_path", PaimonBaseOptions.HADOOP_CONF_PATH.key()}) {
            for (String key :
                    new String[] {"dlf.token-path", "token", "uri", "hadoop.dlf.security-token"}) {
                Path file = directory.resolve("hdfs-site.xml");
                writeConfiguration(file, Collections.singletonMap(key, "private-file-value"));
                Map<String, Object> config = config("rest");
                config.put(
                        option,
                        option.equals("hdfs_site_path") ? file.toString() : directory.toString());
                RuntimeException error =
                        assertThrows(RuntimeException.class, () -> catalogOptions(config), key);
                assertTrue(error.getMessage().contains(key));
                assertFalse(error.getMessage().contains("private-file-value"));
                assertNull(error.getCause());
            }
        }
    }

    @Test
    void testRestLoadsOnlyExplicitFileOptionsWithMapPrecedence() throws Exception {
        Path site = directory.resolve("custom-site.xml");
        writeConfiguration(site, Collections.singletonMap("dfs.nameservices", "site-storage"));
        Map<String, String> fileOptions = new HashMap<>();
        fileOptions.put("fs.s3a.endpoint", "file-endpoint");
        fileOptions.put("fs.s3a.path.style.access", "true");
        writeConfiguration(directory.resolve("core-site.xml"), fileOptions);
        Map<String, Object> config = config("rest");
        config.put("hdfs_site_path", site.toString());
        config.put(PaimonBaseOptions.HADOOP_CONF_PATH.key(), directory.toString());
        config.put(
                "paimon.hadoop.conf", Collections.singletonMap("fs.s3a.endpoint", "map-endpoint"));
        fileOptions.put("fs.s3a.endpoint", "map-endpoint");
        fileOptions.put("dfs.nameservices", "site-storage");
        assertEquals(expectedRestOptions(fileOptions), catalogOptions(config));
    }

    @Test
    void testRestRejectsUnsafeUrisWithoutEchoingValues() {
        for (String uri :
                new String[] {
                    "127.0.0.1:1", "https://user:private-password@catalog.example.com",
                            "https:///path",
                    "ftp://catalog.example.com", "https://", "https://bad host/private-value"
                }) {
            Map<String, Object> config = config("rest");
            config.put("catalog_uri", uri);
            SeaTunnelException error =
                    assertThrows(
                            SeaTunnelException.class,
                            () -> new PaimonConfig(ReadonlyConfig.fromMap(config)));
            assertTrue(error.getMessage().contains("catalog_uri"));
            assertFalse(error.getMessage().contains(uri));
            assertFalse(error.getMessage().contains("private-password"));
            assertNull(error.getCause());
        }
    }

    @Test
    void testRestUriWarnsOnlyForNonLoopbackHttpWithoutEchoingValues() {
        Logger logger = (Logger) LogManager.getLogger(PaimonConfig.class);
        Level previous = logger.getLevel();
        Appender appender = mock(Appender.class);
        when(appender.getName()).thenReturn("rest-uri-test");
        when(appender.isStarted()).thenReturn(true);
        logger.addAppender(appender);
        logger.setLevel(Level.WARN);
        try {
            for (String uri :
                    new String[] {
                        "https://catalog.example.com",
                        "http://localhost:8080",
                        "http://127.0.0.1:8080",
                        "http://[::1]:8080"
                    }) {
                Map<String, Object> config = config("rest");
                config.put("catalog_uri", uri);
                new PaimonConfig(ReadonlyConfig.fromMap(config));
            }
            verify(appender, times(0)).append(any(LogEvent.class));
            Map<String, Object> config = config("rest");
            config.put("catalog_uri", "http://remote-catalog.example.com");
            new PaimonConfig(ReadonlyConfig.fromMap(config));
            ArgumentCaptor<LogEvent> event = ArgumentCaptor.forClass(LogEvent.class);
            verify(appender).append(event.capture());
            assertEquals(Level.WARN, event.getValue().getLevel());
            assertTrue(event.getValue().getMessage().getFormattedMessage().contains("catalog_uri"));
            assertFalse(
                    event.getValue().getMessage().getFormattedMessage().contains("remote-catalog"));
        } finally {
            logger.removeAppender(appender);
            logger.setLevel(previous);
        }
    }

    @Test
    void testRestRequiresSupportedExactTokenProvider() {
        for (String provider : new String[] {null, "", " ", "BEAR", "private-provider"}) {
            Map<String, Object> config = config("rest");
            config.put("token.provider", provider);
            SeaTunnelException error =
                    assertThrows(
                            SeaTunnelException.class,
                            () -> new PaimonConfig(ReadonlyConfig.fromMap(config)));
            assertTrue(error.getMessage().contains("token.provider"));
            assertTrue(error.getMessage().contains("bear"));
            assertTrue(error.getMessage().contains("dlf"));
            assertFalse(error.getMessage().contains("private-provider"));
            assertNull(error.getCause());
        }
    }

    @Test
    void testRestBearerRequiresNonBlankToken() {
        for (String token : new String[] {null, "", " "}) {
            Map<String, Object> config = config("rest");
            config.put("token", token);
            SeaTunnelException error =
                    assertThrows(
                            SeaTunnelException.class,
                            () -> new PaimonConfig(ReadonlyConfig.fromMap(config)));
            assertTrue(error.getMessage().contains("token"));
            assertFalse(error.toString().contains("test-bearer"));
        }
    }

    @Test
    void testRestDlfRequiresBothAccessKeys() {
        for (String key : new String[] {"dlf.access-key-id", "dlf.access-key-secret"}) {
            for (String value : new String[] {null, "", " "}) {
                Map<String, Object> config = config("rest");
                config.put("token.provider", "dlf");
                config.put("dlf.access-key-id", "test-id");
                config.put("dlf.access-key-secret", "test-secret");
                config.put(key, value);
                SeaTunnelException error =
                        assertThrows(
                                SeaTunnelException.class,
                                () -> new PaimonConfig(ReadonlyConfig.fromMap(config)));
                assertTrue(error.getMessage().contains(key));
                assertFalse(error.getMessage().contains("test-secret"));
                assertFalse(String.valueOf(error.getCause()).contains("test-secret"));
            }
        }
    }

    @Test
    void testRestBearerOptions() {
        Map<String, Object> config = config("rest");
        config.put("token.provider", "bear");
        config.put("token", "test-bearer");
        Map<String, String> options = catalogOptions(config);
        assertEquals("rest", options.get("metastore"));
        assertEquals("http://localhost:8080", options.get("uri"));
        assertEquals("managed-warehouse", options.get("warehouse"));
        assertEquals("bear", options.get("token.provider"));
        assertEquals("test-bearer", options.get("token"));
    }

    @Test
    void testQuotedHoconRestOptions() {
        ReadonlyConfig config =
                ReadonlyConfig.fromConfig(
                        ConfigFactory.parseString(
                                "warehouse=managed-warehouse\ncatalog_type=rest\ncatalog_uri=\"https://catalog.example.com\"\n"
                                        + "\"token.provider\"=bear\ntoken=test-bearer\n"
                                        + "\"dlf.access-key-id\"=test-id\n\"dlf.access-key-secret\"=test-secret"));
        Map<String, String> options = new PaimonConfig(config).getRestOptions();
        assertEquals("bear", options.get("token.provider"));
        assertEquals("test-bearer", options.get("token"));
        assertEquals("test-secret", options.get("dlf.access-key-secret"));
    }

    @Test
    void testRestDlfOptions() {
        Map<String, Object> config = config("rest");
        config.put("token.provider", "dlf");
        config.put("dlf.access-key-id", "test-id");
        config.put("dlf.access-key-secret", "test-secret");
        config.put("dlf.security-token", "test-session");
        config.put("dlf.region", "cn-hangzhou");
        Map<String, String> options = catalogOptions(config);
        for (String key :
                new String[] {
                    "token.provider",
                    "dlf.access-key-id",
                    "dlf.access-key-secret",
                    "dlf.security-token",
                    "dlf.region"
                }) {
            assertEquals(config.get(key), options.get(key));
        }
    }

    @Test
    void testRestDoesNotForwardAnUnconfiguredRegion() {
        Map<String, Object> config = config("rest");
        config.put("catalog_uri", "https://cn-hangzhou.example.com");
        config.put("token.provider", "dlf");
        config.put("dlf.access-key-id", "test-id");
        config.put("dlf.access-key-secret", "test-secret");
        assertFalse(catalogOptions(config).containsKey("dlf.region"));
    }

    @Test
    void testRestRetainsHadoopStorageConfiguration() {
        Map<String, Object> config = config("rest");
        Map<String, String> hadoop = new HashMap<>();
        hadoop.put("dfs.nameservices", "managed-storage");
        hadoop.put("dfs.ha.namenodes.managed-storage", "nn1,nn2");
        hadoop.put("fs.s3a.endpoint", "storage.example.com");
        hadoop.put("fs.hdfs.impl.disable.cache", "true");
        config.put("paimon.hadoop.conf", hadoop);
        Map<String, String> options = catalogOptions(config);
        // RESTCatalog rebuilds its context after merging the server's configuration.
        CatalogContext effective = CatalogContext.create(Options.fromMap(options));
        assertEquals("managed-storage", effective.hadoopConf().get("dfs.nameservices"));
        assertEquals("nn1,nn2", effective.hadoopConf().get("dfs.ha.namenodes.managed-storage"));
        assertEquals("storage.example.com", options.get("fs.s3a.endpoint"));
        assertEquals("true", effective.hadoopConf().get("fs.hdfs.impl.disable.cache"));
        assertEquals("managed-warehouse", options.get("warehouse"));
        assertEquals("rest", options.get("metastore"));
        assertEquals("http://localhost:8080", options.get("uri"));
    }

    @Test
    void testMalformedDlfSecurityTokenIsRejectedWithoutLeakingIt() {
        for (String token :
                new String[] {
                    "test-session\ncontinuation",
                    "test-session\rcontinuation",
                    "test-session\u0000continuation",
                    "test-session\u0080continuation"
                }) {
            Map<String, Object> config = config("rest");
            config.put("token.provider", "dlf");
            config.put("dlf.access-key-id", "test-id");
            config.put("dlf.access-key-secret", "test-secret");
            config.put("dlf.security-token", token);
            SeaTunnelException error =
                    assertThrows(
                            SeaTunnelException.class,
                            () -> new PaimonConfig(ReadonlyConfig.fromMap(config)));
            assertTrue(error.getMessage().contains("dlf.security-token"));
            for (Throwable cause = error; cause != null; cause = cause.getCause()) {
                assertFalse(String.valueOf(cause.getMessage()).contains("test-session"));
                assertFalse(String.valueOf(cause.getMessage()).contains("continuation"));
            }
        }
    }

    @Test
    void testRestRetainsSecurityConfigurationAfterInitialization() {
        try (MockedStatic<PaimonSecurityContext> security =
                mockStatic(PaimonSecurityContext.class, CALLS_REAL_METHODS)) {
            security.when(
                            () ->
                                    PaimonSecurityContext.shouldEnableKerberos(
                                            any(Configuration.class)))
                    .thenAnswer(
                            invocation -> {
                                Configuration configuration = invocation.getArgument(0);
                                configuration.set("hadoop.security.authentication", "kerberos");
                                return null;
                            });
            CatalogContext effective =
                    CatalogContext.create(Options.fromMap(catalogOptions(config("rest"))));
            assertEquals("kerberos", effective.hadoopConf().get("hadoop.security.authentication"));
        }
    }

    @Test
    void testRestMissingOrBlankUriFailsWithConnectorError() {
        for (String uri : new String[] {null, "", " "}) {
            Map<String, Object> config = config("rest");
            config.put("catalog_uri", uri);
            SeaTunnelException error =
                    assertThrows(
                            SeaTunnelException.class,
                            () -> new PaimonConfig(ReadonlyConfig.fromMap(config)));
            assertTrue(error.getMessage().contains("catalog_uri"));
        }
    }

    @Test
    void testFilesystemOptionsUnchanged() {
        Map<String, Object> config = config("filesystem");
        config.put("warehouse", "file:///tmp/paimon-test");
        config.put("user", "test-user");
        config.put("password", "test-password");
        config.put("token", "ignored");
        Map<String, String> expected = new HashMap<>();
        expected.put("metastore", "filesystem");
        expected.put("warehouse", "file:///tmp/paimon-test");
        expected.put("user", "test-user");
        expected.put("password", "test-password");
        assertEquals(expected, catalogOptions(config));
    }

    @Test
    void testHiveOptionsUnchanged() {
        Map<String, Object> config = config("hive");
        config.put("warehouse", "file:///tmp/paimon-hive");
        config.put("catalog_uri", "thrift://localhost:9083");
        Map<String, String> expected =
                new HashMap<>(
                        PaimonSecurityContext.loadHadoopConfig(
                                        new PaimonConfig(ReadonlyConfig.fromMap(config)))
                                .getPropsWithPrefix(""));
        expected.put("metastore", "hive");
        expected.put("warehouse", "file:///tmp/paimon-hive");
        expected.put("uri", "thrift://localhost:9083");
        assertEquals(expected, catalogOptions(config));
    }

    @Test
    void testRestCatalogIsClosed() throws Exception {
        Catalog delegate = mock(Catalog.class);
        ClassLoader previous = Thread.currentThread().getContextClassLoader();
        try (MockedStatic<CatalogFactory> factory = mockStatic(CatalogFactory.class)) {
            factory.when(() -> CatalogFactory.createCatalog(any(CatalogContext.class)))
                    .thenReturn(delegate);
            try (PaimonCatalog catalog =
                    new PaimonCatalog("test", ReadonlyConfig.fromMap(config("rest")))) {
                catalog.open();
            }
            verify(delegate).close();
        } finally {
            Thread.currentThread().setContextClassLoader(previous);
        }
    }

    @Test
    void testUnknownCatalogTypeStillFails() {
        Map<String, Object> config = config("unknown");
        assertThrows(
                IllegalArgumentException.class,
                () -> new PaimonConfig(ReadonlyConfig.fromMap(config)));
    }

    private Map<String, Object> config(String type) {
        Map<String, Object> config = new HashMap<>();
        config.put("catalog_type", type);
        config.put("catalog_uri", "http://localhost:8080");
        config.put("warehouse", "managed-warehouse");
        if (type.equals("rest")) {
            config.put("token.provider", "bear");
            config.put("token", "test-bearer");
        }
        return config;
    }

    private Map<String, String> expectedRestOptions(Map<String, String> storage) {
        Map<String, String> expected = new HashMap<>(storage);
        storage.forEach((key, value) -> expected.put("hadoop." + key, value));
        expected.put("uri", "http://localhost:8080");
        expected.put("warehouse", "managed-warehouse");
        expected.put("metastore", "rest");
        expected.put("token.provider", "bear");
        expected.put("token", "test-bearer");
        return expected;
    }

    private void writeConfiguration(Path file, Map<String, String> values) throws Exception {
        Configuration configuration = new Configuration(false);
        values.forEach(configuration::set);
        try (Writer writer = Files.newBufferedWriter(file)) {
            configuration.writeXml(writer);
        }
    }

    private Map<String, String> catalogOptions(Map<String, Object> config) {
        AtomicReference<Map<String, String>> options = new AtomicReference<>();
        ClassLoader previous = Thread.currentThread().getContextClassLoader();
        try (MockedStatic<CatalogFactory> factory = mockStatic(CatalogFactory.class)) {
            factory.when(() -> CatalogFactory.createCatalog(any(CatalogContext.class)))
                    .thenAnswer(
                            invocation -> {
                                CatalogContext context = invocation.getArgument(0);
                                options.set(new HashMap<>(context.options().toMap()));
                                return mock(Catalog.class);
                            });
            new PaimonCatalogLoader(new PaimonConfig(ReadonlyConfig.fromMap(config))).loadCatalog();
            return options.get();
        } finally {
            Thread.currentThread().setContextClassLoader(previous);
        }
    }
}
