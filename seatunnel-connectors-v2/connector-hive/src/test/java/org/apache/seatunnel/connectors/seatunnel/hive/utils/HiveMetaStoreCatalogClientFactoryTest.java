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

package org.apache.seatunnel.connectors.seatunnel.hive.utils;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.table.catalog.exception.CatalogException;
import org.apache.seatunnel.connectors.seatunnel.hive.config.HiveBaseOptions;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaHookLoader;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.lang.reflect.Proxy;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class HiveMetaStoreCatalogClientFactoryTest {

    @TempDir private Path temporaryDirectory;

    @AfterEach
    void resetFactory() {
        TestingMetaStoreClientFactory.reset();
    }

    @Test
    void testCreateClientFromConfiguredFactory() throws Exception {
        Map<String, String> hadoopProperties = new HashMap<>();
        hadoopProperties.put(
                HiveMetaStoreCatalog.METASTORE_CLIENT_FACTORY_CLASS,
                TestingMetaStoreClientFactory.class.getName());
        hadoopProperties.put("hive.metastore.glue.catalogid", "123456789012");
        try (HiveMetaStoreCatalog catalog = createCatalog(null, hadoopProperties)) {
            catalog.open();
        }

        assertNotNull(TestingMetaStoreClientFactory.hiveConf);
        assertNotNull(TestingMetaStoreClientFactory.hookLoader);
        assertFalse(TestingMetaStoreClientFactory.allowEmbedded);
        assertNotNull(TestingMetaStoreClientFactory.metaCallTimeMap);
        assertEquals(
                "123456789012",
                TestingMetaStoreClientFactory.hiveConf.get("hive.metastore.glue.catalogid"));
    }

    @Test
    void testConfiguredFactoryTakesPriorityOverMetastoreUri() throws Exception {
        Map<String, String> hadoopProperties = new HashMap<>();
        hadoopProperties.put(
                HiveMetaStoreCatalog.METASTORE_CLIENT_FACTORY_CLASS,
                TestingMetaStoreClientFactory.class.getName());
        try (HiveMetaStoreCatalog catalog =
                createCatalog("thrift://hive-metastore:9083", hadoopProperties)) {
            catalog.open();
        }

        assertTrue(TestingMetaStoreClientFactory.created);
    }

    @Test
    void testCreateClientFromHiveSite() throws Exception {
        Path hiveSite = temporaryDirectory.resolve("hive-site.xml");
        Files.write(
                hiveSite,
                Arrays.asList(
                        "<configuration>",
                        "  <property>",
                        "    <name>hive.metastore.client.factory.class</name>",
                        "    <value>" + TestingMetaStoreClientFactory.class.getName() + "</value>",
                        "  </property>",
                        "</configuration>"));
        Map<String, Object> options = new HashMap<>();
        options.put(HiveBaseOptions.HIVE_SITE_PATH.key(), hiveSite.toString());
        try (HiveMetaStoreCatalog catalog =
                new HiveMetaStoreCatalog(ReadonlyConfig.fromMap(options))) {
            catalog.open();
        }

        assertEquals(
                TestingMetaStoreClientFactory.class.getName(),
                TestingMetaStoreClientFactory.hiveConf.get(
                        HiveMetaStoreCatalog.METASTORE_CLIENT_FACTORY_CLASS));
    }

    @Test
    void testMissingFactoryDoesNotFallBackToEmbeddedMetastore() throws Exception {
        Map<String, String> hadoopProperties = new HashMap<>();
        hadoopProperties.put(
                HiveMetaStoreCatalog.METASTORE_CLIENT_FACTORY_CLASS,
                "org.apache.seatunnel.hive.MissingMetaStoreClientFactory");
        HiveMetaStoreCatalog catalog = createCatalog(null, hadoopProperties);

        CatalogException exception = assertThrows(CatalogException.class, catalog::open);

        assertTrue(rootCause(exception) instanceof ClassNotFoundException);
        assertTrue(hasCauseMessage(exception, "MissingMetaStoreClientFactory"));
    }

    @Test
    void testMissingMetastoreConfigurationDoesNotUseEmbeddedMetastore() throws Exception {
        HiveMetaStoreCatalog catalog = createCatalog(null, new HashMap<>());

        CatalogException exception = assertThrows(CatalogException.class, catalog::open);

        assertTrue(rootCause(exception) instanceof IllegalArgumentException);
        assertTrue(
                hasCauseMessage(exception, "metastore_uri or hive.metastore.client.factory.class"));
    }

    private static HiveMetaStoreCatalog createCatalog(
            String metastoreUri, Map<String, String> hadoopProperties) {
        Map<String, Object> options = new HashMap<>();
        if (metastoreUri != null) {
            options.put(HiveBaseOptions.METASTORE_URI.key(), metastoreUri);
        }
        options.put(HiveBaseOptions.HADOOP_CONF.key(), hadoopProperties);
        return new HiveMetaStoreCatalog(ReadonlyConfig.fromMap(options));
    }

    private static Throwable rootCause(Throwable throwable) {
        Throwable cause = throwable;
        while (cause.getCause() != null) {
            cause = cause.getCause();
        }
        return cause;
    }

    private static boolean hasCauseMessage(Throwable throwable, String message) {
        Throwable cause = throwable;
        while (cause != null) {
            if (cause.getMessage() != null && cause.getMessage().contains(message)) {
                return true;
            }
            cause = cause.getCause();
        }
        return false;
    }

    public static class TestingMetaStoreClientFactory {
        private static final IMetaStoreClient CLIENT =
                (IMetaStoreClient)
                        Proxy.newProxyInstance(
                                IMetaStoreClient.class.getClassLoader(),
                                new Class<?>[] {IMetaStoreClient.class},
                                (proxy, method, arguments) -> null);

        private static HiveConf hiveConf;
        private static HiveMetaHookLoader hookLoader;
        private static boolean allowEmbedded;
        private static ConcurrentHashMap<String, Long> metaCallTimeMap;
        private static boolean created;

        public IMetaStoreClient createMetaStoreClient(
                HiveConf hiveConf,
                HiveMetaHookLoader hookLoader,
                boolean allowEmbedded,
                ConcurrentHashMap<String, Long> metaCallTimeMap) {
            TestingMetaStoreClientFactory.hiveConf = hiveConf;
            TestingMetaStoreClientFactory.hookLoader = hookLoader;
            TestingMetaStoreClientFactory.allowEmbedded = allowEmbedded;
            TestingMetaStoreClientFactory.metaCallTimeMap = metaCallTimeMap;
            TestingMetaStoreClientFactory.created = true;
            return CLIENT;
        }

        private static void reset() {
            hiveConf = null;
            hookLoader = null;
            allowEmbedded = false;
            metaCallTimeMap = null;
            created = false;
        }
    }
}
