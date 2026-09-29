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

package org.apache.seatunnel.connectors.seatunnel.couchbase.sink;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.configuration.util.ConfigValidator;
import org.apache.seatunnel.api.configuration.util.OptionValidationException;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.TableIdentifier;
import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.api.table.factory.TableSinkFactoryContext;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.Collection;
import com.couchbase.client.java.Scope;

import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class CouchbaseSinkFactoryTest {

    @Test
    void testConfiguredReadinessTimeoutReachesWriter() throws Exception {
        Map<String, Object> config = baseConfig();
        config.put("ready.timeout", 60);
        verifyReadinessTimeout(config, Duration.ofSeconds(60));
    }

    @Test
    void testDefaultReadinessTimeoutRemainsThirtySeconds() throws Exception {
        verifyReadinessTimeout(baseConfig(), Duration.ofSeconds(30));
    }

    @ParameterizedTest
    @ValueSource(ints = {0, -1})
    void testNonPositiveReadinessTimeoutRejectedBeforeConnecting(int timeout) {
        Map<String, Object> config = baseConfig();
        config.put("ready.timeout", timeout);
        try (MockedStatic<Cluster> staticCluster = Mockito.mockStatic(Cluster.class)) {
            OptionValidationException error =
                    assertThrows(OptionValidationException.class, () -> createSink(config));
            assertTrue(error.getMessage().contains("ready.timeout"));
            staticCluster.verifyNoInteractions();
        }
    }

    @ParameterizedTest
    @ValueSource(ints = {0, -1})
    void testDirectFactoryAlsoRejectsNonPositiveTimeout(int timeout) {
        Map<String, Object> config = baseConfig();
        config.put("ready.timeout", timeout);
        try (MockedStatic<Cluster> staticCluster = Mockito.mockStatic(Cluster.class)) {
            IllegalArgumentException error =
                    assertThrows(
                            IllegalArgumentException.class,
                            () ->
                                    new CouchbaseSinkFactory()
                                            .createSink(
                                                    new TableSinkFactoryContext(
                                                            null,
                                                            ReadonlyConfig.fromMap(config),
                                                            getClass().getClassLoader())));
            assertTrue(error.getMessage().contains("ready.timeout"));
            staticCluster.verifyNoInteractions();
        }
    }

    @Test
    void testWriteRetriesDoNotChangeReadinessTimeout() throws Exception {
        Map<String, Object> config = baseConfig();
        config.put("retry.max", 10);
        config.put("retry.interval", 5000L);
        verifyReadinessTimeout(config, Duration.ofSeconds(30));
    }

    private void verifyReadinessTimeout(Map<String, Object> config, Duration expected)
            throws Exception {
        Cluster cluster = mock(Cluster.class);
        Bucket bucket = mock(Bucket.class);
        Scope scope = mock(Scope.class);
        Collection collection = mock(Collection.class);
        when(cluster.bucket("test_bucket")).thenReturn(bucket);
        when(bucket.scope("_default")).thenReturn(scope);
        when(scope.collection("_default")).thenReturn(collection);

        try (MockedStatic<Cluster> staticCluster = Mockito.mockStatic(Cluster.class)) {
            staticCluster
                    .when(() -> Cluster.connect("couchbase://localhost", "user", "pass"))
                    .thenReturn(cluster);
            CouchbaseWriter writer = createSink(config).createWriter(null);
            try {
                verify(bucket).waitUntilReady(expected);
            } finally {
                writer.close();
            }
            verify(cluster).disconnect();
        }
    }

    private CouchbaseSink createSink(Map<String, Object> config) {
        CouchbaseSinkFactory factory = new CouchbaseSinkFactory();
        ReadonlyConfig options = ReadonlyConfig.fromMap(config);
        ConfigValidator.of(options).validate(factory.optionRule());
        CatalogTable table =
                CatalogTable.of(
                        TableIdentifier.of("catalog", "database", "table"),
                        TableSchema.builder().build(),
                        Collections.emptyMap(),
                        Collections.emptyList(),
                        "");
        return (CouchbaseSink)
                factory.createSink(
                                new TableSinkFactoryContext(
                                        table, options, getClass().getClassLoader()))
                        .createSink();
    }

    private Map<String, Object> baseConfig() {
        Map<String, Object> config = new HashMap<>();
        config.put("connection.string", "couchbase://localhost");
        config.put("username", "user");
        config.put("password", "pass");
        config.put("bucket", "test_bucket");
        config.put("collection", "_default");
        return config;
    }
}
