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

package org.apache.seatunnel.connectors.seatunnel.elasticsearch.sink;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.sink.MultiTableResourceManager;
import org.apache.seatunnel.api.sink.SinkWriter;
import org.apache.seatunnel.api.sink.multitablesink.SinkContextProxy;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.PhysicalColumn;
import org.apache.seatunnel.api.table.catalog.TableIdentifier;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.api.table.schema.event.AlterColumnCommentEvent;
import org.apache.seatunnel.api.table.schema.event.AlterTableAddColumnEvent;
import org.apache.seatunnel.api.table.schema.event.AlterTableCommentEvent;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.connectors.seatunnel.elasticsearch.client.EsRestClient;
import org.apache.seatunnel.connectors.seatunnel.elasticsearch.dto.ElasticsearchClusterInfo;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Verifies Elasticsearch REST client ownership for standalone and multi-table sink writers.
 *
 * <p>The tests guard resource reduction, table-specific connection isolation, and the comment-only
 * schema-change no-op behavior of the writer.
 */
class ElasticsearchSinkWriterTest {

    private static final TableIdentifier TABLE_IDENTIFIER =
            TableIdentifier.of("", TablePath.DEFAULT);

    /**
     * Multi-table writers must share one client and release it only after the last writer closes.
     */
    @Test
    void testMultiTableWritersShareRestClient() {
        EsRestClient sharedClient = mock(EsRestClient.class);
        when(sharedClient.getClusterInfo()).thenReturn(clusterInfo());

        try (MockedStatic<EsRestClient> clientFactory = mockStatic(EsRestClient.class)) {
            clientFactory.when(() -> EsRestClient.createInstance(any())).thenReturn(sharedClient);

            ElasticsearchSinkWriter firstWriter =
                    createMultiTableWriter(
                            "first", config("http://localhost:9200", "first_index", null));
            ElasticsearchSinkWriter secondWriter =
                    createMultiTableWriter(
                            "second", config("http://localhost:9200", "second_index", null));
            clientFactory.verify(() -> EsRestClient.createInstance(any()), never());

            MultiTableResourceManager<EsRestClient> resourceManager =
                    firstWriter.initMultiTableResourceManager(2, 1);
            firstWriter.setMultiTableResourceManager(resourceManager, 0);
            secondWriter.setMultiTableResourceManager(resourceManager, 0);

            clientFactory.verify(() -> EsRestClient.createInstance(any()), times(1));
            verify(sharedClient, times(1)).getClusterInfo();
            assertSame(
                    sharedClient,
                    resourceManager.getSharedResource().orElseThrow(AssertionError::new));

            firstWriter.close();
            verify(sharedClient, never()).close();

            secondWriter.close();
            verify(sharedClient, times(1)).close();

            resourceManager.close();
            verify(sharedClient, times(1)).close();
        }
    }

    /**
     * Writers with different connection settings must not share credentials, TLS state, or hosts.
     */
    @Test
    void testMultiTableWritersKeepDifferentConnectionsIsolated() {
        EsRestClient firstClient = mock(EsRestClient.class);
        EsRestClient secondClient = mock(EsRestClient.class);
        when(firstClient.getClusterInfo()).thenReturn(clusterInfo());
        when(secondClient.getClusterInfo()).thenReturn(clusterInfo());

        try (MockedStatic<EsRestClient> clientFactory = mockStatic(EsRestClient.class)) {
            clientFactory
                    .when(() -> EsRestClient.createInstance(any()))
                    .thenReturn(firstClient, secondClient);

            ElasticsearchSinkWriter firstWriter =
                    createMultiTableWriter(
                            "first", config("http://localhost:9200", "first_index", "first_user"));
            ElasticsearchSinkWriter secondWriter =
                    createMultiTableWriter(
                            "second",
                            config("http://localhost:9200", "second_index", "second_user"));
            MultiTableResourceManager<EsRestClient> resourceManager =
                    firstWriter.initMultiTableResourceManager(2, 1);

            firstWriter.setMultiTableResourceManager(resourceManager, 0);
            secondWriter.setMultiTableResourceManager(resourceManager, 0);

            clientFactory.verify(() -> EsRestClient.createInstance(any()), times(2));
            verify(firstClient, times(1)).getClusterInfo();
            verify(secondClient, times(1)).getClusterInfo();
            assertFalse(resourceManager.getSharedResource().isPresent());

            firstWriter.close();
            verify(firstClient, times(1)).close();
            verify(secondClient, never()).close();

            secondWriter.close();
            verify(firstClient, times(1)).close();
            verify(secondClient, times(1)).close();

            resourceManager.close();
            resourceManager.close();
            verify(firstClient, times(1)).close();
            verify(secondClient, times(1)).close();
        }
    }

    /**
     * A later connection-group initialization failure must close earlier groups immediately.
     *
     * <p>This prevents task startup retries from accumulating clients from partial initialization.
     */
    @Test
    void testConnectionGroupInitializationFailureClosesAllClients() {
        EsRestClient firstClient = mock(EsRestClient.class);
        EsRestClient failingClient = mock(EsRestClient.class);
        when(firstClient.getClusterInfo()).thenReturn(clusterInfo());
        when(failingClient.getClusterInfo())
                .thenThrow(new IllegalStateException("cluster info unavailable"));

        try (MockedStatic<EsRestClient> clientFactory = mockStatic(EsRestClient.class)) {
            clientFactory
                    .when(() -> EsRestClient.createInstance(any()))
                    .thenReturn(firstClient, failingClient);

            ElasticsearchSinkWriter firstWriter =
                    createMultiTableWriter(
                            "first", config("http://localhost:9200", "first_index", "first_user"));
            ElasticsearchSinkWriter failingWriter =
                    createMultiTableWriter(
                            "second",
                            config("http://localhost:9200", "second_index", "second_user"));
            MultiTableResourceManager<EsRestClient> resourceManager =
                    firstWriter.initMultiTableResourceManager(2, 1);

            firstWriter.setMultiTableResourceManager(resourceManager, 0);
            assertThrows(
                    IllegalStateException.class,
                    () -> failingWriter.setMultiTableResourceManager(resourceManager, 0));

            verify(firstClient, times(1)).close();
            verify(failingClient, times(1)).close();
            resourceManager.close();
            verify(firstClient, times(1)).close();
            verify(failingClient, times(1)).close();
        }
    }

    /**
     * A serializer initialization failure must close the client cached during resource injection.
     */
    @Test
    void testSerializerInitializationFailureClosesSharedClient() {
        EsRestClient sharedClient = mock(EsRestClient.class);
        when(sharedClient.getClusterInfo())
                .thenReturn(
                        ElasticsearchClusterInfo.builder()
                                .clusterVersion("invalid-version")
                                .build());

        try (MockedStatic<EsRestClient> clientFactory = mockStatic(EsRestClient.class)) {
            clientFactory.when(() -> EsRestClient.createInstance(any())).thenReturn(sharedClient);

            ElasticsearchSinkWriter writer =
                    createMultiTableWriter("table", config("http://localhost:9200", "index", null));
            MultiTableResourceManager<EsRestClient> resourceManager =
                    writer.initMultiTableResourceManager(1, 1);

            assertThrows(
                    NumberFormatException.class,
                    () -> writer.setMultiTableResourceManager(resourceManager, 0));

            verify(sharedClient, times(1)).close();
            resourceManager.close();
            verify(sharedClient, times(1)).close();
        }
    }

    /**
     * Standalone writers must retain the existing eager initialization and ownership behavior.
     *
     * <p>This prevents the multi-table optimization from weakening startup validation.
     */
    @Test
    void testStandaloneWriterOwnsRestClient() {
        EsRestClient standaloneClient = mock(EsRestClient.class);
        when(standaloneClient.getClusterInfo()).thenReturn(clusterInfo());

        try (MockedStatic<EsRestClient> clientFactory = mockStatic(EsRestClient.class)) {
            clientFactory
                    .when(() -> EsRestClient.createInstance(any()))
                    .thenReturn(standaloneClient);

            ElasticsearchSinkWriter writer =
                    createWriter(
                            mock(SinkWriter.Context.class),
                            "standalone",
                            config("http://localhost:9200", "standalone_index", null));

            clientFactory.verify(() -> EsRestClient.createInstance(any()), times(1));
            verify(standaloneClient, times(1)).getClusterInfo();

            writer.close();
            verify(standaloneClient, times(1)).close();
        }
    }

    /** Comment-only schema changes must not trigger any Elasticsearch mapping update. */
    @Test
    void commentOnlySchemaChangeEventsAreNoOpForElasticsearch() {
        Assertions.assertTrue(
                ElasticsearchSinkWriter.isCommentOnlyEvent(
                        AlterTableCommentEvent.of(TABLE_IDENTIFIER, "old", "new")));
        Assertions.assertTrue(
                ElasticsearchSinkWriter.isCommentOnlyEvent(
                        AlterColumnCommentEvent.of(TABLE_IDENTIFIER, "name", "old", "new")));
    }

    /** Physical schema changes still require an Elasticsearch mapping change. */
    @Test
    void physicalSchemaChangeEventsStillRequireElasticsearchMappingChanges() {
        Assertions.assertFalse(
                ElasticsearchSinkWriter.isCommentOnlyEvent(
                        AlterTableAddColumnEvent.add(
                                TABLE_IDENTIFIER,
                                PhysicalColumn.builder()
                                        .name("name")
                                        .dataType(BasicType.STRING_TYPE)
                                        .build())));
    }

    /**
     * Creates a writer with the context used by the multi-table sink runtime.
     *
     * @param tableName target table name
     * @param config table-specific Elasticsearch configuration
     * @return uninitialized multi-table writer
     */
    private ElasticsearchSinkWriter createMultiTableWriter(
            String tableName, ReadonlyConfig config) {
        SinkWriter.Context context = mock(SinkWriter.Context.class);
        return createWriter(new SinkContextProxy(0, 1, context), tableName, config);
    }

    /**
     * Creates a writer with deterministic table metadata and connector options.
     *
     * @param context sink writer context
     * @param tableName target table name
     * @param config table-specific Elasticsearch configuration
     * @return Elasticsearch sink writer
     */
    private ElasticsearchSinkWriter createWriter(
            SinkWriter.Context context, String tableName, ReadonlyConfig config) {
        SeaTunnelRowType rowType =
                new SeaTunnelRowType(
                        new String[] {"id"}, new SeaTunnelDataType<?>[] {BasicType.INT_TYPE});
        CatalogTable catalogTable = mock(CatalogTable.class);
        when(catalogTable.getTableId())
                .thenReturn(TableIdentifier.of("catalog", "database", tableName));
        when(catalogTable.getSeaTunnelRowType()).thenReturn(rowType);
        when(catalogTable.getTableSchema()).thenReturn(mock(TableSchema.class));
        return new ElasticsearchSinkWriter(context, catalogTable, config, 1000, 3);
    }

    /**
     * Returns the minimal connection configuration required by the writer.
     *
     * @param host Elasticsearch HTTP endpoint
     * @param index table-specific target index
     * @param username optional basic-auth username
     * @return Elasticsearch connector configuration
     */
    private ReadonlyConfig config(String host, String index, String username) {
        Map<String, Object> options = new HashMap<>();
        options.put("hosts", Collections.singletonList(host));
        options.put("index", index);
        if (username != null) {
            options.put("username", username);
            options.put("password", "password");
        }
        return ReadonlyConfig.fromMap(options);
    }

    /**
     * Returns deterministic cluster metadata for serializer construction.
     *
     * @return Elasticsearch cluster metadata
     */
    private ElasticsearchClusterInfo clusterInfo() {
        return ElasticsearchClusterInfo.builder().clusterVersion("7.17.0").build();
    }
}
