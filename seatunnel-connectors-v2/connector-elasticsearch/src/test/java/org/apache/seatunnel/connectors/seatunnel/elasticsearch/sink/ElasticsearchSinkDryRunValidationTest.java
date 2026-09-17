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
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.CatalogTableUtil;
import org.apache.seatunnel.api.table.factory.SupportSinkDryRunValidation;
import org.apache.seatunnel.api.table.factory.TableSinkFactoryContext;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.common.utils.SeaTunnelException;
import org.apache.seatunnel.connectors.seatunnel.elasticsearch.client.EsRestClient;
import org.apache.seatunnel.connectors.seatunnel.elasticsearch.config.ElasticsearchSinkOptions;
import org.apache.seatunnel.connectors.seatunnel.elasticsearch.dto.ElasticsearchClusterInfo;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Tests for the dry-run validation and connection verification logic in {@link
 * ElasticsearchSinkFactory}.
 */
class ElasticsearchSinkDryRunValidationTest {

    @Test
    void testImplementsSupportSinkDryRunValidation() {
        ElasticsearchSinkFactory factory = new ElasticsearchSinkFactory();
        Assertions.assertInstanceOf(SupportSinkDryRunValidation.class, factory);
    }

    @Test
    void testValidateConnectionForDryRunSucceedsWithValidCluster() throws Exception {
        ElasticsearchSinkFactory factory = new ElasticsearchSinkFactory();
        EsRestClient mockClient = mock(EsRestClient.class);
        ElasticsearchClusterInfo clusterInfo =
                ElasticsearchClusterInfo.builder()
                        .distribution("elasticsearch")
                        .clusterVersion("8.10.0")
                        .build();
        when(mockClient.getClusterInfo()).thenReturn(clusterInfo);
        when(mockClient.checkIndexExist("test_index")).thenReturn(true);

        try (MockedStatic<EsRestClient> mockedStatic = mockStatic(EsRestClient.class)) {
            mockedStatic.when(() -> EsRestClient.createInstance(any())).thenReturn(mockClient);

            TableSinkFactoryContext context = createSinkContext("test_index");
            Assertions.assertDoesNotThrow(() -> factory.validateConnectionForDryRun(context));

            verify(mockClient).getClusterInfo();
            verify(mockClient).checkIndexExist("test_index");
        }
    }

    @Test
    void testValidateConnectionForDryRunSucceedsWithOpenSearch() throws Exception {
        ElasticsearchSinkFactory factory = new ElasticsearchSinkFactory();
        EsRestClient mockClient = mock(EsRestClient.class);
        ElasticsearchClusterInfo clusterInfo =
                ElasticsearchClusterInfo.builder()
                        .distribution("opensearch")
                        .clusterVersion("2.11.0")
                        .build();
        when(mockClient.getClusterInfo()).thenReturn(clusterInfo);
        when(mockClient.checkIndexExist("os_index")).thenReturn(false);

        try (MockedStatic<EsRestClient> mockedStatic = mockStatic(EsRestClient.class)) {
            mockedStatic.when(() -> EsRestClient.createInstance(any())).thenReturn(mockClient);

            TableSinkFactoryContext context = createSinkContext("os_index");
            Assertions.assertDoesNotThrow(() -> factory.validateConnectionForDryRun(context));

            verify(mockClient).getClusterInfo();
            verify(mockClient).checkIndexExist("os_index");
        }
    }

    @Test
    void testValidateConnectionForDryRunFailsWhenClusterInfoIsNull() {
        ElasticsearchSinkFactory factory = new ElasticsearchSinkFactory();
        EsRestClient mockClient = mock(EsRestClient.class);
        when(mockClient.getClusterInfo()).thenReturn(null);

        try (MockedStatic<EsRestClient> mockedStatic = mockStatic(EsRestClient.class)) {
            mockedStatic.when(() -> EsRestClient.createInstance(any())).thenReturn(mockClient);

            TableSinkFactoryContext context = createSinkContext("test_index");
            SeaTunnelException exception =
                    Assertions.assertThrows(
                            SeaTunnelException.class,
                            () -> factory.validateConnectionForDryRun(context));
            Assertions.assertTrue(
                    exception.getMessage().contains("getClusterInfo() returned null"),
                    "Actual: " + exception.getMessage());
        }
    }

    @Test
    void testValidateConnectionForDryRunFailsWhenConnectionFails() {
        ElasticsearchSinkFactory factory = new ElasticsearchSinkFactory();
        EsRestClient mockClient = mock(EsRestClient.class);
        when(mockClient.getClusterInfo()).thenThrow(new RuntimeException("Connection refused"));

        try (MockedStatic<EsRestClient> mockedStatic = mockStatic(EsRestClient.class)) {
            mockedStatic.when(() -> EsRestClient.createInstance(any())).thenReturn(mockClient);

            TableSinkFactoryContext context = createSinkContext("test_index");
            SeaTunnelException exception =
                    Assertions.assertThrows(
                            SeaTunnelException.class,
                            () -> factory.validateConnectionForDryRun(context));
            Assertions.assertTrue(
                    exception.getMessage().contains("Connection refused"),
                    "Actual: " + exception.getMessage());
        }
    }

    @Test
    void testValidateConnectionSkipsIndexCheckForDynamicIndex() throws Exception {
        ElasticsearchSinkFactory factory = new ElasticsearchSinkFactory();
        EsRestClient mockClient = mock(EsRestClient.class);
        ElasticsearchClusterInfo clusterInfo =
                ElasticsearchClusterInfo.builder()
                        .distribution("elasticsearch")
                        .clusterVersion("8.10.0")
                        .build();
        when(mockClient.getClusterInfo()).thenReturn(clusterInfo);

        try (MockedStatic<EsRestClient> mockedStatic = mockStatic(EsRestClient.class)) {
            mockedStatic.when(() -> EsRestClient.createInstance(any())).thenReturn(mockClient);

            // Dynamic index with placeholder should skip checkIndexExist
            TableSinkFactoryContext context = createSinkContext("seatunnel_${age}");
            Assertions.assertDoesNotThrow(() -> factory.validateConnectionForDryRun(context));

            verify(mockClient).getClusterInfo();
            // checkIndexExist should NOT be called for dynamic index names
            verify(mockClient, never()).checkIndexExist(any());
        }
    }

    @Test
    void testValidateConnectionChecksIndexForStaticIndex() throws Exception {
        ElasticsearchSinkFactory factory = new ElasticsearchSinkFactory();
        EsRestClient mockClient = mock(EsRestClient.class);
        ElasticsearchClusterInfo clusterInfo =
                ElasticsearchClusterInfo.builder()
                        .distribution("elasticsearch")
                        .clusterVersion("8.10.0")
                        .build();
        when(mockClient.getClusterInfo()).thenReturn(clusterInfo);
        when(mockClient.checkIndexExist("static_index")).thenReturn(true);

        try (MockedStatic<EsRestClient> mockedStatic = mockStatic(EsRestClient.class)) {
            mockedStatic.when(() -> EsRestClient.createInstance(any())).thenReturn(mockClient);

            TableSinkFactoryContext context = createSinkContext("static_index");
            Assertions.assertDoesNotThrow(() -> factory.validateConnectionForDryRun(context));

            verify(mockClient).checkIndexExist("static_index");
        }
    }

    @Test
    void testValidateConnectionFailsWhenIndexCheckThrows() {
        ElasticsearchSinkFactory factory = new ElasticsearchSinkFactory();
        EsRestClient mockClient = mock(EsRestClient.class);
        ElasticsearchClusterInfo clusterInfo =
                ElasticsearchClusterInfo.builder()
                        .distribution("elasticsearch")
                        .clusterVersion("8.10.0")
                        .build();
        when(mockClient.getClusterInfo()).thenReturn(clusterInfo);
        when(mockClient.checkIndexExist("forbidden_index"))
                .thenThrow(new RuntimeException("403 Forbidden"));

        try (MockedStatic<EsRestClient> mockedStatic = mockStatic(EsRestClient.class)) {
            mockedStatic.when(() -> EsRestClient.createInstance(any())).thenReturn(mockClient);

            TableSinkFactoryContext context = createSinkContext("forbidden_index");
            SeaTunnelException exception =
                    Assertions.assertThrows(
                            SeaTunnelException.class,
                            () -> factory.validateConnectionForDryRun(context));
            Assertions.assertTrue(
                    exception.getMessage().contains("403 Forbidden"),
                    "Actual: " + exception.getMessage());
        }
    }

    @Test
    void testIndexVariablePrefixConstant() {
        Assertions.assertEquals("${", ElasticsearchSinkOptions.INDEX_VARIABLE_PREFIX);
    }

    @Test
    void testDynamicIndexDetection() {
        // Verify that index names with ${...} are correctly identified as dynamic
        String dynamicIndex = "seatunnel_${age}";
        Assertions.assertTrue(
                dynamicIndex.contains(ElasticsearchSinkOptions.INDEX_VARIABLE_PREFIX));

        String staticIndex = "seatunnel_data";
        Assertions.assertFalse(
                staticIndex.contains(ElasticsearchSinkOptions.INDEX_VARIABLE_PREFIX));

        String anotherDynamic = "logs_${date}_${region}";
        Assertions.assertTrue(
                anotherDynamic.contains(ElasticsearchSinkOptions.INDEX_VARIABLE_PREFIX));
    }

    private TableSinkFactoryContext createSinkContext(String indexName) {
        Map<String, Object> config = new HashMap<>();
        config.put("hosts", Arrays.asList("localhost:9200"));
        config.put("index", indexName);
        config.put("schema_save_mode", "CREATE_SCHEMA_WHEN_NOT_EXIST");
        config.put("data_save_mode", "APPEND_DATA");

        CatalogTable upstreamTable =
                CatalogTableUtil.getCatalogTable(
                        "elasticsearch",
                        "default",
                        null,
                        "upstream_table",
                        new SeaTunnelRowType(
                                new String[] {"id", "name"},
                                new SeaTunnelDataType<?>[] {
                                    BasicType.INT_TYPE, BasicType.STRING_TYPE
                                }));

        return new TableSinkFactoryContext(
                upstreamTable,
                ReadonlyConfig.fromMap(config),
                Thread.currentThread().getContextClassLoader());
    }
}
