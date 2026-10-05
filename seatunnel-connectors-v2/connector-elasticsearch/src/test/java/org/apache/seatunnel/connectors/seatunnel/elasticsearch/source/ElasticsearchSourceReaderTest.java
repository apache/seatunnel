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

package org.apache.seatunnel.connectors.seatunnel.elasticsearch.source;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.source.Collector;
import org.apache.seatunnel.api.source.SourceReader;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.PhysicalColumn;
import org.apache.seatunnel.api.table.catalog.TableIdentifier;
import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.connectors.seatunnel.elasticsearch.client.EsRestClient;
import org.apache.seatunnel.connectors.seatunnel.elasticsearch.config.ElasticsearchConfig;
import org.apache.seatunnel.connectors.seatunnel.elasticsearch.config.SearchTypeEnum;
import org.apache.seatunnel.connectors.seatunnel.elasticsearch.dto.source.ScrollResult;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.util.Collections;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class ElasticsearchSourceReaderTest {

    @Test
    public void testCloseSqlCursorWhenSearchWithSqlThrowsException() throws Exception {
        SourceReader.Context context = mock(SourceReader.Context.class);
        ElasticsearchSourceReader reader =
                new ElasticsearchSourceReader(
                        context, ReadonlyConfig.fromMap(Collections.emptyMap()));

        EsRestClient esRestClient = mock(EsRestClient.class);
        injectEsRestClient(reader, esRestClient);

        ScrollResult firstPage = new ScrollResult();
        firstPage.setScrollId("cursor-1");
        firstPage.setDocs(Collections.emptyList());

        when(esRestClient.searchBySql(anyString(), anyInt())).thenReturn(firstPage);
        when(esRestClient.searchWithSql(eq("cursor-1"), any()))
                .thenThrow(new RuntimeException("mock sql scroll exception"));

        reader.addSplits(
                Collections.singletonList(new ElasticsearchSourceSplit("0", createSqlSplit())));

        Collector<SeaTunnelRow> collector = mock(Collector.class);
        when(collector.getCheckpointLock()).thenReturn(new Object());

        Assertions.assertThrows(RuntimeException.class, () -> reader.pollNext(collector));

        verify(esRestClient).closeSqlCursor("cursor-1");
    }

    private static ElasticsearchConfig createSqlSplit() {
        ElasticsearchConfig config = new ElasticsearchConfig();
        config.setIndex("test_index");
        config.setSqlQuery("SELECT id FROM test_index");
        config.setScrollSize(100);
        config.setSearchType(SearchTypeEnum.SQL);
        config.setSource(Collections.singletonList("id"));
        config.setCatalogTable(createCatalogTable());
        return config;
    }

    private static CatalogTable createCatalogTable() {
        return CatalogTable.of(
                TableIdentifier.of("test_catalog", "test_database", "test_table"),
                TableSchema.builder()
                        .column(
                                PhysicalColumn.of(
                                        "id", BasicType.INT_TYPE, (Long) null, true, null, ""))
                        .build(),
                Collections.emptyMap(),
                Collections.emptyList(),
                "");
    }

    private static void injectEsRestClient(ElasticsearchSourceReader reader, EsRestClient client)
            throws Exception {
        Field field = ElasticsearchSourceReader.class.getDeclaredField("esRestClient");
        field.setAccessible(true);
        field.set(reader, client);
    }
}
