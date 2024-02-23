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

package org.apache.seatunnel.connectors.seatunnel.elasticsearch.catalog;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.table.catalog.Catalog;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.InfoPreviewResult;
import org.apache.seatunnel.api.table.catalog.PhysicalColumn;
import org.apache.seatunnel.api.table.catalog.PreviewResult;
import org.apache.seatunnel.api.table.catalog.TableIdentifier;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.api.table.type.BasicType;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.Optional;

public class PreviewActionTest {

    private static final CatalogTable CATALOG_TABLE =
            CatalogTable.of(
                    TableIdentifier.of("catalog", "database", "table"),
                    TableSchema.builder()
                            .column(
                                    PhysicalColumn.of(
                                            "test",
                                            BasicType.STRING_TYPE,
                                            (Long) null,
                                            true,
                                            null,
                                            ""))
                            .build(),
                    Collections.emptyMap(),
                    Collections.emptyList(),
                    "comment");

    @Test
    public void testElasticSearchPreviewAction() {
        ElasticSearchCatalogFactory factory = new ElasticSearchCatalogFactory();
        Catalog catalog = factory.createCatalog("test", ReadonlyConfig.fromMap(new HashMap<>()));
        assertPreviewResult(
                catalog,
                Catalog.ActionType.CREATE_DATABASE,
                "create index testtable",
                Optional.empty());
        assertPreviewResult(
                catalog,
                Catalog.ActionType.DROP_DATABASE,
                "delete index testtable",
                Optional.empty());
        assertPreviewResult(
                catalog,
                Catalog.ActionType.TRUNCATE_TABLE,
                "delete and create index testtable",
                Optional.empty());
        assertPreviewResult(
                catalog, Catalog.ActionType.DROP_TABLE, "delete index testtable", Optional.empty());
        assertPreviewResult(
                catalog,
                Catalog.ActionType.CREATE_TABLE,
                "create index testtable",
                Optional.of(CATALOG_TABLE));
    }

    private void assertPreviewResult(
            Catalog catalog,
            Catalog.ActionType actionType,
            String expectedSql,
            Optional<CatalogTable> catalogTable) {
        PreviewResult previewResult =
                catalog.previewAction(
                        actionType, TablePath.of("testddatabase.testtable"), catalogTable);
        Assertions.assertInstanceOf(InfoPreviewResult.class, previewResult);
        Assertions.assertEquals(expectedSql, ((InfoPreviewResult) previewResult).getInfo());
    }
}
