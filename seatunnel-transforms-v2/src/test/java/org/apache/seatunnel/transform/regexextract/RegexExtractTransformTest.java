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

package org.apache.seatunnel.transform.regexextract;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.Column;
import org.apache.seatunnel.api.table.catalog.PhysicalColumn;
import org.apache.seatunnel.api.table.catalog.TableIdentifier;
import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.api.table.type.BasicType;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

public class RegexExtractTransformTest {

    private CatalogTable catalogTable;

    @BeforeEach
    void setUp() {
        catalogTable =
                CatalogTable.of(
                        TableIdentifier.of("default", "default", "default", "test"),
                        TableSchema.builder()
                                .column(
                                        PhysicalColumn.of(
                                                "text", BasicType.STRING_TYPE, 1000, true, "", ""))
                                .column(
                                        PhysicalColumn.of(
                                                "id", BasicType.INT_TYPE, 0, true, "", ""))
                                .build(),
                        new HashMap<>(),
                        Arrays.asList(),
                        "");
    }

    @Test
    void testGetProducedCatalogTable() {
        Map<String, Object> configMap = new HashMap<>();
        configMap.put("source_field", "text");
        configMap.put("regex_pattern", "(\\w+)@(\\w+\\.\\w+)");
        configMap.put("output_fields", Arrays.asList("username", "domain"));

        ReadonlyConfig config = ReadonlyConfig.fromMap(configMap);
        RegexExtractTransformConfig transformConfig = RegexExtractTransformConfig.of(config);
        RegexExtractTransform transform = new RegexExtractTransform(transformConfig, catalogTable);

        CatalogTable outputTable = transform.getProducedCatalogTable();
        Column usernameColumn = outputTable.getTableSchema().getColumn("username");
        Column domainColumn = outputTable.getTableSchema().getColumn("domain");

        Assertions.assertEquals(BasicType.STRING_TYPE, usernameColumn.getDataType());
        Assertions.assertEquals(BasicType.STRING_TYPE, domainColumn.getDataType());
        Assertions.assertEquals(200, usernameColumn.getColumnLength());
        Assertions.assertEquals(200, domainColumn.getColumnLength());
    }
}
