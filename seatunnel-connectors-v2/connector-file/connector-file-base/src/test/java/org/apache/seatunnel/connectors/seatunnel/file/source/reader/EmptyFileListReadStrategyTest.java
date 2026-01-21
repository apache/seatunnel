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

package org.apache.seatunnel.connectors.seatunnel.file.source.reader;

import org.apache.seatunnel.shade.com.typesafe.config.Config;
import org.apache.seatunnel.shade.com.typesafe.config.ConfigFactory;

import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.CatalogTableUtil;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.connectors.seatunnel.file.config.FileBaseSourceOptions;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

public class EmptyFileListReadStrategyTest {

    @Test
    public void testSetCatalogTableShouldNotThrowWhenFileListIsEmpty() {
        Config pluginConfig = ConfigFactory.parseMap(buildBasePluginConfigWithPartitions());
        CatalogTable catalogTable = buildCatalogTable();

        Assertions.assertAll(
                () ->
                        assertSetCatalogTableWithEmptyFileNames(
                                new TextReadStrategy(), pluginConfig, catalogTable),
                () ->
                        assertSetCatalogTableWithEmptyFileNames(
                                new CsvReadStrategy(), pluginConfig, catalogTable),
                () ->
                        assertSetCatalogTableWithEmptyFileNames(
                                new ExcelReadStrategy(), pluginConfig, catalogTable),
                () ->
                        assertSetCatalogTableWithEmptyFileNames(
                                new XmlReadStrategy(), pluginConfig, catalogTable),
                () ->
                        assertSetCatalogTableWithEmptyFileNames(
                                new JsonReadStrategy(), pluginConfig, catalogTable));
    }

    @Test
    public void testGetSeaTunnelRowTypeInfoShouldNotThrowWhenFileListIsEmpty() {
        Config pluginConfig = ConfigFactory.parseMap(buildBasePluginConfigWithPartitions());

        TextReadStrategy textReadStrategy = new TextReadStrategy();
        textReadStrategy.setPluginConfig(pluginConfig);
        SeaTunnelRowType textRowType =
                Assertions.assertDoesNotThrow(
                        () -> textReadStrategy.getSeaTunnelRowTypeInfo("/tmp/dt=2024-01-01"));
        Assertions.assertEquals(
                "dt", textRowType.getFieldNames()[textRowType.getTotalFields() - 1]);

        CsvReadStrategy csvReadStrategy = new CsvReadStrategy();
        csvReadStrategy.setPluginConfig(pluginConfig);
        SeaTunnelRowType csvRowType =
                Assertions.assertDoesNotThrow(
                        () -> csvReadStrategy.getSeaTunnelRowTypeInfo("/tmp/dt=2024-01-01"));
        Assertions.assertEquals("dt", csvRowType.getFieldNames()[csvRowType.getTotalFields() - 1]);
    }

    private static Map<String, Object> buildBasePluginConfigWithPartitions() {
        Map<String, Object> config = new HashMap<>();
        config.put(FileBaseSourceOptions.FILE_PATH.key(), "/tmp/dt=2024-01-01");
        return config;
    }

    private static CatalogTable buildCatalogTable() {
        SeaTunnelRowType rowType =
                new SeaTunnelRowType(
                        new String[] {"id"}, new SeaTunnelDataType[] {BasicType.INT_TYPE});
        return CatalogTableUtil.getCatalogTable("test", rowType);
    }

    private static void assertSetCatalogTableWithEmptyFileNames(
            ReadStrategy readStrategy, Config pluginConfig, CatalogTable catalogTable) {
        readStrategy.setPluginConfig(pluginConfig);
        Assertions.assertDoesNotThrow(() -> readStrategy.setCatalogTable(catalogTable));
        SeaTunnelRowType actualRowType = readStrategy.getActualSeaTunnelRowTypeInfo();
        Assertions.assertArrayEquals(new String[] {"id", "dt"}, actualRowType.getFieldNames());
    }
}
