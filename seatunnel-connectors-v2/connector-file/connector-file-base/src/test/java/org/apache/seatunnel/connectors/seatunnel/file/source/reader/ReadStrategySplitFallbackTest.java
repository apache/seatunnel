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

import org.apache.seatunnel.api.source.Collector;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.CatalogTableUtil;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.connectors.seatunnel.file.config.FileBaseSourceOptions;
import org.apache.seatunnel.connectors.seatunnel.file.source.split.FileSourceSplit;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ReadStrategySplitFallbackTest {

    private static final class ListCollector implements Collector<SeaTunnelRow> {
        private final List<SeaTunnelRow> rows;
        private final Object checkpointLock = new Object();

        private ListCollector(List<SeaTunnelRow> rows) {
            this.rows = rows;
        }

        @Override
        public void collect(SeaTunnelRow record) {
            rows.add(record);
        }

        @Override
        public Object getCheckpointLock() {
            return checkpointLock;
        }
    }

    @Test
    void testTextReadStrategyShouldSkipHeaderWhenEnableSplitButNoRangeInSplit() throws Exception {
        Map<String, Object> configMap = new HashMap<>();
        configMap.put(FileBaseSourceOptions.FILE_PATH.key(), "/tmp/test");
        configMap.put(FileBaseSourceOptions.ENABLE_FILE_SPLIT.key(), true);
        configMap.put(FileBaseSourceOptions.SKIP_HEADER_ROW_NUMBER.key(), 1L);
        Config pluginConfig = ConfigFactory.parseMap(configMap);

        SeaTunnelRowType rowType =
                new SeaTunnelRowType(
                        new String[] {"name"}, new SeaTunnelDataType[] {BasicType.STRING_TYPE});
        CatalogTable catalogTable = CatalogTableUtil.getCatalogTable("test", rowType);

        List<SeaTunnelRow> rows = new ArrayList<>();
        ListCollector collector = new ListCollector(rows);
        FileSourceSplit split = new FileSourceSplit("test", "/tmp/test/e2e.txt");

        try (TextReadStrategy strategy = new TextReadStrategy()) {
            strategy.setPluginConfig(pluginConfig);
            strategy.setCatalogTable(catalogTable);

            strategy.readProcess(
                    split,
                    collector,
                    new ByteArrayInputStream("name\na\n".getBytes(StandardCharsets.UTF_8)),
                    Collections.emptyMap(),
                    "e2e.txt");
        }

        Assertions.assertEquals(1, rows.size());
        Assertions.assertEquals("a", rows.get(0).getField(0));
    }

    @Test
    void testCsvReadStrategyShouldUseHeaderWhenEnableSplitButNoRangeInSplit() throws Exception {
        Map<String, Object> configMap = new HashMap<>();
        configMap.put(FileBaseSourceOptions.FILE_PATH.key(), "/tmp/test");
        configMap.put(FileBaseSourceOptions.ENABLE_FILE_SPLIT.key(), true);
        configMap.put(FileBaseSourceOptions.CSV_USE_HEADER_LINE.key(), true);
        Config pluginConfig = ConfigFactory.parseMap(configMap);

        SeaTunnelRowType rowType =
                new SeaTunnelRowType(
                        new String[] {"id", "name"},
                        new SeaTunnelDataType[] {BasicType.INT_TYPE, BasicType.STRING_TYPE});
        CatalogTable catalogTable = CatalogTableUtil.getCatalogTable("test", rowType);

        List<SeaTunnelRow> rows = new ArrayList<>();
        ListCollector collector = new ListCollector(rows);
        FileSourceSplit split = new FileSourceSplit("test", "/tmp/test/e2e.csv");

        try (CsvReadStrategy strategy = new CsvReadStrategy()) {
            strategy.setPluginConfig(pluginConfig);
            strategy.setCatalogTable(catalogTable);

            strategy.readProcess(
                    split,
                    collector,
                    new ByteArrayInputStream("id,name\n1,a\n".getBytes(StandardCharsets.UTF_8)),
                    Collections.emptyMap(),
                    "e2e.csv");
        }

        Assertions.assertEquals(1, rows.size());
        Assertions.assertEquals(1, rows.get(0).getField(0));
        Assertions.assertEquals("a", rows.get(0).getField(1));
    }
}
