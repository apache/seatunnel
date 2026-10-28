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

import org.apache.seatunnel.shade.com.typesafe.config.ConfigFactory;

import org.apache.seatunnel.api.table.catalog.CatalogTableUtil;
import org.apache.seatunnel.api.table.type.LocalTimeType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.connectors.seatunnel.file.config.FileBaseSourceOptions;
import org.apache.seatunnel.connectors.seatunnel.file.config.HadoopConf;
import org.apache.seatunnel.connectors.seatunnel.file.source.split.FileSourceSplit;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.Paths;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.FS_DEFAULT_NAME_DEFAULT;

public class TextReadStrategyTest {

    @Test
    public void testDatetimeStrAutoParse() throws Exception {
        URL resource = TextReadStrategyTest.class.getResource("/datetime_format.txt");
        String path = Paths.get(resource.toURI()).toString();
        TempCollector TempCollector = new TempCollector();
        try (TextReadStrategy TextReadStrategy = new TextReadStrategy()) {
            TextReadStrategyTest.LocalConf localConf =
                    new TextReadStrategyTest.LocalConf(FS_DEFAULT_NAME_DEFAULT);
            TextReadStrategy.init(localConf);
            TextReadStrategy.getFileNamesByPath(path);
            TextReadStrategy.setPluginConfig(
                    ConfigFactory.parseMap(getOptionsForSpecialQuoteChar()));
            TextReadStrategy.setCatalogTable(
                    CatalogTableUtil.getCatalogTable(
                            "test",
                            new SeaTunnelRowType(
                                    new String[] {"date", "datetime", "time"},
                                    new SeaTunnelDataType[] {
                                        LocalTimeType.LOCAL_DATE_TYPE,
                                        LocalTimeType.LOCAL_DATE_TIME_TYPE,
                                        LocalTimeType.LOCAL_TIME_TYPE,
                                    })));
            TextReadStrategy.read(path, "", TempCollector);
        }
        final List<SeaTunnelRow> rows = TempCollector.getRows();
        Assertions.assertEquals(10, rows.size());
        for (SeaTunnelRow row : rows) {
            LocalDate date = (LocalDate) row.getField(0);
            LocalDateTime datetime = (LocalDateTime) row.getField(1);
            LocalTime time = (LocalTime) row.getField(2);
            Assertions.assertNotNull(date);
            Assertions.assertNotNull(datetime);
            Assertions.assertNotNull(time);
        }
    }

    /**
     * Keeps raw text intact with default or configured field separators and optional partitions.
     *
     * <p>Custom row delimiters and decoding must still use the shared configuration even though
     * schema-less content must not be split into fields.
     */
    @Test
    public void testSchemaLessTextPreservesWholeLine() throws Exception {
        String path = "/input/day=2026-01-01/raw.txt";
        List<String> lines = Arrays.asList("a,b,c\u0001tail||end", ",先頭,\u0001尾||");
        String rowDelimiter = "<ROW>";
        byte[] input = String.join(rowDelimiter, lines).getBytes(StandardCharsets.UTF_16LE);
        for (boolean mergePartitions : new boolean[] {false, true}) {
            for (String fieldDelimiter : new String[] {null, ",", "||"}) {
                Map<String, Object> options = new HashMap<>();
                options.put(FileBaseSourceOptions.PARSE_PARTITION_FROM_PATH.key(), mergePartitions);
                options.put(FileBaseSourceOptions.ROW_DELIMITER.key(), rowDelimiter);
                options.put(FileBaseSourceOptions.ENCODING.key(), StandardCharsets.UTF_16LE.name());
                if (fieldDelimiter != null) {
                    options.put(FileBaseSourceOptions.FIELD_DELIMITER.key(), fieldDelimiter);
                }
                TempCollector output = new TempCollector();
                try (TextReadStrategy strategy = new TextReadStrategy()) {
                    strategy.setPluginConfig(ConfigFactory.parseMap(options));
                    strategy.getSeaTunnelRowTypeInfo(path);
                    strategy.readProcess(
                            new FileSourceSplit("raw-text-table", path),
                            output,
                            new ByteArrayInputStream(input),
                            mergePartitions
                                    ? Collections.singletonMap("day", "2026-01-01")
                                    : Collections.emptyMap(),
                            "raw.txt");
                }
                Assertions.assertEquals(lines.size(), output.getRows().size());
                for (int i = 0; i < lines.size(); i++) {
                    SeaTunnelRow row = output.getRows().get(i);
                    Assertions.assertEquals(mergePartitions ? 2 : 1, row.getArity());
                    Assertions.assertEquals(lines.get(i), row.getField(0));
                    Assertions.assertEquals("raw-text-table", row.getTableId());
                    if (mergePartitions) {
                        Assertions.assertEquals("2026-01-01", row.getField(1));
                    }
                }
            }
        }
    }

    private Map<String, Object> getOptionsForSpecialQuoteChar() {
        Map<String, Object> map = new HashMap<>();
        map.put(FileBaseSourceOptions.QUOTE_CHAR.key(), "`");
        map.put(FileBaseSourceOptions.ESCAPE_CHAR.key(), "\"");
        map.put(FileBaseSourceOptions.FIELD_DELIMITER.key(), ",");
        return map;
    }

    public static class LocalConf extends HadoopConf {
        private static final String HDFS_IMPL = "org.apache.hadoop.fs.LocalFileSystem";
        private static final String SCHEMA = "file";

        public LocalConf(String hdfsNameKey) {
            super(hdfsNameKey);
        }

        @Override
        public String getFsHdfsImpl() {
            return HDFS_IMPL;
        }

        @Override
        public String getSchema() {
            return SCHEMA;
        }
    }
}
