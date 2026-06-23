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

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.net.URL;
import java.nio.file.Paths;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
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
        Assertions.assertEquals(11, rows.size());
        for (SeaTunnelRow row : rows) {
            LocalDate date = (LocalDate) row.getField(0);
            LocalDateTime datetime = (LocalDateTime) row.getField(1);
            LocalTime time = (LocalTime) row.getField(2);
            Assertions.assertNotNull(date);
            Assertions.assertNotNull(datetime);
            Assertions.assertNotNull(time);
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
