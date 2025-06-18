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

import org.apache.seatunnel.api.source.Collector;
import org.apache.seatunnel.api.table.catalog.CatalogTableUtil;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.connectors.seatunnel.file.config.HadoopConf;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import lombok.extern.slf4j.Slf4j;

import java.net.URL;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.FS_DEFAULT_NAME_DEFAULT;

@Slf4j
public class CsvReadStrategyTest {

    @Test
    public void testReadCsv() throws Exception {
        URL resource = CsvReadStrategyTest.class.getResource("/test.csv");
        String path = Paths.get(resource.toURI()).toString();
        CsvReadStrategy csvReadStrategy = new CsvReadStrategy();
        LocalConf localConf = new LocalConf(FS_DEFAULT_NAME_DEFAULT);
        csvReadStrategy.init(localConf);
        csvReadStrategy.getFileNamesByPath(path);
        csvReadStrategy.setPluginConfig(ConfigFactory.empty());
        csvReadStrategy.setCatalogTable(
                CatalogTableUtil.getCatalogTable(
                        "test",
                        new SeaTunnelRowType(
                                new String[] {"id", "name", "age"},
                                new SeaTunnelDataType[] {
                                    BasicType.INT_TYPE, BasicType.STRING_TYPE, BasicType.INT_TYPE
                                })));
        TestCollector testCollector = new TestCollector();
        csvReadStrategy.read(path, "", testCollector);

        Assertions.assertEquals(2, testCollector.getRows().size());
        Assertions.assertEquals(1, testCollector.getRows().get(0).getField(0));
        Assertions.assertEquals("a", testCollector.getRows().get(0).getField(1));
        Assertions.assertEquals(10, testCollector.getRows().get(0).getField(2));
        Assertions.assertEquals(2, testCollector.getRows().get(1).getField(0));
        Assertions.assertEquals("b", testCollector.getRows().get(1).getField(1));
        Assertions.assertEquals(100, testCollector.getRows().get(1).getField(2));
    }

    public static class TestCollector implements Collector<SeaTunnelRow> {

        private final List<SeaTunnelRow> rows = new ArrayList<>();

        public List<SeaTunnelRow> getRows() {
            return rows;
        }

        @Override
        public void collect(SeaTunnelRow record) {
            log.info(record.toString());
            rows.add(record);
        }

        @Override
        public Object getCheckpointLock() {
            return null;
        }
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
