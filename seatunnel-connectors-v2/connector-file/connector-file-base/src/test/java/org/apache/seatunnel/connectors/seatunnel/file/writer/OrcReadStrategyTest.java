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

package org.apache.seatunnel.connectors.seatunnel.file.writer;

import org.apache.seatunnel.shade.com.typesafe.config.Config;
import org.apache.seatunnel.shade.com.typesafe.config.ConfigFactory;

import org.apache.seatunnel.api.source.Collector;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.connectors.seatunnel.file.config.HadoopConf;
import org.apache.seatunnel.connectors.seatunnel.file.source.reader.OrcReadStrategy;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.net.URL;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.FS_DEFAULT_NAME_DEFAULT;

public class OrcReadStrategyTest {

    @Test
    public void testOrcRead() throws Exception {
        URL orcFile = OrcReadStrategyTest.class.getResource("/test.orc");
        Assertions.assertNotNull(orcFile);
        String orcFilePath = Paths.get(orcFile.toURI()).toString();
        OrcReadStrategy orcReadStrategy = new OrcReadStrategy();
        LocalConf localConf = new LocalConf(FS_DEFAULT_NAME_DEFAULT);
        orcReadStrategy.init(localConf);
        TestCollector testCollector = new TestCollector();
        SeaTunnelRowType seaTunnelRowTypeInfo =
                orcReadStrategy.getSeaTunnelRowTypeInfo(localConf, orcFilePath);
        Assertions.assertNotNull(seaTunnelRowTypeInfo);
        System.out.println(seaTunnelRowTypeInfo);
        orcReadStrategy.read(orcFilePath, testCollector);
        for (SeaTunnelRow row : testCollector.getRows()) {
            Assertions.assertEquals(row.getField(0).getClass(), Boolean.class);
            Assertions.assertEquals(row.getField(1).getClass(), Byte.class);
            Assertions.assertEquals(row.getField(16).getClass(), SeaTunnelRow.class);
        }
    }

    @Test
    public void testOrcReadProjection() throws Exception {
        URL orcFile = OrcReadStrategyTest.class.getResource("/test.orc");
        URL conf = OrcReadStrategyTest.class.getResource("/test_read_orc.conf");
        Assertions.assertNotNull(orcFile);
        Assertions.assertNotNull(conf);
        String orcFilePath = Paths.get(orcFile.toURI()).toString();
        String confPath = Paths.get(conf.toURI()).toString();
        OrcReadStrategy orcReadStrategy = new OrcReadStrategy();
        LocalConf localConf = new LocalConf(FS_DEFAULT_NAME_DEFAULT);
        Config pluginConfig = ConfigFactory.parseFile(new File(confPath));
        orcReadStrategy.init(localConf);
        orcReadStrategy.setPluginConfig(pluginConfig);
        TestCollector testCollector = new TestCollector();
        SeaTunnelRowType seaTunnelRowTypeInfo =
                orcReadStrategy.getSeaTunnelRowTypeInfo(localConf, orcFilePath);
        Assertions.assertNotNull(seaTunnelRowTypeInfo);
        System.out.println(seaTunnelRowTypeInfo);
        orcReadStrategy.read(orcFilePath, testCollector);
        for (SeaTunnelRow row : testCollector.getRows()) {
            Assertions.assertEquals(row.getField(0).getClass(), Byte.class);
            Assertions.assertEquals(row.getField(1).getClass(), Boolean.class);
        }
    }

    public static class TestCollector implements Collector<SeaTunnelRow> {

        private final List<SeaTunnelRow> rows = new ArrayList<>();

        public List<SeaTunnelRow> getRows() {
            return rows;
        }

        @Override
        public void collect(SeaTunnelRow record) {
            System.out.println(record);
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
