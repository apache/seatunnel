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
import org.apache.seatunnel.connectors.seatunnel.file.source.reader.ParquetReadStrategy;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.net.URL;
import java.nio.file.Paths;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.TimeZone;

import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.FS_DEFAULT_NAME_DEFAULT;

public class ParquetReadStrategyTest {
    @Test
    public void testParquetRead1() throws Exception {
        URL resource = ParquetReadStrategyTest.class.getResource("/timestamp_as_int64.parquet");
        Assertions.assertNotNull(resource);
        String path = Paths.get(resource.toURI()).toString();
        ParquetReadStrategy parquetReadStrategy = new ParquetReadStrategy();
        LocalConf localConf = new LocalConf(FS_DEFAULT_NAME_DEFAULT);
        parquetReadStrategy.init(localConf);
        SeaTunnelRowType seaTunnelRowTypeInfo =
                parquetReadStrategy.getSeaTunnelRowTypeInfo(localConf, path);
        Assertions.assertNotNull(seaTunnelRowTypeInfo);
        System.out.println(seaTunnelRowTypeInfo);
        TestCollector testCollector = new TestCollector();
        parquetReadStrategy.read(path, testCollector);
    }

    @Test
    public void testParquetRead2() throws Exception {
        URL resource = ParquetReadStrategyTest.class.getResource("/hive.parquet");
        Assertions.assertNotNull(resource);
        String path = Paths.get(resource.toURI()).toString();
        ParquetReadStrategy parquetReadStrategy = new ParquetReadStrategy();
        LocalConf localConf = new LocalConf(FS_DEFAULT_NAME_DEFAULT);
        parquetReadStrategy.init(localConf);
        SeaTunnelRowType seaTunnelRowTypeInfo =
                parquetReadStrategy.getSeaTunnelRowTypeInfo(localConf, path);
        Assertions.assertNotNull(seaTunnelRowTypeInfo);
        System.out.println(seaTunnelRowTypeInfo);
        TestCollector testCollector = new TestCollector();
        parquetReadStrategy.read(path, testCollector);
    }

    @Test
    public void testParquetReadUseSystemDefaultTimeZone() throws Exception {
        URL resource = ParquetReadStrategyTest.class.getResource("/timestamp_as_int64.parquet");
        Assertions.assertNotNull(resource);
        String path = Paths.get(resource.toURI()).toString();
        ParquetReadStrategy parquetReadStrategy = new ParquetReadStrategy();
        LocalConf localConf = new LocalConf(FS_DEFAULT_NAME_DEFAULT);
        parquetReadStrategy.init(localConf);
        SeaTunnelRowType seaTunnelRowTypeInfo =
                parquetReadStrategy.getSeaTunnelRowTypeInfo(localConf, path);
        Assertions.assertNotNull(seaTunnelRowTypeInfo);
        System.out.println(seaTunnelRowTypeInfo);
        int index = seaTunnelRowTypeInfo.indexOf("c_timestamp");
        TimeZone tz1 = TimeZone.getTimeZone("Asia/Shanghai");
        TimeZone.setDefault(tz1);
        TestCollector testCollector = new TestCollector();
        parquetReadStrategy.read(path, testCollector);
        LocalDateTime time1 = (LocalDateTime) testCollector.getRows().get(0).getField(index);

        TimeZone tz2 = TimeZone.getTimeZone("UTC");
        TimeZone.setDefault(tz2);
        TestCollector testCollector2 = new TestCollector();
        parquetReadStrategy.read(path, testCollector2);
        LocalDateTime time2 = (LocalDateTime) testCollector2.getRows().get(0).getField(index);

        Assertions.assertTrue(time1.isAfter(time2));
        Assertions.assertEquals(
                time1.atZone(tz1.toZoneId()).withZoneSameInstant(tz2.toZoneId()).toLocalDateTime(),
                time2);
    }

    @Test
    public void testParquetReadProjection1() throws Exception {
        URL resource = ParquetReadStrategyTest.class.getResource("/timestamp_as_int96.parquet");
        URL conf = OrcReadStrategyTest.class.getResource("/test_read_parquet.conf");
        Assertions.assertNotNull(resource);
        Assertions.assertNotNull(conf);
        String path = Paths.get(resource.toURI()).toString();
        String confPath = Paths.get(conf.toURI()).toString();
        Config pluginConfig = ConfigFactory.parseFile(new File(confPath));
        ParquetReadStrategy parquetReadStrategy = new ParquetReadStrategy();
        LocalConf localConf = new LocalConf(FS_DEFAULT_NAME_DEFAULT);
        parquetReadStrategy.init(localConf);
        parquetReadStrategy.setPluginConfig(pluginConfig);
        SeaTunnelRowType seaTunnelRowTypeInfo =
                parquetReadStrategy.getSeaTunnelRowTypeInfo(localConf, path);
        Assertions.assertNotNull(seaTunnelRowTypeInfo);
        System.out.println(seaTunnelRowTypeInfo);
        TestCollector testCollector = new TestCollector();
        parquetReadStrategy.read(path, testCollector);
        List<SeaTunnelRow> rows = testCollector.getRows();
        for (SeaTunnelRow row : rows) {
            Assertions.assertEquals(row.getField(0).getClass(), Long.class);
            Assertions.assertEquals(row.getField(1).getClass(), Byte.class);
            Assertions.assertEquals(row.getField(2).getClass(), Short.class);
            Assertions.assertEquals(row.getField(0), 40000000000L);
            Assertions.assertEquals(row.getField(1), (byte) 1);
            Assertions.assertEquals(row.getField(2), (short) 1);
        }
    }

    @Test
    public void testParquetReadProjection2() throws Exception {
        URL resource = ParquetReadStrategyTest.class.getResource("/hive.parquet");
        URL conf = OrcReadStrategyTest.class.getResource("/test_read_parquet2.conf");
        Assertions.assertNotNull(resource);
        Assertions.assertNotNull(conf);
        String path = Paths.get(resource.toURI()).toString();
        String confPath = Paths.get(conf.toURI()).toString();
        Config pluginConfig = ConfigFactory.parseFile(new File(confPath));
        ParquetReadStrategy parquetReadStrategy = new ParquetReadStrategy();
        LocalConf localConf = new LocalConf(FS_DEFAULT_NAME_DEFAULT);
        parquetReadStrategy.init(localConf);
        parquetReadStrategy.setPluginConfig(pluginConfig);
        SeaTunnelRowType seaTunnelRowTypeInfo =
                parquetReadStrategy.getSeaTunnelRowTypeInfo(localConf, path);
        Assertions.assertNotNull(seaTunnelRowTypeInfo);
        System.out.println(seaTunnelRowTypeInfo);
        TestCollector testCollector = new TestCollector();
        parquetReadStrategy.read(path, testCollector);
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
