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
import org.apache.seatunnel.shade.org.apache.commons.lang3.tuple.Pair;

import org.apache.seatunnel.api.source.Collector;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.connectors.seatunnel.file.util.LocalFileSystemConf;

import org.apache.hadoop.conf.Configuration;
import org.apache.orc.OrcFile;
import org.apache.orc.Reader;
import org.apache.orc.RecordReader;
import org.apache.orc.TypeDescription;
import org.apache.orc.storage.ql.exec.vector.ColumnVector;
import org.apache.orc.storage.ql.exec.vector.VectorizedRowBatch;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import lombok.extern.slf4j.Slf4j;

import java.io.File;
import java.io.FileNotFoundException;
import java.net.URL;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.FS_DEFAULT_NAME_DEFAULT;

@Slf4j
public class OrcReadStrategyTest {

    @Test
    public void testOrcRead() throws Exception {
        URL orcFile = OrcReadStrategyTest.class.getResource("/test.orc");
        Assertions.assertNotNull(orcFile);
        String orcFilePath = Paths.get(orcFile.toURI()).toString();
        OrcReadStrategy orcReadStrategy = new OrcReadStrategy();
        LocalFileSystemConf.LocalConf localConf =
                new LocalFileSystemConf.LocalConf(FS_DEFAULT_NAME_DEFAULT);
        orcReadStrategy.init(localConf);
        TestCollector testCollector = new TestCollector();
        SeaTunnelRowType seaTunnelRowTypeInfo =
                orcReadStrategy.getSeaTunnelRowTypeInfo(orcFilePath);
        Assertions.assertNotNull(seaTunnelRowTypeInfo);
        log.info(seaTunnelRowTypeInfo.toString());
        orcReadStrategy.read(orcFilePath, "", testCollector);
        for (SeaTunnelRow row : testCollector.getRows()) {
            Assertions.assertEquals(row.getField(0).getClass(), Boolean.class);
            Assertions.assertEquals(row.getField(1).getClass(), Byte.class);
            Assertions.assertEquals(row.getField(16).getClass(), SeaTunnelRow.class);
        }
    }

    @Test
    public void testReadNotExistedFile() throws Exception {
        OrcReadStrategy orcReadStrategy = new OrcReadStrategy();
        LocalFileSystemConf.LocalConf localConf =
                new LocalFileSystemConf.LocalConf(FS_DEFAULT_NAME_DEFAULT);
        orcReadStrategy.init(localConf);
        Exception exception =
                Assertions.assertThrows(
                        Exception.class,
                        () -> orcReadStrategy.getSeaTunnelRowTypeInfo("not_existed_file.orc"));
        Assertions.assertInstanceOf(FileNotFoundException.class, exception.getCause());
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
        LocalFileSystemConf.LocalConf localConf =
                new LocalFileSystemConf.LocalConf(FS_DEFAULT_NAME_DEFAULT);
        Config pluginConfig = ConfigFactory.parseFile(new File(confPath));
        orcReadStrategy.init(localConf);
        orcReadStrategy.setPluginConfig(pluginConfig);
        TestCollector testCollector = new TestCollector();
        SeaTunnelRowType seaTunnelRowTypeInfo =
                orcReadStrategy.getSeaTunnelRowTypeInfo(orcFilePath);
        Assertions.assertNotNull(seaTunnelRowTypeInfo);
        log.info(seaTunnelRowTypeInfo.toString());
        orcReadStrategy.read(orcFilePath, "", testCollector);
        for (SeaTunnelRow row : testCollector.getRows()) {
            Assertions.assertEquals(row.getField(0).getClass(), Byte.class);
            Assertions.assertEquals(row.getField(1).getClass(), Boolean.class);
        }
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testReadRealOrcFileWithReflection() throws Exception {
        URL orcFile = OrcReadStrategyTest.class.getResource("/test_binary_and_union.orc");
        Assertions.assertNotNull(orcFile, "Missing test_binary_and_union.orc in resources");
        String orcFilePath = Paths.get(orcFile.toURI()).toString();

        Configuration conf = new Configuration();
        Reader reader =
                org.apache.orc.OrcFile.createReader(
                        new org.apache.hadoop.fs.Path(orcFilePath), OrcFile.readerOptions(conf));

        TypeDescription schema = reader.getSchema();
        RecordReader rows = reader.rows(reader.options().schema(schema));
        VectorizedRowBatch batch = schema.createRowBatch();

        Assertions.assertTrue(rows.nextBatch(batch), "Batch should not be empty");

        TypeDescription binaryType = schema.getChildren().get(0);
        TypeDescription unionType = schema.getChildren().get(1);

        OrcReadStrategy orcReadStrategy = new OrcReadStrategy();
        java.lang.reflect.Method readColumnMethod =
                OrcReadStrategy.class.getDeclaredMethod(
                        "readColumn",
                        ColumnVector.class,
                        TypeDescription.class,
                        org.apache.seatunnel.api.table.type.SeaTunnelDataType.class,
                        int.class,
                        java.nio.charset.Charset.class);
        readColumnMethod.setAccessible(true);
        java.nio.charset.Charset charset = java.nio.charset.StandardCharsets.UTF_8;

        Object row0Binary =
                readColumnMethod.invoke(
                        orcReadStrategy, batch.cols[0], binaryType, null, 0, charset);
        Assertions.assertInstanceOf(byte[].class, row0Binary, "BINARY column should return byte[]");

        byte[] expectedBinary =
                new byte[] {
                    (byte) 0xDE, (byte) 0xAD, (byte) 0xBE, (byte) 0xEF, (byte) 0x80, (byte) 0xFF
                };
        Assertions.assertArrayEquals(
                expectedBinary,
                (byte[]) row0Binary,
                "BINARY data should not be corrupted by String conversion!");

        Object row0Union =
                readColumnMethod.invoke(
                        orcReadStrategy, batch.cols[1], unionType, null, 0, charset);
        Pair<org.apache.orc.TypeDescription, Object> unionResult0 =
                (Pair<org.apache.orc.TypeDescription, Object>) row0Union;
        Assertions.assertEquals(
                org.apache.orc.TypeDescription.Category.STRING,
                unionResult0.getLeft().getCategory());
        Assertions.assertInstanceOf(
                String.class, unionResult0.getRight(), "UNION string branch should return String");
        Assertions.assertEquals("seatunnel_union_string", unionResult0.getRight());
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
}
