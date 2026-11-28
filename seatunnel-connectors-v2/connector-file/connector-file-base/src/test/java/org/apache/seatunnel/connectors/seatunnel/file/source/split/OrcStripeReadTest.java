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

package org.apache.seatunnel.connectors.seatunnel.file.source.split;

import org.apache.seatunnel.shade.com.typesafe.config.ConfigFactory;

import org.apache.seatunnel.api.source.Collector;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.connectors.seatunnel.file.config.HadoopConf;
import org.apache.seatunnel.connectors.seatunnel.file.source.reader.OrcReadStrategy;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.orc.CompressionKind;
import org.apache.orc.OrcFile;
import org.apache.orc.Reader;
import org.apache.orc.StripeInformation;
import org.apache.orc.TypeDescription;
import org.apache.orc.Writer;
import org.apache.orc.storage.ql.exec.vector.BytesColumnVector;
import org.apache.orc.storage.ql.exec.vector.DecimalColumnVector;
import org.apache.orc.storage.ql.exec.vector.LongColumnVector;
import org.apache.orc.storage.ql.exec.vector.VectorizedRowBatch;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class OrcStripeReadTest {
    @TempDir java.nio.file.Path directory;

    public static class TestLocalFileSystem extends org.apache.hadoop.fs.RawLocalFileSystem {
        @Override
        protected java.io.OutputStream createOutputStreamWithMode(
                Path path, boolean append, org.apache.hadoop.fs.permission.FsPermission permission)
                throws java.io.IOException {
            return new java.io.FileOutputStream(pathToFile(path), append);
        }
    }

    private Configuration configuration() {
        Configuration conf = new Configuration();
        conf.set("fs.file.impl", TestLocalFileSystem.class.getName());
        conf.setBoolean("fs.file.impl.disable.cache", true);
        return conf;
    }

    public static class LocalConf extends HadoopConf {
        public LocalConf() {
            super("file:///");
        }

        @Override
        public String getSchema() {
            return "file";
        }

        @Override
        public String getFsHdfsImpl() {
            return TestLocalFileSystem.class.getName();
        }
    }

    private Path createFile(CompressionKind codec) throws Exception {
        java.nio.file.Path partition =
                java.nio.file.Files.createDirectories(directory.resolve("dt=20261026"));
        Path path = new Path(partition.resolve(codec.name() + ".orc").toUri());
        TypeDescription schema =
                TypeDescription.fromString("struct<id:bigint,name:string,amount:decimal(12,2)>");
        try (Writer writer =
                OrcFile.createWriter(
                        path,
                        OrcFile.writerOptions(configuration()).setSchema(schema).compress(codec))) {
            for (int stripe = 0; stripe < 6; stripe++) {
                VectorizedRowBatch batch = schema.createRowBatch(128);
                batch.size = 128;
                for (int i = 0; i < batch.size; i++) {
                    ((LongColumnVector) batch.cols[0]).vector[i] = stripe * 128 + i;
                    byte[] value = ("中文-" + (i % 7)).getBytes(StandardCharsets.UTF_8);
                    ((BytesColumnVector) batch.cols[1]).setVal(i, value);
                    if (i % 5 == 0) {
                        batch.cols[1].noNulls = false;
                        batch.cols[1].isNull[i] = true;
                    }
                    ((DecimalColumnVector) batch.cols[2]).vector[i].setFromLong(i);
                }
                writer.addRowBatch(batch);
                writer.writeIntermediateFooter();
            }
        }
        return path;
    }

    private List<List<Object>> read(Path path, FileSourceSplit split, boolean projection)
            throws Exception {
        List<List<Object>> result = new ArrayList<>();
        try (OrcReadStrategy strategy = new OrcReadStrategy()) {
            strategy.setPluginConfig(
                    ConfigFactory.parseString(
                            "enable_file_split=true"
                                    + (projection ? "\nread_columns=[name,id]" : "")));
            strategy.init(new LocalConf());
            strategy.getSeaTunnelRowTypeInfo(path.toString());
            strategy.read(
                    split,
                    new Collector<SeaTunnelRow>() {
                        public void collect(SeaTunnelRow row) {
                            assertEquals("ads.test", row.getTableId());
                            List<Object> values = new ArrayList<>();
                            for (int i = 0; i < row.getArity(); i++) values.add(row.getField(i));
                            result.add(values);
                        }

                        public Object getCheckpointLock() {
                            return result;
                        }
                    });
        }
        return result;
    }

    @Test
    void stripeGroupsRespectBoundsAndOversizedStripes() throws Exception {
        List<StripeInformation> stripes = new ArrayList<>();
        for (long[] bounds : new long[][] {{3, 100}, {103, 150}, {253, 500}, {753, 20}}) {
            StripeInformation stripe = org.mockito.Mockito.mock(StripeInformation.class);
            org.mockito.Mockito.when(stripe.getOffset()).thenReturn(bounds[0]);
            org.mockito.Mockito.when(stripe.getLength()).thenReturn(bounds[1]);
            stripes.add(stripe);
        }
        try (OrcFileSplitStrategy strategy = new OrcFileSplitStrategy(250, new LocalConf())) {
            List<FileSourceSplit> splits = strategy.splitByStripes("ads.test", "test.orc", stripes);
            assertEquals(3, splits.size());
            assertEquals(3, splits.get(0).getStart());
            assertEquals(250, splits.get(0).getLength());
            assertEquals(253, splits.get(1).getStart());
            assertEquals(500, splits.get(1).getLength());
            assertEquals(753, splits.get(2).getStart());
            assertEquals(20, splits.get(2).getLength());
        }
    }

    @Test
    void rangeReadsOnlyTheSelectedStripe() throws Exception {
        for (CompressionKind codec : Arrays.asList(CompressionKind.NONE, CompressionKind.SNAPPY)) {
            Path path = createFile(codec);
            try (Reader reader =
                    OrcFile.createReader(path, OrcFile.readerOptions(configuration()))) {
                assertEquals(6, reader.getStripes().size());
                StripeInformation stripe = reader.getStripes().get(2);
                List<List<Object>> rows =
                        read(
                                path,
                                new FileSourceSplit(
                                        "ads.test",
                                        path.toString(),
                                        stripe.getOffset(),
                                        stripe.getLength()),
                                false);
                assertEquals(stripe.getNumberOfRows(), rows.size());
                assertEquals(256L, rows.get(0).get(0));
                assertEquals("20261026", rows.get(0).get(3));
            }
        }
    }

    @Test
    void splitAndWholeReadsHaveExactlyTheSameRows() throws Exception {
        for (CompressionKind codec : Arrays.asList(CompressionKind.NONE, CompressionKind.SNAPPY)) {
            Path path = createFile(codec);
            for (boolean projection : Arrays.asList(false, true)) {
                List<List<Object>> whole =
                        read(path, new FileSourceSplit("ads.test", path.toString()), projection);
                assertEquals(768, whole.size());
                for (long size : new long[] {1, 2000, Long.MAX_VALUE}) {
                    List<List<Object>> actual = new ArrayList<>();
                    try (OrcFileSplitStrategy strategy =
                            new OrcFileSplitStrategy(size, new LocalConf())) {
                        List<FileSourceSplit> splits = strategy.split("ads.test", path.toString());
                        assertFalse(splits.isEmpty());
                        if (size == 1) assertEquals(6, splits.size());
                        if (size == Long.MAX_VALUE) assertEquals(1, splits.size());
                        Set<String> ids = new HashSet<>();
                        for (FileSourceSplit split : splits) {
                            assertTrue(ids.add(split.splitId()));
                            // Checkpoint serialization must preserve the range and split identity.
                            java.io.ByteArrayOutputStream bytes =
                                    new java.io.ByteArrayOutputStream();
                            try (java.io.ObjectOutputStream out =
                                    new java.io.ObjectOutputStream(bytes)) {
                                out.writeObject(split);
                            }
                            FileSourceSplit restored;
                            try (java.io.ObjectInputStream in =
                                    new java.io.ObjectInputStream(
                                            new java.io.ByteArrayInputStream(
                                                    bytes.toByteArray()))) {
                                restored = (FileSourceSplit) in.readObject();
                            }
                            assertEquals(split, restored);
                            actual.addAll(read(path, restored, projection));
                        }
                    }
                    assertEquals(whole, actual);
                }
            }
        }
    }

    @Test
    void emptyOrcProducesNoSplits() throws Exception {
        Path path = new Path(directory.resolve("empty.orc").toUri());
        try (Writer writer =
                OrcFile.createWriter(
                        path,
                        OrcFile.writerOptions(configuration())
                                .setSchema(TypeDescription.fromString("struct<id:bigint>")))) {
            // A valid ORC file with zero stripes.
        }
        try (OrcFileSplitStrategy strategy = new OrcFileSplitStrategy(64, new LocalConf())) {
            assertTrue(strategy.split("ads.test", path.toString()).isEmpty());
        }
        assertThrows(RuntimeException.class, () -> new OrcFileSplitStrategy(0, new LocalConf()));
    }

    @Test
    void snappyFooterAndRowsAreReadable() throws Exception {
        Path path = createFile(CompressionKind.SNAPPY);
        try (Reader reader = OrcFile.createReader(path, OrcFile.readerOptions(configuration()))) {
            assertEquals(CompressionKind.SNAPPY, reader.getCompressionKind());
            assertEquals(6, reader.getStripes().size());
        }
        List<List<Object>> rows =
                read(path, new FileSourceSplit("ads.test", path.toString()), false);
        assertEquals(768, rows.size());
        for (int i = 0; i < rows.size(); i++) {
            assertEquals((long) i, rows.get(i).get(0));
            assertEquals("20261026", rows.get(i).get(3));
        }
    }
}
