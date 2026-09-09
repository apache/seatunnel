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
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.connectors.seatunnel.file.config.ArchiveCompressFormat;
import org.apache.seatunnel.connectors.seatunnel.file.config.FileFormat;
import org.apache.seatunnel.connectors.seatunnel.file.config.HadoopConf;
import org.apache.seatunnel.connectors.seatunnel.file.hadoop.HadoopFileSystemProxy;
import org.apache.seatunnel.connectors.seatunnel.file.source.split.FileSourceSplit;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.PositionedReadable;
import org.apache.hadoop.fs.Seekable;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.zip.GZIPOutputStream;

import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.FS_DEFAULT_NAME_DEFAULT;

/**
 * Verifies that the GZ branch of {@link AbstractReadStrategy#resolveArchiveCompressedInputStream}
 * closes the underlying input stream (#10531). The fix wraps the {@code GzipCompressorInputStream}
 * in a try-with-resources, so after the read the stream backing it must have been closed.
 */
public class AbstractReadStrategyGzCloseTest {

    @Test
    public void gzCaseShouldCloseUnderlyingInputStream() throws Exception {
        byte[] gzipBytes = gzip("1,a,10\n2,b,100\n");
        CloseTrackingSeekableInputStream tracked = new CloseTrackingSeekableInputStream(gzipBytes);

        // The proxy returns our close-tracking stream so we can assert it is released afterwards.
        HadoopFileSystemProxy proxy = Mockito.mock(HadoopFileSystemProxy.class);
        Mockito.when(proxy.getInputStream(Mockito.anyString()))
                .thenReturn(new FSDataInputStream(tracked));

        CsvReadStrategy strategy = new CsvReadStrategy();
        strategy.init(new LocalConf(FS_DEFAULT_NAME_DEFAULT));
        strategy.setPluginConfig(ConfigFactory.empty());
        strategy.setCatalogTable(
                CatalogTableUtil.getCatalogTable(
                        "test",
                        new SeaTunnelRowType(
                                new String[] {"id", "name", "age"},
                                new SeaTunnelDataType[] {
                                    BasicType.INT_TYPE, BasicType.STRING_TYPE, BasicType.INT_TYPE
                                })));
        // Inject the mocked proxy and force the GZ archive path.
        strategy.hadoopFileSystemProxy = proxy;
        strategy.archiveCompressFormat = ArchiveCompressFormat.GZ;

        TempCollector collector = new TempCollector();
        FileSourceSplit split = new FileSourceSplit("test", "/data/test.csv.gz");

        // The GZ branch opens the stream before delegating to readProcess. Regardless of whether
        // downstream parsing completes, the try-with-resources must release the GZ stream; the old
        // code left it open. We therefore assert solely on the stream being closed.
        try {
            strategy.resolveArchiveCompressedInputStream(
                    split, collector, new HashMap<>(), FileFormat.CSV);
        } catch (Exception ignored) {
            // Reading may fail for reasons unrelated to resource handling; not relevant here.
        }

        Assertions.assertTrue(
                tracked.isClosed(), "GZ underlying input stream must be closed after the read");
    }

    private static byte[] gzip(String content) throws Exception {
        try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
                GZIPOutputStream gzip = new GZIPOutputStream(baos)) {
            gzip.write(content.getBytes(StandardCharsets.UTF_8));
            gzip.finish();
            return baos.toByteArray();
        }
    }

    /**
     * A {@link ByteArrayInputStream} that satisfies the {@link FSDataInputStream} contract
     * (Seekable + PositionedReadable) and records when it is closed. The GZ read path only reads
     * sequentially, so the positioned-read methods are never exercised.
     */
    private static final class CloseTrackingSeekableInputStream extends ByteArrayInputStream
            implements Seekable, PositionedReadable {

        private boolean closed = false;

        CloseTrackingSeekableInputStream(byte[] buf) {
            super(buf);
        }

        boolean isClosed() {
            return closed;
        }

        @Override
        public void close() throws java.io.IOException {
            closed = true;
            super.close();
        }

        @Override
        public void seek(long p) {
            this.pos = (int) p;
        }

        @Override
        public long getPos() {
            return this.pos;
        }

        @Override
        public boolean seekToNewSource(long targetPos) {
            return false;
        }

        @Override
        public int read(long position, byte[] buffer, int offset, int length) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void readFully(long position, byte[] buffer, int offset, int length) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void readFully(long position, byte[] buffer) {
            throw new UnsupportedOperationException();
        }
    }

    /** Local filesystem HadoopConf, mirroring the other read-strategy tests. */
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
