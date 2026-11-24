/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.seatunnel.engine.imap.storage.file.wal.writer;

import org.apache.seatunnel.engine.imap.storage.file.bean.IMapFileData;
import org.apache.seatunnel.engine.imap.storage.file.common.WALReader;
import org.apache.seatunnel.engine.imap.storage.file.config.FileConfiguration;
import org.apache.seatunnel.engine.serializer.protobuf.ProtoStuffSerializer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSOutputStream;
import org.apache.hadoop.hdfs.client.HdfsDataOutputStream;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledOnOs;
import org.junit.jupiter.api.io.TempDir;

import java.io.OutputStream;
import java.lang.reflect.Field;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.Map;

import static org.junit.jupiter.api.condition.OS.LINUX;
import static org.junit.jupiter.api.condition.OS.MAC;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Asserts method-call-count parity for {@link HdfsWriter#flush()}: exactly one hsync-family
 * invocation per branch.
 *
 * <p>This is not disk-sync-count parity and is not evidence that the reported checkpoint
 * state-store latency variance is resolved. {@code LocalFileSystem} {@code hsync}/{@code hflush}
 * may not map 1:1 to a device fsync; these mocks only lock the control-flow / call-count contract.
 * Use {@link HdfsWriterDurableFlushTest} for same-process read-back visibility (also not a crash
 * durability proof).
 */
class HdfsWriterFlushCallCountTest {

    private static final EnumSet<HdfsDataOutputStream.SyncFlag> UPDATE_LENGTH =
            EnumSet.of(HdfsDataOutputStream.SyncFlag.UPDATE_LENGTH);

    @Test
    void flushShouldCallHdfsDataOutputStreamHsyncOnce() throws Exception {
        HdfsDataOutputStream out = mock(HdfsDataOutputStream.class);
        HdfsWriter writer = writerWithOut(out);

        writer.flush();

        verify(out, times(1)).hsync(UPDATE_LENGTH);
        verify(out, never()).hsync();
        verify(out, never()).hflush();
    }

    @Test
    void flushShouldCallWrappedDfsOutputStreamHsyncOnce() throws Exception {
        DFSOutputStream dfs = mock(DFSOutputStream.class);
        FSDataOutputStream out = mock(FSDataOutputStream.class);
        when(out.getWrappedStream()).thenReturn(dfs);
        HdfsWriter writer = writerWithOut(out);

        writer.flush();

        // instanceof check + cast each call getWrappedStream() once.
        verify(out, times(2)).getWrappedStream();
        verify(dfs, times(1)).hsync(UPDATE_LENGTH);
        verify(out, never()).hsync();
        verify(out, never()).hflush();
        verify(dfs, never()).hflush();
    }

    @EnabledOnOs({LINUX, MAC})
    @Test
    void flushShouldExposeRecordsOnLocalFileSystem(@TempDir java.nio.file.Path tempDir)
            throws Exception {
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", "file:///");
        conf.set("fs.hdfs.impl", "org.apache.hadoop.fs.LocalFileSystem");
        FileSystem fs = privateLocalFileSystem(conf);
        fs.setWriteChecksum(false);
        ProtoStuffSerializer serializer = new ProtoStuffSerializer();
        Path parentPath = new Path(tempDir.resolve("wal").toUri());
        WALReader reader = new WALReader(fs, FileConfiguration.HDFS, serializer);

        try (HdfsWriter writer = new HdfsWriter()) {
            writer.initialize(fs, parentPath, serializer);
            writer.write(
                    IMapFileData.builder()
                            .key(serializer.serialize("flush-key"))
                            .keyClassName(String.class.getName())
                            .value(serializer.serialize("flush-value"))
                            .valueClassName(String.class.getName())
                            .timestamp(System.nanoTime())
                            .deleted(false)
                            .build());
            writer.flush();

            Map<Object, Object> loaded = reader.loadAllData(parentPath, new HashSet<>());
            Assertions.assertEquals(1, loaded.size());
            Assertions.assertEquals("flush-value", loaded.get("flush-key"));
        } finally {
            fs.setWriteChecksum(true);
            fs.close();
        }
    }

    /** A local file system that is not the shared instance returned by {@code getLocal()}. */
    private static FileSystem privateLocalFileSystem(Configuration conf) throws Exception {
        FileSystem fs = new org.apache.hadoop.fs.LocalFileSystem();
        fs.initialize(java.net.URI.create("file:///"), conf);
        return fs;
    }

    @Test
    void flushShouldCallPlainFsDataOutputStreamHsyncOnce() throws Exception {
        FSDataOutputStream out = mock(FSDataOutputStream.class);
        when(out.getWrappedStream()).thenReturn(mock(OutputStream.class));
        HdfsWriter writer = writerWithOut(out);

        writer.flush();

        verify(out, times(1)).getWrappedStream();
        verify(out, times(1)).hsync();
        verify(out, never()).hflush();
    }

    private static HdfsWriter writerWithOut(FSDataOutputStream out) throws Exception {
        HdfsWriter writer = new HdfsWriter();
        Field field = HdfsWriter.class.getDeclaredField("out");
        field.setAccessible(true);
        field.set(writer, out);
        return writer;
    }
}
