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

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.hdfs.DFSOutputStream;
import org.apache.hadoop.hdfs.client.HdfsDataOutputStream;

import org.junit.jupiter.api.Test;

import java.io.OutputStream;
import java.lang.reflect.Field;
import java.util.EnumSet;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Asserts that {@link HdfsWriter#flush()} takes exactly one hsync-family path per call.
 *
 * <p>Unlike {@link HdfsWriterDurableFlushTest}, these checks do not rely on same-process read-back
 * visibility (which {@code hflush()} would also satisfy via the OS page cache). They fail if a
 * branch silently regresses to {@code hflush()}-only or stacks multiple sync calls.
 */
class HdfsWriterFlushSyncPathTest {

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
