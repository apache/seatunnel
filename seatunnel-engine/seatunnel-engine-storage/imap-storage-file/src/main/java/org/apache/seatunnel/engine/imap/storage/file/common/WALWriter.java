/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 */

package org.apache.seatunnel.engine.imap.storage.file.common;

import org.apache.seatunnel.engine.imap.storage.api.common.Serializer;
import org.apache.seatunnel.engine.imap.storage.file.bean.IMapFileData;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSOutputStream;
import org.apache.hadoop.hdfs.client.HdfsDataOutputStream;

import java.io.IOException;
import java.util.EnumSet;

public class WALWriter implements AutoCloseable {

    FSDataOutputStream out;

    Serializer serializer;

    public WALWriter(FileSystem fs, Path parentPath, Serializer serializer) throws IOException {
        Path path = new Path(parentPath, "wal.txt");
        this.out = fs.create(path);
        this.serializer = serializer;
    }

    public void write(IMapFileData data) throws IOException {
        byte[] bytes = serializer.serialize(data);
        this.write(bytes);
    }

    public void flush() throws IOException {
        // hsync to flag
        if (out instanceof HdfsDataOutputStream) {
            ((HdfsDataOutputStream) out).hsync(EnumSet.of(HdfsDataOutputStream.SyncFlag.UPDATE_LENGTH));
        }
        if (out.getWrappedStream() instanceof DFSOutputStream) {
            ((DFSOutputStream) out.getWrappedStream()).hsync(EnumSet.of(HdfsDataOutputStream.SyncFlag.UPDATE_LENGTH));
        } else {
            out.hsync();
        }
        this.out.hflush();

    }

    private void write(byte[] bytes) throws IOException {
        byte[] data = WALDataUtils.wrapperBytes(bytes);
        this.out.write(data);
        this.flush();
    }

    @Override
    public void close() throws Exception {
        out.close();
    }
}
