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

package org.apache.seatunnel.engine.imap.storage.file.wal.writer;

import org.apache.seatunnel.engine.imap.storage.file.bean.IMapFileData;
import org.apache.seatunnel.engine.imap.storage.file.common.WALDataUtils;
import org.apache.seatunnel.engine.serializer.api.Serializer;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSOutputStream;
import org.apache.hadoop.hdfs.client.HdfsDataOutputStream;

import java.io.IOException;
import java.util.EnumSet;

public class HdfsWriter implements IFileWriter<IMapFileData> {

    private FSDataOutputStream out;

    private Serializer serializer;

    @Override
    public String identifier() {
        return "hdfs";
    }

    @Override
    public void initialize(FileSystem fs, Path parentPath, Serializer serializer)
            throws IOException {
        Path path = new Path(parentPath, FILE_NAME);
        this.out = fs.create(path);
        this.serializer = serializer;
    }

    @Override
    public void write(IMapFileData data) throws IOException {
        byte[] bytes = serializer.serialize(data);
        this.write(bytes);
    }

    /**
     * Forces the appended WAL record to durable storage with exactly one sync path.
     *
     * <p>Write-through MapStore waits on every append, so duplicate {@code hsync}/{@code hflush}
     * calls only add latency variance without improving durability. Prefer the HDFS-aware sync when
     * available; otherwise fall back to a single {@link FSDataOutputStream#hsync()}.
     */
    public void flush() throws IOException {
        if (out instanceof HdfsDataOutputStream) {
            ((HdfsDataOutputStream) out)
                    .hsync(EnumSet.of(HdfsDataOutputStream.SyncFlag.UPDATE_LENGTH));
            return;
        }
        if (out.getWrappedStream() instanceof DFSOutputStream) {
            ((DFSOutputStream) out.getWrappedStream())
                    .hsync(EnumSet.of(HdfsDataOutputStream.SyncFlag.UPDATE_LENGTH));
            return;
        }
        out.hsync();
    }

    private void write(byte[] bytes) throws IOException {
        byte[] data = WALDataUtils.wrapperBytes(bytes);
        this.out.write(data);
        this.flush();
    }

    @Override
    public void close() throws Exception {
        if (out != null) {
            out.close();
        }
    }
}
