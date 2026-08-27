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

import org.apache.seatunnel.engine.imap.storage.api.exception.IMapStorageException;
import org.apache.seatunnel.engine.imap.storage.file.bean.IMapFileData;
import org.apache.seatunnel.engine.imap.storage.file.common.WALDataUtils;
import org.apache.seatunnel.engine.serializer.api.Serializer;

import org.apache.curator.shaded.com.google.common.io.ByteStreams;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicLong;

@Slf4j
public abstract class CloudWriter implements IFileWriter<IMapFileData> {
    private FileSystem fs;
    private Path parentPath;
    private Path path;
    private Serializer serializer;

    private ByteBuf bf = Unpooled.buffer(1024);

    // block size,  default 1024*1024
    private long blockSize = 1024 * 1024;

    private AtomicLong index = new AtomicLong(0);

    @Override
    public void initialize(FileSystem fs, Path parentPath, Serializer serializer)
            throws IOException {

        this.fs = fs;
        this.serializer = serializer;
        this.parentPath = parentPath;
        this.path = createNewPath();
        if (fs.exists(path)) {
            try (FSDataInputStream fsDataInputStream = fs.open(path)) {
                bf.writeBytes(ByteStreams.toByteArray(fsDataInputStream));
            }
        }
    }

    @Override
    public void setBlockSize(Long blockSize) {
        if (blockSize != null && blockSize > DEFAULT_BLOCK_SIZE) {
            this.blockSize = blockSize;
        }
    }

    // TODO Synchronous write, asynchronous write can be added in the future
    @Override
    public void write(IMapFileData data) throws IOException {
        byte[] bytes = serializer.serialize(data);
        this.write(bytes);
    }

    private void write(byte[] bytes) {
        try (FSDataOutputStream out = fs.create(path, true)) {
            // Write to bytebuffer
            byte[] data = WALDataUtils.wrapperBytes(bytes);
            bf.writeBytes(data);

            // Read all bytes
            byte[] allBytes = new byte[bf.readableBytes()];
            bf.readBytes(allBytes);

            // write filesystem
            out.write(allBytes);

            // check and reset
            checkAndSetNextScheduleRotation(allBytes.length);

        } catch (Exception ex) {
            throw new IMapStorageException(ex);
        }
    }

    private void checkAndSetNextScheduleRotation(long allBytes) {
        if (allBytes > blockSize) {
            this.path = createNewPath();
            this.bf.clear();
        } else {
            // reset index
            bf.resetReaderIndex();
        }
    }

    public Path createNewPath() {
        return new Path(parentPath, index.incrementAndGet() + "_" + FILE_NAME);
    }

    @Override
    public void close() throws Exception {
        bf.clear();
        this.bf = null;
    }
}
