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

package org.apache.seatunnel.engine.imap.storage.file.wal.writer.lsm;

import org.apache.seatunnel.engine.imap.storage.api.exception.IMapStorageException;
import org.apache.seatunnel.engine.imap.storage.file.bean.IMapFileData;
import org.apache.seatunnel.engine.imap.storage.file.common.WALDataUtils;
import org.apache.seatunnel.engine.serializer.api.Serializer;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.util.Map;

@Slf4j
public abstract class CloudLSMWriter extends AbstractLSMWriter {
    protected ByteBuf bf = Unpooled.buffer(1024);

    protected CloudLSMWriter(Map<String, Object> config) {
        super(config);
    }

    public void initialize(FileSystem fs, Path parentPath, Serializer serializer)
            throws IOException {
        this.fs = fs;
        this.parentPath = parentPath;
        this.finalPath = createNewPath();
        this.serializer = serializer;
        this.currentTmpPath = createNewTmpPath();
        // attempt recover existing tmp files
        try {
            recoverFromCrash();
        } catch (InterruptedException e) {
            log.warn("Recover from crash interrupted", e);
            Thread.currentThread().interrupt();
        }
    }

    @Override
    public String identifier() {
        return "hdfs-lsm";
    }

    @Override
    public void setBlockSize(Long blockSize) {
        if (blockSize != null && blockSize > DEFAULT_BLOCK_SIZE) {
            this.blockSize = blockSize;
        }
    }

    @Override
    protected void writeInternal(byte[] bytes, IMapFileData iMapFileData, boolean flush) {
        boolean isLast = flush;
        try (FSDataOutputStream out = fs.create(currentTmpPath, true)) {
            // Write to bytebuffer
            if (iMapFileData != null) {
                writeBatch.add(iMapFileData);
            }

            byte[] data = WALDataUtils.wrapperBytes(bytes);
            bf.writeBytes(data);

            // Read all bytes
            byte[] allBytes = new byte[bf.readableBytes()];
            bf.readBytes(allBytes);

            // write filesystem
            out.write(allBytes);

            // check and reset
            isLast = checkAndSetNextScheduleRotation(allBytes.length, flush);
        } catch (Exception ex) {
            throw new IMapStorageException(ex);
        }
        try {
            if (isLast) {
                sortFlush();
                this.bf.clear();
            }
        } catch (Exception e) {
            throw new IMapStorageException(e);
        }
    }

    @Override
    protected long writeWithBatch(Path tmpPath) throws IOException {
        ByteBuf sortedBuf = null;
        long totalWritten = 0;
        try {
            sortedBuf = Unpooled.buffer();
            for (IMapFileData data : writeBatch) {
                byte[] serialized = serializer.serialize(data);
                byte[] wrapper = WALDataUtils.wrapperBytes(serialized);
                sortedBuf.writeBytes(wrapper);
                totalWritten += wrapper.length;
            }

            try (FSDataOutputStream out = fs.create(finalPath, true)) {
                byte[] allBytes = new byte[sortedBuf.readableBytes()];
                sortedBuf.readBytes(allBytes);
                out.write(allBytes);
            }
        } finally {
            if (sortedBuf != null) sortedBuf.release();
        }
        return totalWritten;
    }

    @Override
    public void close() throws Exception {
        isRunning = false;
        bf.clear();
        if (!writeBatch.isEmpty()) {
            sortFlush();
        }
        this.bf = null;

        clearScheduler();
    }
}
