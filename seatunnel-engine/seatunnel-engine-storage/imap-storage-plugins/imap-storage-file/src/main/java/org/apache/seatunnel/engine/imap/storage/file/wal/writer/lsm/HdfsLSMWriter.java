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
import org.apache.hadoop.hdfs.DFSOutputStream;
import org.apache.hadoop.hdfs.client.HdfsDataOutputStream;

import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.util.EnumSet;
import java.util.Map;

@Slf4j
public class HdfsLSMWriter extends AbstractLSMWriter {
    private FSDataOutputStream out;

    public HdfsLSMWriter(Map<String, Object> config) {
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
        try {
            openTmpOut();
        } catch (IOException e) {
            throw new IMapStorageException(e);
        }
    }

    @Override
    public String identifier() {
        return "hdfs-lsm";
    }

    protected void writeInternal(byte[] bytes, IMapFileData iMapFileData, boolean flush) {
        boolean isLast;
        try {
            // add to in-memory batch (for sorting on rotation)
            if (iMapFileData != null) {
                writeBatch.add(iMapFileData);
            }

            byte[] wrapped = WALDataUtils.wrapperBytes(bytes);
            out.write(wrapped);
            flush();

            long currentSize = out.size();

            isLast = checkAndSetNextScheduleRotation(currentSize, flush);
        } catch (IOException e) {
            throw new IMapStorageException(e);
        }

        try {
            if (isLast) {
                out.close();
                sortFlush();
                openTmpOut();
            }
        } catch (Exception e) {
            throw new IMapStorageException(e);
        }
    }

    private void openTmpOut() throws IOException {
        this.out = fs.create(currentTmpPath, true);
    }

    private void flush() throws IOException {
        if (out == null) return;
        if (out instanceof HdfsDataOutputStream) {
            ((HdfsDataOutputStream) out)
                    .hsync(EnumSet.of(HdfsDataOutputStream.SyncFlag.UPDATE_LENGTH));
        }
        if (out.getWrappedStream() instanceof DFSOutputStream) {
            ((DFSOutputStream) out.getWrappedStream())
                    .hsync(EnumSet.of(HdfsDataOutputStream.SyncFlag.UPDATE_LENGTH));
        } else {
            out.hsync();
        }
        out.hflush();
    }

    @Override
    protected long writeWithBatch(Path path) throws IOException {
        long totalWritten = 0;
        try (FSDataOutputStream outStream = fs.create(path, true)) {
            for (IMapFileData d : writeBatch) {
                byte[] ser = serializer.serialize(d);
                byte[] wrap = WALDataUtils.wrapperBytes(ser);
                outStream.write(wrap);
                totalWritten += wrap.length;
            }
            outStream.hflush();
        }
        return totalWritten;
    }

    @Override
    public void close() throws Exception {
        isRunning = false;
        try {
            if (out != null) {
                out.close();
            }

            if (!writeBatch.isEmpty()) {
                sortFlush();
            }
        } finally {
            out = null;

            clearScheduler();
        }
    }
}
