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

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.util.Arrays;

@Slf4j
public class HdfsWriter implements IFileWriter<IMapFileData> {
    // block size,  default 1024*1024
    private long blockSize = 1024 * 1024;

    private Serializer serializer;

    private FileSystem fs;
    private Path parentPath;

    @Override
    public String identifier() {
        return "hdfs";
    }

    @Override
    public void initialize(FileSystem fs, Path parentPath, Serializer serializer)
            throws IOException {
        this.fs = fs;
        this.serializer = serializer;
        this.parentPath = parentPath;
    }

    @Override
    public void setBlockSize(Long blockSize) {
        if (blockSize != null && blockSize > DEFAULT_BLOCK_SIZE) {
            this.blockSize = blockSize;
        }
    }

    @Override
    public void write(IMapFileData data) throws IOException {
        byte[] bytes = serializer.serialize(data);
        this.write(bytes);
    }

    private void write(byte[] bytes) {
        // delete old files, if delete failed, just ignore.
        try {
            fs.delete(parentPath, true);
        } catch (IOException e) {
            log.warn("Failed to delete old IMap files in hdfs, cause: {}", e.getMessage(), e);
        }

        // wrap data with metadata
        byte[] data = WALDataUtils.wrapperBytes(bytes);

        // write data into each block
        long blocks = data.length / blockSize + (data.length % blockSize == 0 ? 0 : 1);
        for (int i = 0; i < blocks; i++) {
            Path path = new Path(parentPath, i + "_" + FILE_NAME);
            // get block data
            int start = (int) (i * blockSize);
            int end = (int) Math.min(start + blockSize, data.length);
            byte[] blockData = Arrays.copyOfRange(data, start, end);

            // write to file
            try (FSDataOutputStream out = fs.create(path, true)) {
                out.write(blockData);
                out.hsync();
            } catch (Exception e) {
                throw new IMapStorageException(e);
            }
        }
    }

    @Override
    public void close() throws Exception {}
}
