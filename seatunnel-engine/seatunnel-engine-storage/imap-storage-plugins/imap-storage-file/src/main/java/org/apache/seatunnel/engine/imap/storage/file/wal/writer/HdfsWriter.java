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

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.SequenceInputStream;
import java.util.List;
import java.util.stream.Collectors;

import static org.apache.seatunnel.engine.imap.storage.file.common.WALDataUtils.FILE_NAME;
import static org.apache.seatunnel.engine.imap.storage.file.common.WALDataUtils.PROGRESSING_SUFFIX;

@Slf4j
public class HdfsWriter implements IFileWriter<IMapFileData> {
    // block size,  default 1024*1024
    private long blockSize = 1024 * 1024;

    private Serializer serializer;

    private FileSystem fs;
    private Path parentPath;
    private long blockRemaining;
    private int currentBlock;
    private FSDataOutputStream out;

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
    public synchronized void write(IMapFileData data) throws IOException {
        List<String> dataFiles = WALDataUtils.getDataFiles(fs, parentPath, FILE_NAME);
        SequenceInputStream stream = WALDataUtils.getComposedInputStream(fs, dataFiles);
        // reset block info
        reset();

        // write data
        writeEntry(serializer.serialize(data));
        byte[] bytes;
        boolean encountered = false;
        while ((bytes = WALDataUtils.readNextData(stream)) != null) {
            IMapFileData diskData = serializer.deserialize(bytes, IMapFileData.class);

            if (encountered) writeEntry(serializer.serialize(diskData));
            else if (isKeyEquals(data, diskData))
                encountered = true; // if current data is the entry which have be updated
            else writeEntry(serializer.serialize(diskData));
        }
        stream.close();
        commit(dataFiles);
    }

    public void commit(List<String> filenames) throws IOException {
        // close last file stream
        out.hsync();
        out.close();
        // delete old data file
        filenames.forEach(
                filename -> {
                    try {
                        fs.delete(new Path(filename), false);
                    } catch (IOException e) {
                        throw new IMapStorageException(
                                "delete old imap file failed, cause: " + e.getMessage(), e);
                    }
                });

        // move new data file
        WALDataUtils.getDataFiles(fs, parentPath, PROGRESSING_SUFFIX).stream()
                .collect(
                        Collectors.toMap(
                                Path::new,
                                filename -> new Path(filename.replace(PROGRESSING_SUFFIX, ""))))
                .forEach(
                        (src, dest) -> {
                            try {
                                fs.rename(src, dest);
                            } catch (IOException e) {
                                throw new IMapStorageException(
                                        "rename imap file failed, cause: " + e.getMessage(), e);
                            }
                        });
    }

    public boolean isKeyEquals(IMapFileData left, IMapFileData right) throws IOException {
        try {
            Object leftKey =
                    serializer.deserialize(left.getKey(), Class.forName(left.getKeyClassName()));
            Object rightKey =
                    serializer.deserialize(right.getKey(), Class.forName(right.getKeyClassName()));
            return leftKey.equals(rightKey);
        } catch (ClassNotFoundException e) {
            throw new IMapStorageException(
                    "imap data broken, cannot deserialize key, cause: %s" + e.getMessage(), e);
        }
    }

    /** reset block info and create the first block file */
    public void reset() throws IOException {
        this.blockRemaining = blockSize;
        this.currentBlock = 0;
        this.out =
                fs.create(
                        new Path(parentPath, currentBlock + "_" + FILE_NAME + PROGRESSING_SUFFIX),
                        true);
    }

    /** set block info to next block, and create new block file */
    public void nextBlock() throws IOException {
        out.hsync();
        out.close();
        blockRemaining = blockSize;
        out =
                fs.create(
                        new Path(parentPath, ++currentBlock + "_" + FILE_NAME + PROGRESSING_SUFFIX),
                        true);
    }

    private void writeEntry(byte[] bytes) throws IOException {
        // wrap data with metadata
        byte[] data = WALDataUtils.wrapperBytes(bytes);
        int tobeWritten = data.length;

        // write data
        ByteArrayInputStream in = new ByteArrayInputStream(data);
        byte[] buffer = new byte[1024];
        while (tobeWritten != 0) {
            int len = (int) Math.min(buffer.length, Math.min(tobeWritten, blockRemaining));
            int read = in.read(buffer, 0, len);
            out.write(buffer, 0, read);

            tobeWritten -= read;
            blockRemaining -= read;

            // rolling to next block
            if (blockRemaining == 0) nextBlock();
        }
    }

    @Override
    public void close() throws Exception {
        if (out != null) {
            out.hsync();
            out.close();
        }
    }
}
