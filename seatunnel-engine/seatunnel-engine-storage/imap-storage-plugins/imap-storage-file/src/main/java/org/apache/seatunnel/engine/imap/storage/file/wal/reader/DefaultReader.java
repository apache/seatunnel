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
package org.apache.seatunnel.engine.imap.storage.file.wal.reader;

import org.apache.seatunnel.engine.imap.storage.file.bean.IMapFileData;
import org.apache.seatunnel.engine.imap.storage.file.common.WALDataUtils;
import org.apache.seatunnel.engine.serializer.api.Serializer;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;

import java.io.BufferedInputStream;
import java.io.DataInputStream;
import java.io.EOFException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.apache.seatunnel.engine.imap.storage.file.common.WALDataUtils.WAL_DATA_METADATA_LENGTH;

public class DefaultReader implements IFileReader<IMapFileData> {
    FileSystem fs;
    Serializer serializer;

    @Override
    public String identifier() {
        return "default";
    }

    @Override
    public void initialize(FileSystem fs, Serializer serializer) throws IOException {
        this.fs = fs;
        this.serializer = serializer;
    }

    @Override
    public List<IMapFileData> readAllData(Path parentPath) throws IOException {
        List<IMapFileData> result = new ArrayList<>();
        forEachData(parentPath, result::add);
        return result;
    }

    @Override
    public void forEachData(Path parentPath, RecordConsumer<IMapFileData> consumer)
            throws IOException {
        if (!fs.exists(parentPath)) {
            return;
        }

        RemoteIterator<LocatedFileStatus> files = fs.listFiles(parentPath, true);
        while (files.hasNext()) {
            LocatedFileStatus file = files.next();
            if (file.getPath().getName().endsWith("wal.txt")) {
                readData(file, consumer);
            }
        }
    }

    private void readData(LocatedFileStatus file, RecordConsumer<IMapFileData> consumer)
            throws IOException {
        long remainingBytes = file.getLen();
        byte[] metadata = new byte[WAL_DATA_METADATA_LENGTH];
        try (DataInputStream input =
                new DataInputStream(new BufferedInputStream(fs.open(file.getPath())))) {
            while (remainingBytes >= WAL_DATA_METADATA_LENGTH) {
                if (!readFully(input, metadata)) {
                    break;
                }
                remainingBytes -= WAL_DATA_METADATA_LENGTH;

                int dataLength = WALDataUtils.byteArrayToInt(metadata);
                if (dataLength > remainingBytes) {
                    // The writer may have stopped in the middle of its last record.
                    break;
                }

                byte[] serializedRecord = new byte[dataLength];
                if (!readFully(input, serializedRecord)) {
                    break;
                }
                remainingBytes -= dataLength;
                consumer.accept(serializer.deserialize(serializedRecord, IMapFileData.class));
            }
        }
    }

    private boolean readFully(DataInputStream input, byte[] target) throws IOException {
        try {
            input.readFully(target);
            return true;
        } catch (EOFException ignored) {
            return false;
        }
    }
}
