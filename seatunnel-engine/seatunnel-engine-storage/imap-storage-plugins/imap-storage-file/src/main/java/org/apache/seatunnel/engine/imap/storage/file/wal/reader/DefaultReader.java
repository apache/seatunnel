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
import org.apache.seatunnel.engine.imap.storage.file.compaction.ConcurrentCompactionException;
import org.apache.seatunnel.engine.imap.storage.file.compaction.HdfsCompactionStorage;
import org.apache.seatunnel.engine.serializer.api.Serializer;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;

import java.io.BufferedInputStream;
import java.io.DataInputStream;
import java.io.EOFException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

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

        String scheme = fs.getUri().getScheme();
        if (!"hdfs".equalsIgnoreCase(scheme) && !"file".equalsIgnoreCase(scheme)) {
            // Object-store writers retain their existing streaming replay and do not use the
            // HDFS atomic-token protocol.
            RemoteIterator<LocatedFileStatus> files = fs.listFiles(parentPath, true);
            while (files.hasNext()) {
                LocatedFileStatus file = files.next();
                if (file.getPath().getName().endsWith("wal.txt")) {
                    readData(file, consumer, false);
                }
            }
            return;
        }

        List<LocatedFileStatus> files = listFiles(parentPath);
        Map<Path, String> tokens = captureTokens(files);
        if (!tokens.isEmpty()) {
            // Capture tokens BEFORE enumerating data. A paginated listing can miss both an
            // input removed behind its cursor and the snapshot that replaced it.
            files = listFiles(parentPath);
            if (!tokenPaths(files).equals(tokens.keySet())) {
                throw new ConcurrentCompactionException(
                        "IMap compaction scopes changed under " + parentPath);
            }
            // Regional plans compact a prefix; snapshots must keep that prefix's replay position.
            files.sort(Comparator.comparing(file -> file.getPath().toString()));
        }
        for (LocatedFileStatus file : files) {
            if (file.getPath().getName().endsWith("wal.txt")) {
                readData(file, consumer, false);
            }
        }
        for (Map.Entry<Path, String> token : tokens.entrySet()) {
            if (!token.getValue().equals(HdfsCompactionStorage.readToken(fs, token.getKey()))) {
                throw new ConcurrentCompactionException(
                        "IMap compaction committed during replay: " + token.getKey());
            }
        }
    }

    private List<LocatedFileStatus> listFiles(Path path) throws IOException {
        List<LocatedFileStatus> result = new ArrayList<>();
        RemoteIterator<LocatedFileStatus> files = fs.listFiles(path, true);
        while (files.hasNext()) {
            result.add(files.next());
        }
        return result;
    }

    private Set<Path> tokenPaths(List<LocatedFileStatus> files) throws IOException {
        Set<Path> paths = new HashSet<>();
        Set<Path> segmentDirectories = new HashSet<>();
        for (LocatedFileStatus file : files) {
            if (file.getPath().getName().equals(HdfsCompactionStorage.CURRENT_FILE)) {
                paths.add(file.getPath());
            } else if (file.getPath().getName().startsWith("segment-")
                    || file.getPath().getName().startsWith(HdfsCompactionStorage.SNAPSHOT_PREFIX)) {
                segmentDirectories.add(file.getPath().getParent());
            }
        }
        // A newly initialized scope's token can fall behind a directory-listing cursor while
        // its later-created segments are still encountered. Its token exists before any WAL.
        for (Path directory : segmentDirectories) {
            Path token = new Path(directory, HdfsCompactionStorage.CURRENT_FILE);
            if (!paths.contains(token) && fs.exists(token)) {
                paths.add(token);
            }
        }
        return paths;
    }

    private Map<Path, String> captureTokens(List<LocatedFileStatus> files) throws IOException {
        Map<Path, String> tokens = new HashMap<>();
        for (Path path : tokenPaths(files)) {
            tokens.put(path, HdfsCompactionStorage.readToken(fs, path));
        }
        return tokens;
    }

    /**
     * Completed compaction inputs must contain whole frames; a truncated sealed file is an error.
     */
    public void readCompletedFile(Path path, RecordConsumer<IMapFileData> consumer)
            throws IOException {
        readData(fs.getFileStatus(path), consumer, true);
    }

    private void readData(
            FileStatus file, RecordConsumer<IMapFileData> consumer, boolean requireComplete)
            throws IOException {
        long remainingBytes = file.getLen();
        byte[] metadata = new byte[WAL_DATA_METADATA_LENGTH];
        try (DataInputStream input =
                new DataInputStream(new BufferedInputStream(fs.open(file.getPath())))) {
            while (remainingBytes >= WAL_DATA_METADATA_LENGTH) {
                if (!readFully(input, metadata)) {
                    if (requireComplete) {
                        throw new EOFException("Incomplete sealed WAL metadata: " + file.getPath());
                    }
                    break;
                }
                remainingBytes -= WAL_DATA_METADATA_LENGTH;

                int dataLength = WALDataUtils.byteArrayToInt(metadata);
                if (dataLength < 0) {
                    throw new IOException("Negative WAL record length in " + file.getPath());
                }
                if (dataLength > remainingBytes) {
                    if (requireComplete) {
                        throw new EOFException("Incomplete sealed WAL record: " + file.getPath());
                    }
                    // The writer may have stopped in the middle of its last record.
                    break;
                }

                byte[] serializedRecord = new byte[dataLength];
                if (!readFully(input, serializedRecord)) {
                    if (requireComplete) {
                        throw new EOFException("Incomplete sealed WAL record: " + file.getPath());
                    }
                    break;
                }
                remainingBytes -= dataLength;
                consumer.accept(serializer.deserialize(serializedRecord, IMapFileData.class));
            }
            if (requireComplete && remainingBytes != 0) {
                throw new EOFException("Incomplete sealed WAL tail: " + file.getPath());
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
