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

import org.apache.seatunnel.engine.imap.storage.api.exception.IMapStorageException;
import org.apache.seatunnel.engine.imap.storage.file.bean.IMapFileData;
import org.apache.seatunnel.engine.imap.storage.file.common.WALDataUtils;
import org.apache.seatunnel.engine.serializer.api.Serializer;

import org.apache.commons.collections.CollectionUtils;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.io.SequenceInputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;

import static org.apache.seatunnel.engine.imap.storage.file.common.WALDataUtils.FILE_NAME;

public class DefaultReader implements IFileReader<IMapFileData> {
    private static final int DEFAULT_QUERY_LIST_SIZE = 1024;
    private FileSystem fs;
    private Serializer serializer;

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
        List<String> fileNames = WALDataUtils.getDataFiles(fs, parentPath, FILE_NAME);
        if (CollectionUtils.isEmpty(fileNames)) {
            return new ArrayList<>();
        }

        List<Path> paths =
                fileNames.stream()
                        .map(filename -> new Path(parentPath, filename))
                        .collect(Collectors.toList());
        return readData(paths);
    }

    public List<IMapFileData> readData(List<Path> paths) throws IOException {
        List<IMapFileData> result = new ArrayList<>(DEFAULT_QUERY_LIST_SIZE);

        // get file streams
        List<FSDataInputStream> streams =
                paths.stream()
                        .sorted(Comparator.comparing(Path::getName)) // sort Path by filename asc
                        .map(
                                path -> {
                                    try {
                                        return fs.open(path);
                                    } catch (IOException e) {
                                        throw new IMapStorageException(e);
                                    }
                                }) // open files
                        .collect(Collectors.toList());

        // read data
        byte[] bytes;
        try (SequenceInputStream in = new SequenceInputStream(Collections.enumeration(streams))) {
            while ((bytes = WALDataUtils.readNextData(in)) != null) {
                IMapFileData diskData = serializer.deserialize(bytes, IMapFileData.class);
                result.add(diskData);
            }
        }

        return result;
    }
}
