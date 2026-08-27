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
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.apache.seatunnel.engine.imap.storage.file.common.WALDataUtils.WAL_DATA_METADATA_LENGTH;

public class DefaultReader implements IFileReader<IMapFileData> {
    private static final int DEFAULT_QUERY_LIST_SIZE = 1024;
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
        List<String> fileNames = getFileNames(parentPath);
        if (CollectionUtils.isEmpty(fileNames)) {
            return new ArrayList<>();
        }
        List<IMapFileData> result = new ArrayList<>(DEFAULT_QUERY_LIST_SIZE);
        for (String fileName : fileNames) {
            result.addAll(readData(new Path(parentPath, fileName)));
        }
        return result;
    }

    private List<String> getFileNames(Path parentPath) {
        try {
            if (!fs.exists(parentPath)) {
                return new ArrayList<>();
            }
            RemoteIterator<LocatedFileStatus> fileStatusRemoteIterator =
                    fs.listFiles(parentPath, true);
            List<String> fileNames = new ArrayList<>();
            while (fileStatusRemoteIterator.hasNext()) {
                LocatedFileStatus fileStatus = fileStatusRemoteIterator.next();
                if (fileStatus.getPath().getName().endsWith("wal.txt")) {
                    fileNames.add(fileStatus.getPath().toString());
                }
            }
            return fileNames;
        } catch (IOException e) {
            throw new IMapStorageException(e, "get file names error,path is s%", parentPath);
        }
    }

    private List<IMapFileData> readData(Path path) throws IOException {
        List<IMapFileData> result = new ArrayList<>(DEFAULT_QUERY_LIST_SIZE);
        long length = fs.getFileStatus(path).getLen();
        try (FSDataInputStream in = fs.open(path)) {
            byte[] datas = new byte[(int) length];
            in.readFully(datas);
            int startIndex = 0;
            while (startIndex + WAL_DATA_METADATA_LENGTH < datas.length) {

                byte[] metadata = new byte[WAL_DATA_METADATA_LENGTH];
                System.arraycopy(datas, startIndex, metadata, 0, WAL_DATA_METADATA_LENGTH);
                int dataLength = WALDataUtils.byteArrayToInt(metadata);
                startIndex += WAL_DATA_METADATA_LENGTH;
                if (startIndex + dataLength > datas.length) {
                    break;
                }
                byte[] data = new byte[dataLength];
                System.arraycopy(datas, startIndex, data, 0, data.length);
                IMapFileData fileData = serializer.deserialize(data, IMapFileData.class);
                result.add(fileData);
                startIndex += data.length;
            }
        }
        return result;
    }
}
