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

package org.apache.seatunnel.engine.imap.storage.file.common;

import static org.apache.seatunnel.engine.imap.storage.file.common.WALDataUtils.WAL_DATA_METADATA_LENGTH;

import org.apache.seatunnel.engine.imap.storage.api.common.Serializer;
import org.apache.seatunnel.engine.imap.storage.api.exception.IMapStorageException;
import org.apache.seatunnel.engine.imap.storage.file.bean.IMapFileData;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.ClassUtils;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class WALReader {
    private static final int DEFAULT_QUERY_LIST_SIZE = 1024;
    private FileSystem fs;
    private final Serializer serializer;

    public WALReader(FileSystem fs, Serializer serializer) throws IOException {
        this.serializer = serializer;
        this.fs = fs;
    }

    private List<IMapFileData> readAllData(Path parentPath) throws IOException {
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

    public Set<Object> loadAllKeys(Path parentPath) throws IOException {
        List<IMapFileData> allData = readAllData(parentPath);
        if (CollectionUtils.isEmpty(allData)) {
            return new HashSet<>();
        }
        Collections.sort(allData);
        Set<Object> result = new HashSet<>(allData.size());
        Map<Object, Long> deleteMap = new HashMap<>();
        for (IMapFileData data : allData) {
            Object key = deserializeData(data.getKey(), data.getKeyClassName());
            if (deleteMap.containsKey(key)) {
                continue;
            }
            if (data.isDeleted()) {
                deleteMap.put(key, data.getTimestamp());
                continue;
            }
            if (result.contains(key)) {
                continue;
            }
            result.add(key);
        }
        return result;
    }

    public Map<Object, Object> loadAllData(Path parentPath, Set<Object> searchKeys) throws IOException {
        List<IMapFileData> allData = readAllData(parentPath);
        if (CollectionUtils.isEmpty(allData)) {
            return new HashMap<>();
        }
        Collections.sort(allData);
        Map<Object, Object> result = new HashMap<>(allData.size());
        Map<Object, Long> deleteMap = new HashMap<>();
        boolean searchByKeys = CollectionUtils.isNotEmpty(searchKeys);
        for (IMapFileData data : allData) {
            Object key = deserializeData(data.getKey(), data.getKeyClassName());
            if (searchByKeys && !searchKeys.contains(data.getKey())) {
                continue;
            }
            if (deleteMap.containsKey(key)) {
                continue;
            }
            if (data.isDeleted()) {
                deleteMap.put(key, data.getTimestamp());
                continue;
            }
            if (result.containsKey(key)) {
                continue;
            }
            Object value = deserializeData(data.getValue(), data.getValueClassName());
            result.put(key, value);
        }
        return result;
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

    private List<String> getFileNames(Path parentPath) {
        try {

            RemoteIterator<LocatedFileStatus> fileStatusRemoteIterator = fs.listFiles(parentPath, true);
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

    private Object deserializeData(byte[] data, String className) {
        try {
            Class<?> clazz = ClassUtils.getClass(className);
            try {
                return serializer.deserialize(data, clazz);
            } catch (IOException e) {
                //log.error("deserialize data error, data is {}, className is {}", data, className, e);
                throw new IMapStorageException(e, "deserialize data error: data is s%, className is s%", data, className);
            }
        } catch (ClassNotFoundException e) {
            //  log.error("deserialize data error, class name is {}", className, e);
            throw new IMapStorageException(e, "deserialize data error, class name is {}", className);
        }
    }
}


