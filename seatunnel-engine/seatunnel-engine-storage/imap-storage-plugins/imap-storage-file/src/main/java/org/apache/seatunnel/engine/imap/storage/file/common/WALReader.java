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

import org.apache.seatunnel.shade.org.apache.commons.lang3.ClassUtils;

import org.apache.seatunnel.engine.imap.storage.api.exception.IMapStorageException;
import org.apache.seatunnel.engine.imap.storage.file.bean.IMapFileData;
import org.apache.seatunnel.engine.imap.storage.file.config.FileConfiguration;
import org.apache.seatunnel.engine.imap.storage.file.wal.DiscoveryWalFileFactory;
import org.apache.seatunnel.engine.imap.storage.file.wal.reader.IFileReader;
import org.apache.seatunnel.engine.serializer.api.Serializer;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import static org.apache.seatunnel.engine.imap.storage.file.common.LatestMutationAccumulator.SerializedKey;

public class WALReader {
    private final Serializer serializer;
    private final IFileReader<IMapFileData> fileReader;

    public WALReader(FileSystem fs, FileConfiguration configuration, Serializer serializer)
            throws IOException {
        this.serializer = serializer;
        this.fileReader = DiscoveryWalFileFactory.getReader(configuration.getName());
        this.fileReader.initialize(fs, serializer);
    }

    public Set<Object> loadAllKeys(Path parentPath) throws IOException {
        LatestMutationAccumulator accumulator = readLatestMutations(parentPath, null);
        Set<Object> result = new HashSet<>(accumulator.size());
        Iterator<IMapFileData> mutations = accumulator.iterator();
        while (mutations.hasNext()) {
            IMapFileData mutation = mutations.next();
            mutations.remove();
            if (!mutation.isDeleted()) {
                result.add(deserializeData(mutation.getKey(), mutation.getKeyClassName()));
            }
        }
        return result;
    }

    public Map<Object, Object> loadAllData(Path parentPath, Set<Object> searchKeys)
            throws IOException {
        Set<SerializedKey> serializedSearchKeys = serializeSearchKeys(searchKeys);
        LatestMutationAccumulator accumulator =
                readLatestMutations(parentPath, serializedSearchKeys);
        Map<Object, Object> result = new HashMap<>(accumulator.size());
        Iterator<IMapFileData> mutations = accumulator.iterator();
        while (mutations.hasNext()) {
            IMapFileData mutation = mutations.next();
            mutations.remove();
            if (mutation.isDeleted()) {
                continue;
            }
            Object key = deserializeData(mutation.getKey(), mutation.getKeyClassName());
            Object value = deserializeData(mutation.getValue(), mutation.getValueClassName());
            result.put(key, value);
        }
        return result;
    }

    private LatestMutationAccumulator readLatestMutations(
            Path parentPath, Set<SerializedKey> serializedSearchKeys) throws IOException {
        LatestMutationAccumulator accumulator = new LatestMutationAccumulator(serializedSearchKeys);
        fileReader.forEachData(parentPath, accumulator::accept);
        return accumulator;
    }

    private Set<SerializedKey> serializeSearchKeys(Set<Object> searchKeys) throws IOException {
        if (searchKeys == null || searchKeys.isEmpty()) {
            return null;
        }

        Set<SerializedKey> serializedKeys = new HashSet<>(searchKeys.size());
        for (Object key : searchKeys) {
            serializedKeys.add(
                    SerializedKey.of(serializer.serialize(key), key.getClass().getName()));
        }
        return serializedKeys;
    }

    private Object deserializeData(byte[] data, String className) {
        try {
            Class<?> clazz = ClassUtils.getClass(className);
            try {
                return serializer.deserialize(data, clazz);
            } catch (IOException e) {
                // log.error("deserialize data error, data is {}, className is {}", data, className,
                // e);
                throw new IMapStorageException(
                        e, "deserialize data error: data is s%, className is s%", data, className);
            }
        } catch (ClassNotFoundException e) {
            //  log.error("deserialize data error, class name is {}", className, e);
            throw new IMapStorageException(
                    e, "deserialize data error, class name is {}", className);
        }
    }
}
