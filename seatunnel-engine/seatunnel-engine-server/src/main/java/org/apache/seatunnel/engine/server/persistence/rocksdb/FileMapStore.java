/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.seatunnel.engine.server.persistence.rocksdb;

import org.apache.seatunnel.engine.imap.storage.api.RocksDBStorage;
import org.apache.seatunnel.engine.imap.storage.api.RocksDBStorageFactory;

import lombok.SneakyThrows;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class FileMapStore {
    private RocksDBStorage mapStorage;

    public FileMapStore(RocksDBStorageFactory factory, Map<String, Object> configuration) {
        this.mapStorage = factory.create(configuration);
    }

    public void destroy() {
        mapStorage.destroy(false);
    }

    public void store(Object key, Object value) {
        mapStorage.store(key, value);
    }

    public void storeAll(Map<Object, Object> map) {
        mapStorage.storeAll(map);
    }

    public void delete(Object key) {
        mapStorage.delete(key);
    }

    public void deleteAll(Collection<Object> keys) {
        mapStorage.deleteAll(keys);
    }

    @SneakyThrows
    public Map<Object, Object> loadAll(Collection<Object> keys) {
        Map<Object, Object> allMap = mapStorage.loadAll();
        Map<Object, Object> retMap = new HashMap<>();
        keys.forEach(key -> retMap.put(key, allMap.get(key)));

        return Collections.unmodifiableMap(retMap);
    }

    public Iterable<Object> loadAllKeys() {
        return mapStorage.loadAllKeys();
    }
}
