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

package org.apache.seatunnel.engine.server.persistence;

import org.apache.seatunnel.engine.imap.storage.api.RocksDBStorageFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class FileMapStoreManager {
    private final FileMapStoreFactory factory = new FileMapStoreFactory();
    private final Map<String, FileMapStore> mapStores = new HashMap<>();

    public FileMapStoreManager(
            List<String> stateNames,
            RocksDBStorageFactory storageFactory,
            Map<String, Object> configuration) {
        for (String name : stateNames) {
            FileMapStore fileMapStore = factory.newMapStore(storageFactory, configuration, name);
            mapStores.put(name, fileMapStore);
        }
    }

    public void put(String name, Object key, Object value) {
        mapStores.get(name).store(key, value);
    }

    public void remove(String name, Object key) {
        mapStores.get(name).delete(key);
    }

    public Map<Object, Object> loadAll(String name) {
        Iterable<Object> loadedAllKeys = mapStores.get(name).loadAllKeys();
        List<Object> keys = new ArrayList<>();
        for (Object k : loadedAllKeys) {
            keys.add(k);
        }
        return mapStores.get(name).loadAll(keys);
    }

    public void destroy() {
        for (FileMapStore store : mapStores.values()) {
            store.destroy();
        }
    }
}
