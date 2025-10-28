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

package org.apache.seatunnel.engine.server.rocksdb;

import org.apache.seatunnel.engine.common.config.server.MapStoreConfig;

import java.util.Map;

public class RocksDBService {
    private final String dbPath;
    private final RocksDBStateBackend stateBackend;

    public RocksDBService(String dbPath, MapStoreConfig config) {
        this.dbPath = dbPath;
        this.stateBackend = BackendFactory.createRocksDBStateBackend(dbPath, config);
        stateBackend.init();
    }

    public <K, V> RocksDBValueState<K, V> getValueState(String stateName) {
        return stateBackend.getValueState(stateName);
    }

    public <K, V> Map<K, V> getAllData(String stateName) {
        RocksDBValueState<K, V> valueState = stateBackend.getValueState(stateName);
        return RocksDBUtils.toMap(valueState.iterator());
    }

    public <K, V> V getData(String stateName, K key) {
        RocksDBValueState<K, V> valueState = stateBackend.getValueState(stateName);
        return valueState.get(key);
    }

    public <K, V> void putData(String stateName, Map<K, V> map) {
        for (Map.Entry<K, V> e : map.entrySet()) {
            stateBackend.put(stateName, e.getKey(), e.getValue());
        }
    }

    public <K> void removeData(String stateName, K key) {
        stateBackend.remove(stateName, key);
    }

    public void close() {
        stateBackend.close(dbPath);
    }
}
