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

package org.apache.seatunnel.engine.server.persistence;

import org.apache.seatunnel.engine.common.utils.FactoryUtil;
import org.apache.seatunnel.engine.imap.storage.api.IMapStorage;
import org.apache.seatunnel.engine.imap.storage.api.IMapStorageFactory;

import com.google.common.collect.Maps;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.map.MapLoaderLifecycleSupport;
import com.hazelcast.map.MapStore;
import lombok.SneakyThrows;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class FileMapStore implements MapStore<Object, Object>, MapLoaderLifecycleSupport {

    private IMapStorage mapStorage;

    @Override
    public void init(HazelcastInstance hazelcastInstance, Properties properties, String mapName) {

        Map<String, Object> initMap = new HashMap<>(Maps.fromProperties(properties));
        this.mapStorage =
                FactoryUtil.discoverFactory(
                                Thread.currentThread().getContextClassLoader(),
                                IMapStorageFactory.class,
                                (String) initMap.get("type"))
                        .create(initMap);
    }

    @Override
    public void destroy() {
        mapStorage.destroy(false);
    }

    @Override
    public void store(Object key, Object value) {
        mapStorage.store(key, value);
    }

    @Override
    public void storeAll(Map<Object, Object> map) {
        mapStorage.storeAll(map);
    }

    @Override
    public void delete(Object key) {
        mapStorage.delete(key);
    }

    @Override
    public void deleteAll(Collection<Object> keys) {
        mapStorage.deleteAll(keys);
    }

    @SneakyThrows
    @Override
    public Object load(Object key) {
        return null;
    }

    @SneakyThrows
    @Override
    public Map<Object, Object> loadAll(Collection<Object> keys) {
        Map<Object, Object> allMap = mapStorage.loadAll();
        Map<Object, Object> retMap = new HashMap<>();
        keys.forEach(key -> retMap.put(key, allMap.get(key)));

        return Collections.unmodifiableMap(retMap);
    }

    @Override
    public Iterable<Object> loadAllKeys() {
        return mapStorage.loadAllKeys();
    }
}
