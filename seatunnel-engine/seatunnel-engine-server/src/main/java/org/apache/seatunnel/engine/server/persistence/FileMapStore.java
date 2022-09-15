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

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.map.MapLoaderLifecycleSupport;
import com.hazelcast.map.MapStore;

import java.util.Collection;
import java.util.Map;
import java.util.Properties;

public class FileMapStore implements MapStore<Object, Object>, MapLoaderLifecycleSupport {
    //TODO Wait for the file Kv storage development to complete

    @Override
    public void init(HazelcastInstance hazelcastInstance, Properties properties, String mapName) {
    }

    @Override
    public void destroy() {

    }

    @Override
    public void store(Object key, Object value) {
    }

    @Override
    public void storeAll(Map<Object, Object> map) {

    }

    @Override
    public void delete(Object key) {

    }

    @Override
    public void deleteAll(Collection<Object> keys) {

    }

    @Override
    public String load(Object key) {
        return null;
    }

    @Override
    public Map<Object, Object> loadAll(Collection<Object> keys) {
        return null;
    }

    @Override
    public Iterable<Object> loadAllKeys() {
        return null;
    }

}
