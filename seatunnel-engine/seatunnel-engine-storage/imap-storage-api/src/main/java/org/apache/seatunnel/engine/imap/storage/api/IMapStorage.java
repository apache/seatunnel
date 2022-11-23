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

package org.apache.seatunnel.engine.imap.storage.api;

import java.io.IOException;
import java.util.Collection;
import java.util.Map;
import java.util.Set;

public interface IMapStorage {

    public void initialize(Map<String, Object> properties);

    /**
     * Store a key-value pair in the map.
     * @param key storage key
     * @param value storage value
     * @return storage status, true is success, false is fail
     */
    public boolean store(Object key, Object value);

    /**
     * Store a key-value pair in the map storage.
     * @param map storage key-value pair
     * @return if some key-value pair is not stored, return this keys;
     *         if all key-value pair is stored, return empty set.
     */
    public Set<Object> storeAll(Map<Object, Object> map);

    /**
     * Delete a key  in the map storage.
     * @param key storage key
     * @return storage status, true is success, false is fail
     */
    public boolean delete(Object key);

    /**
     * Delete a collection of keys from the map storage.
     * @param  keys delete keys
     * @return if some keys delete fail, will return this keys
     *         if all keys delete success, will return empty set
     */
    public Set<Object> deleteAll(Collection<Object> keys);

    public Map<Object, Object> loadAll() throws IOException;

    public Set<Object> loadAllKeys();

    public void destroy();
}
