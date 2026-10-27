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
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public interface IMapStorage {

    public void initialize(Map<String, Object> properties);

    /**
     * Store a key-value pair in the map. todo: it's better add timeout parameter
     *
     * @param key storage key
     * @param value storage value
     * @return storage status, true is success, false is fail
     */
    public boolean store(Object key, Object value);

    /**
     * Store a key-value pair in the map storage.
     *
     * @param map storage key-value pair
     * @return if some key-value pair is not stored, return this keys; if all key-value pair is
     *     stored, return empty set.
     */
    public Set<Object> storeAll(Map<Object, Object> map);

    /**
     * Store a key-value pair that expires at the given time.
     *
     * <p>Storages that do not persist expiration times store the pair without it, so the entry
     * never expires after it is loaded again.
     *
     * @param key storage key
     * @param value storage value
     * @param expirationTime epoch milliseconds at which the entry expires, or {@link
     *     ExpirableValue#NO_EXPIRATION_TIME}
     * @return storage status, true is success, false is fail
     */
    default boolean store(Object key, Object value, long expirationTime) {
        return store(key, value);
    }

    /**
     * Store key-value pairs together with the times at which they expire.
     *
     * @param map storage key to value and expiration time
     * @return if some key-value pair is not stored, return this keys; if all key-value pair is
     *     stored, return empty set.
     * @see #store(Object, Object, long)
     */
    default Set<Object> storeAllWithExpirationTime(Map<Object, ExpirableValue> map) {
        Map<Object, Object> values = new HashMap<>(map.size());
        map.forEach((key, value) -> values.put(key, value.getValue()));
        return storeAll(values);
    }

    /**
     * Delete a key in the map storage.
     *
     * @param key storage key
     * @return storage status, true is success, false is fail
     */
    public boolean delete(Object key);

    /**
     * Delete a collection of keys from the map storage.
     *
     * @param keys delete keys
     * @return if some keys delete fail, will return this keys if all keys delete success, will
     *     return empty set
     */
    public Set<Object> deleteAll(Collection<Object> keys);

    public Map<Object, Object> loadAll() throws IOException;

    /**
     * Load all entries together with the times at which they expire. Entries that have already
     * expired are not returned.
     *
     * <p>Storages that do not persist expiration times return every entry with {@link
     * ExpirableValue#NO_EXPIRATION_TIME}.
     *
     * @return storage key to value and expiration time
     */
    default Map<Object, ExpirableValue> loadAllWithExpirationTime() throws IOException {
        Map<Object, Object> values = loadAll();
        Map<Object, ExpirableValue> result = new HashMap<>(values.size());
        values.forEach((key, value) -> result.put(key, new ExpirableValue(value)));
        return result;
    }

    public Set<Object> loadAllKeys();

    public void destroy(boolean deleteAllFileFlag);
}
