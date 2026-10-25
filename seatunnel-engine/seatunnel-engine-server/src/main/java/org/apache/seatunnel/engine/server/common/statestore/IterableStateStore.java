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

package org.apache.seatunnel.engine.server.common.statestore;

import java.util.Collection;
import java.util.Map;
import java.util.Set;

/**
 * Backend storage capability for full iteration.
 *
 * <p>Operations that read all values can have very different costs depending on the backend, so
 * this capability is separated from the base {@link StateStore}.
 *
 * <p>This is better treated as a backend SPI used internally by higher-level stores such as history
 * or metrics stores, rather than as an engine-facing contract.
 *
 * @param <K> key type
 * @param <V> value type
 */
public interface IterableStateStore<K, V> extends StateStore<K, V> {

    /** @return all stored entries */
    Set<Map.Entry<K, V>> entrySet();

    /** @return all stored values */
    Collection<V> values();

    /**
     * Returns whether the store is empty.
     *
     * @return {@code true} if the store is empty
     */
    boolean isEmpty();

    /**
     * Returns the total number of entries.
     *
     * @return entry count
     */
    int size();
}
