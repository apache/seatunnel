/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.seatunnel.connectors.seatunnel.aerospike.fastjson;

import java.util.HashMap;
import java.util.Map;

/** Stores hash-to-symbol IDs for the bundled Fastjson2 2.0.33 JSONB writer. */
public final class JsonbSymbolTable {
    private final Map<Long, Integer> symbols = new HashMap<>();

    public JsonbSymbolTable() {}

    public JsonbSymbolTable(long hash, int symbol) {
        put(hash, symbol);
    }

    /** Returns the cached symbol ID or {@code -1} if the hash is absent. Zero is always absent. */
    public int get(long hash) {
        return symbols.getOrDefault(hash, -1);
    }

    /** Stores or replaces the symbol ID for a nonzero hash. A zero hash is ignored. */
    public void put(long hash, int symbol) {
        if (hash != 0L) {
            symbols.put(hash, symbol);
        }
    }

    /**
     * Returns the cached symbol ID if present. Otherwise stores and returns {@code symbol}. For a
     * zero hash, returns {@code symbol} without caching it.
     */
    public int putIfAbsent(long hash, int symbol) {
        if (hash == 0L) {
            return symbol;
        }
        Integer existing = symbols.putIfAbsent(hash, symbol);
        return existing == null ? symbol : existing;
    }
}
