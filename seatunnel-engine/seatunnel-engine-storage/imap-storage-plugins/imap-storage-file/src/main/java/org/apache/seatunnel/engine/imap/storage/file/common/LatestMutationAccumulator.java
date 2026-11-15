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

package org.apache.seatunnel.engine.imap.storage.file.common;

import org.apache.seatunnel.engine.imap.storage.file.bean.IMapFileData;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/** Retains only the latest WAL mutation for each serialized key. */
class LatestMutationAccumulator {

    private final Map<SerializedKey, IMapFileData> latestMutations = new HashMap<>();
    private final Set<SerializedKey> searchKeys;

    LatestMutationAccumulator(Set<SerializedKey> searchKeys) {
        this.searchKeys = searchKeys;
    }

    void accept(IMapFileData mutation) {
        SerializedKey serializedKey =
                SerializedKey.of(mutation.getKey(), mutation.getKeyClassName());
        if (searchKeys != null && !searchKeys.contains(serializedKey)) {
            return;
        }

        IMapFileData current = latestMutations.get(serializedKey);
        if (current == null) {
            latestMutations.put(serializedKey, mutation);
            return;
        }
        if (mutation.compareTo(current) < 0) {
            latestMutations.put(serializedKey, mutation);
        }
    }

    int size() {
        return latestMutations.size();
    }

    Collection<IMapFileData> values() {
        return latestMutations.values();
    }

    Iterator<IMapFileData> iterator() {
        return latestMutations.values().iterator();
    }

    static final class SerializedKey {
        private final byte[] key;
        private final String className;
        private final int hashCode;

        private SerializedKey(byte[] key, String className) {
            this.key = key;
            this.className = className;
            this.hashCode = 31 * Arrays.hashCode(key) + Objects.hashCode(className);
        }

        static SerializedKey of(byte[] key, String className) {
            return new SerializedKey(key, className);
        }

        @Override
        public boolean equals(Object object) {
            if (this == object) {
                return true;
            }
            if (!(object instanceof SerializedKey)) {
                return false;
            }
            SerializedKey that = (SerializedKey) object;
            return Arrays.equals(key, that.key) && Objects.equals(className, that.className);
        }

        @Override
        public int hashCode() {
            return hashCode;
        }
    }
}
