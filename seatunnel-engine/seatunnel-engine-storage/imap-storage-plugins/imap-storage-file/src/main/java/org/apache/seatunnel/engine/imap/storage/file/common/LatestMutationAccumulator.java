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
import org.apache.seatunnel.engine.imap.storage.file.compaction.CompactionDeferredException;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/** Retains only the latest WAL mutation for each serialized key. */
class LatestMutationAccumulator {

    private final Map<SerializedKey, IMapFileData> latestMutations = new HashMap<>();
    private final Set<SerializedKey> searchKeys;
    private final Set<SerializedKey> ambiguousExpirationTimes = new HashSet<>();
    private long retainedBytes;

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
        accept(mutation, serializedKey, current);
    }

    /** Checks the retained working set before adding or replacing a mutation. */
    void acceptWithinLimits(IMapFileData mutation, long maxMemoryBytes, long maxKeys)
            throws CompactionDeferredException {
        SerializedKey serializedKey =
                SerializedKey.of(mutation.getKey(), mutation.getKeyClassName());
        if (searchKeys != null && !searchKeys.contains(serializedKey)) {
            return;
        }
        IMapFileData current = latestMutations.get(serializedKey);
        if (current == null && latestMutations.size() >= maxKeys) {
            throw new CompactionDeferredException(
                    "Compaction deferred: distinct keys exceed compaction.max-keys=" + maxKeys);
        }
        if (current == null || mutation.compareTo(current) < 0) {
            long replacedBytes = current == null ? 0 : estimateRetainedBytes(current);
            long remainingBytes = maxMemoryBytes - (retainedBytes - replacedBytes);
            if (estimateRetainedBytes(mutation) > remainingBytes) {
                throw new CompactionDeferredException(
                        "Compaction deferred: retained mutations exceed compaction.max-memory-bytes="
                                + maxMemoryBytes);
            }
        }
        accept(mutation, serializedKey, current);
    }

    private void accept(IMapFileData mutation, SerializedKey serializedKey, IMapFileData current) {
        if (current == null) {
            latestMutations.put(serializedKey, mutation);
            retainedBytes += estimateRetainedBytes(mutation);
            return;
        }
        int comparison = mutation.compareTo(current);
        if (comparison < 0) {
            latestMutations.put(serializedKey, mutation);
            retainedBytes += estimateRetainedBytes(mutation) - estimateRetainedBytes(current);
            ambiguousExpirationTimes.remove(serializedKey);
        } else if (comparison == 0
                && !Objects.equals(mutation.getExpirationTime(), current.getExpirationTime())) {
            // compareTo predates persisted TTLs and does not order this otherwise-equal pair.
            // Reordering it into a snapshot could change the expiry selected by replay.
            ambiguousExpirationTimes.add(serializedKey);
        }
    }

    /**
     * Estimates serialized payloads, class names and per-key map/object bookkeeping. Actual heap
     * usage also depends on the serializer and JVM; this guard is not an exact heap reservation.
     */
    private static long estimateRetainedBytes(IMapFileData mutation) {
        // HashMap retains its original SerializedKey on replacement; the latest mutation and
        // the ambiguity set can each retain another copy of its key bytes and class name.
        return 512L
                + (mutation.getKey() == null ? 0 : 3L * mutation.getKey().length)
                + (mutation.getValue() == null ? 0 : mutation.getValue().length)
                + (mutation.getKeyClassName() == null
                        ? 0
                        : 6L * mutation.getKeyClassName().length())
                + (mutation.getValueClassName() == null
                        ? 0
                        : 2L * mutation.getValueClassName().length());
    }

    boolean hasAmbiguousExpirationTimes() {
        return !ambiguousExpirationTimes.isEmpty();
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
