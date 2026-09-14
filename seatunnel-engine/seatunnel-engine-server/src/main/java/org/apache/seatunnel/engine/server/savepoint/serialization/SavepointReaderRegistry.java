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

package org.apache.seatunnel.engine.server.savepoint.serialization;

import org.apache.seatunnel.engine.checkpoint.storage.savepoint.SavepointMeta;
import org.apache.seatunnel.engine.checkpoint.storage.savepoint.SavepointStorageConstants;

import java.util.HashMap;
import java.util.Map;

/**
 * Dispatches savepoint bundles to the reader registered for their format version.
 *
 * <p>Compatibility policy: single direction. New engines read savepoints written by the same or
 * older format versions down to {@link #MIN_SUPPORTED_FORMAT_VERSION}; bundles below the window
 * require a migration tool, bundles above it were written by a newer engine and are rejected with
 * an explicit error - never a guess.
 */
public final class SavepointReaderRegistry {

    /** Oldest format version the current engine can still read. */
    public static final int MIN_SUPPORTED_FORMAT_VERSION = 1;

    /**
     * Readers keyed by the bundle format version ({@link SavepointStorageConstants#FORMAT_VERSION}
     * - single source of truth).
     */
    private static final Map<Integer, SavepointReader> READERS;

    static {
        Map<Integer, SavepointReader> readers = new HashMap<>();
        readers.put(SavepointStorageConstants.FORMAT_VERSION, new SavepointReaderV1());
        READERS = java.util.Collections.unmodifiableMap(readers);
    }

    private SavepointReaderRegistry() {}

    /** Current bundle format version = {@link SavepointStorageConstants#FORMAT_VERSION}. */
    public static int currentFormatVersion() {
        return SavepointStorageConstants.FORMAT_VERSION;
    }

    public static SavepointReader forVersion(SavepointMeta meta) {
        int formatVersion = meta.getFormatVersion();
        SavepointReader reader = READERS.get(formatVersion);
        if (reader != null) {
            return reader;
        }
        if (formatVersion < MIN_SUPPORTED_FORMAT_VERSION) {
            throw new SavepointIncompatibleException(
                    meta.getSavepointId(),
                    formatVersion,
                    "format version is older than the supported window ["
                            + MIN_SUPPORTED_FORMAT_VERSION
                            + ", "
                            + currentFormatVersion()
                            + "]; a migration tool is required");
        }
        throw new SavepointIncompatibleException(
                meta.getSavepointId(),
                formatVersion,
                "savepoint was written by a newer engine; supported versions: ["
                        + MIN_SUPPORTED_FORMAT_VERSION
                        + ", "
                        + currentFormatVersion()
                        + "]");
    }
}
