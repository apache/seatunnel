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

package org.apache.seatunnel.connectors.seatunnel.cdc.base.options;

import static com.google.common.base.Preconditions.checkNotNull;

import java.io.Serializable;
import java.util.Objects;

/** SeaTunnel CDC Connector startup options. */
public final class StartupOptions implements Serializable {
    private static final long serialVersionUID = 1L;

    public final StartupMode startupMode;
    public final String specificOffsetFile;
    public final Integer specificOffsetPos;
    public final Long startupTimestampMillis;

    /**
     * Performs an initial snapshot on the monitored database tables upon first startup, and
     * continue to read the latest change log.
     */
    public static StartupOptions initial() {
        return new StartupOptions(StartupMode.INITIAL, null, null, null);
    }

    /**
     * Never to perform snapshot on the monitored database tables upon first startup, just read from
     * the beginning of the change log. This should be used with care, as it is only valid when the
     * change log is guaranteed to contain the entire history of the database.
     */
    public static StartupOptions earliest() {
        return new StartupOptions(StartupMode.EARLIEST_OFFSET, null, null, null);
    }

    /**
     * Never to perform snapshot on the monitored database tables upon first startup, just read from
     * the end of the change log which means only have the changes since the connector was started.
     */
    public static StartupOptions latest() {
        return new StartupOptions(StartupMode.LATEST_OFFSET, null, null, null);
    }

    /**
     * Never to perform snapshot on the monitored database tables upon first startup, and directly
     * read change log from the specified offset.
     */
    public static StartupOptions specificOffset(String specificOffsetFile, int specificOffsetPos) {
        return new StartupOptions(
                StartupMode.SPECIFIC_OFFSETS, specificOffsetFile, specificOffsetPos, null);
    }

    /**
     * Never to perform snapshot on the monitored database tables upon first startup, and directly
     * read change log from the specified timestamp.
     *
     * <p>The consumer will traverse the change log from the beginning and ignore change events
     * whose timestamp is smaller than the specified timestamp.
     *
     * @param startupTimestampMillis timestamp for the startup offsets, as milliseconds from epoch.
     */
    public static StartupOptions timestamp(long startupTimestampMillis) {
        return new StartupOptions(StartupMode.TIMESTAMP, null, null, startupTimestampMillis);
    }

    private StartupOptions(
            StartupMode startupMode,
            String specificOffsetFile,
            Integer specificOffsetPos,
            Long startupTimestampMillis) {
        this.startupMode = startupMode;
        this.specificOffsetFile = specificOffsetFile;
        this.specificOffsetPos = specificOffsetPos;
        this.startupTimestampMillis = startupTimestampMillis;

        switch (startupMode) {
            case INITIAL:
            case EARLIEST_OFFSET:
            case LATEST_OFFSET:
                break;
            case SPECIFIC_OFFSETS:
                checkNotNull(specificOffsetFile, "specificOffsetFile shouldn't be null");
                checkNotNull(specificOffsetPos, "specificOffsetPos shouldn't be null");
                break;
            case TIMESTAMP:
                checkNotNull(startupTimestampMillis, "startupTimestampMillis shouldn't be null");
                break;
            default:
                throw new UnsupportedOperationException(startupMode + " mode is not supported.");
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        StartupOptions that = (StartupOptions) o;
        return startupMode == that.startupMode
                && Objects.equals(specificOffsetFile, that.specificOffsetFile)
                && Objects.equals(specificOffsetPos, that.specificOffsetPos)
                && Objects.equals(startupTimestampMillis, that.startupTimestampMillis);
    }

    @Override
    public int hashCode() {
        return Objects.hash(
                startupMode, specificOffsetFile, specificOffsetPos, startupTimestampMillis);
    }
}
