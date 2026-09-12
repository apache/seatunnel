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

package org.apache.seatunnel.connectors.seatunnel.bigtable.source;

import org.apache.seatunnel.api.source.SourceSplit;

import java.io.Serializable;

/**
 * Describes a bounded Bigtable scan split and optional in-split resume progress.
 *
 * <p>When {@link #lastReadRowKey} is present, checkpoint restore resumes from that row key
 * (inclusive) instead of re-scanning from {@link #startRowKey}. Semantics are at-least-once: the
 * resume row may be read again after failover.
 */
public class BigtableSourceSplit implements SourceSplit, Serializable {

    /** Kept at {@code 1L} so existing checkpoint/savepoint bytes remain deserializable. */
    private static final long serialVersionUID = 1L;

    public static final String SPLIT_PREFIX = "bigtable_source_split_";

    private final String splitId;
    /** Inclusive start row key (empty means table start). */
    private final String startRowKey;
    /** Exclusive end row key (empty means table end). */
    private final String endRowKey;

    /**
     * Last successfully emitted row key (UTF-8), persisted in checkpoint state. {@code null} for
     * legacy checkpoints written before row-level resume was added.
     */
    private volatile String lastReadRowKey;

    public BigtableSourceSplit(int splitIndex, String startRowKey, String endRowKey) {
        this(splitIndex, startRowKey, endRowKey, null);
    }

    public BigtableSourceSplit(
            int splitIndex, String startRowKey, String endRowKey, String lastReadRowKey) {
        this.splitId = SPLIT_PREFIX + splitIndex;
        this.startRowKey = startRowKey;
        this.endRowKey = endRowKey;
        this.lastReadRowKey = lastReadRowKey;
    }

    @Override
    public String splitId() {
        return splitId;
    }

    public String getStartRowKey() {
        return startRowKey;
    }

    public String getEndRowKey() {
        return endRowKey;
    }

    public String getLastReadRowKey() {
        return lastReadRowKey;
    }

    /**
     * Updates in-split progress; called under the same lock as {@code collect()} so snapshots see
     * the last emitted row.
     *
     * @param rowKeyUtf8 current row key (UTF-8)
     */
    void setLastReadRowKey(String rowKeyUtf8) {
        this.lastReadRowKey = rowKeyUtf8;
    }

    /**
     * Row key used as the inclusive scan start when building a Bigtable {@code Query}.
     *
     * <p>Returns {@link #lastReadRowKey} when set, otherwise {@link #startRowKey}.
     */
    public String getResumeStartRowKey() {
        if (lastReadRowKey != null && !lastReadRowKey.isEmpty()) {
            return lastReadRowKey;
        }
        return startRowKey;
    }

    @Override
    public String toString() {
        return String.format(
                "{\"split_id\":\"%s\", \"start\":\"%s\", \"end\":\"%s\", \"last_read\":\"%s\"}",
                splitId, startRowKey, endRowKey, lastReadRowKey);
    }
}
