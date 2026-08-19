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

package org.apache.seatunnel.connectors.seatunnel.hugegraph.source;

import org.apache.seatunnel.api.source.SourceSplit;

import org.apache.hugegraph.structure.graph.Shard;

import java.util.Objects;

/**
 * A unit of read work for the HugeGraph source.
 *
 * <p>Two modes:
 *
 * <ul>
 *   <li>{@code LABEL_LIST}: a single split that pages the whole label via the server-side list API
 *       ({@code /vertices?label=&page=}). Used when parallelism == 1; preserves server-side label
 *       and property-equality filtering. There is exactly one such split per job.
 *   <li>{@code SHARD}: one split per HugeGraph key-range shard (from {@code
 *       traverser().vertexShards / edgeShards}). Used when parallelism &gt; 1 so shards can be
 *       scanned by multiple readers in parallel. The scan API returns all labels in the range, so
 *       the reader filters by label client-side; server-side property filters are not supported in
 *       this mode (rejected at the factory).
 * </ul>
 *
 * <p>The split also carries resumable progress ({@link #page}, {@link #finished}, {@link
 * #lastEmittedId}) so a reader can checkpoint mid-scan and continue from the same page after
 * failover. Identity ({@link #equals}/{@link #hashCode}) is on {@link #splitId} only; the mutable
 * progress fields are excluded so a split keeps its identity in the enumerator's sets as it
 * advances.
 */
public class HugeGraphSourceSplit implements SourceSplit {

    private static final long serialVersionUID = 1L;

    private final String splitId;
    private final boolean shardMode;
    // The label a LABEL_LIST split pages. null for SHARD splits, which scan all labels in a key
    // range and let the reader filter/route by label client-side.
    private final String label;
    // Shard bounds, only meaningful when shardMode == true. Shard itself is not Serializable, so we
    // store its three primitive components and rebuild it on demand.
    private final String shardStart;
    private final String shardEnd;
    private final long shardLength;

    // Resumable progress. page == null means "from the beginning"; the reader sends it as the empty
    // string to enter the server's paged mode.
    private String page;
    private boolean finished;
    private String lastEmittedId;

    /** Creates a label-list split that pages exactly {@code label}. */
    public static HugeGraphSourceSplit labelListSplit(String splitId, String label) {
        return new HugeGraphSourceSplit(splitId, false, label, null, null, 0L);
    }

    /** Creates a shard split (parallelism &gt; 1 path). Scans all labels in the key range. */
    public static HugeGraphSourceSplit shardSplit(String splitId, Shard shard) {
        return new HugeGraphSourceSplit(
                splitId, true, null, shard.start(), shard.end(), shard.length());
    }

    private HugeGraphSourceSplit(
            String splitId,
            boolean shardMode,
            String label,
            String shardStart,
            String shardEnd,
            long shardLength) {
        this.splitId = splitId;
        this.shardMode = shardMode;
        this.label = label;
        this.shardStart = shardStart;
        this.shardEnd = shardEnd;
        this.shardLength = shardLength;
        this.page = null;
        this.finished = false;
        this.lastEmittedId = null;
    }

    @Override
    public String splitId() {
        return splitId;
    }

    public boolean isShardMode() {
        return shardMode;
    }

    /** The label this split pages; null for shard splits (which scan all labels in a range). */
    public String getLabel() {
        return label;
    }

    /** Rebuilds the client {@link Shard} for a shard-mode split. */
    public Shard toShard() {
        return new Shard(shardStart, shardEnd, shardLength);
    }

    public String getPage() {
        return page;
    }

    public void setPage(String page) {
        this.page = page;
    }

    public boolean isFinished() {
        return finished;
    }

    public void setFinished(boolean finished) {
        this.finished = finished;
    }

    public String getLastEmittedId() {
        return lastEmittedId;
    }

    public void setLastEmittedId(String lastEmittedId) {
        this.lastEmittedId = lastEmittedId;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        return splitId.equals(((HugeGraphSourceSplit) o).splitId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(splitId);
    }

    @Override
    public String toString() {
        return "HugeGraphSourceSplit{"
                + "splitId='"
                + splitId
                + '\''
                + ", shardMode="
                + shardMode
                + ", page='"
                + page
                + '\''
                + ", finished="
                + finished
                + '}';
    }
}
