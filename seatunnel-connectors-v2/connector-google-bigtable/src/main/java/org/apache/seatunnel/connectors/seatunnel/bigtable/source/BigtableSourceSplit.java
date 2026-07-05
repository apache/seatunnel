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
 * Bigtable Source 分片定义：描述一次范围扫描的边界与可选的片内续读进度。
 *
 * <p>checkpoint 恢复时，若 {@link #lastReadRowKey} 非空，Reader 会从该 rowkey（含）继续扫描， 避免对整个 split 从头重扫。语义为
 * at-least-once，恢复后可能重复读取最后一条已提交行。
 */
public class BigtableSourceSplit implements SourceSplit, Serializable {

    private static final long serialVersionUID = 2L;
    public static final String SPLIT_PREFIX = "bigtable_source_split_";

    private final String splitId;
    /** 分片起始 rowkey（含）；空串表示表起点。 */
    private final String startRowKey;
    /** 分片结束 rowkey（不含）；空串表示表终点。 */
    private final String endRowKey;

    /**
     * 已成功 emit 的最后一条 rowkey（UTF-8）。checkpoint 时随 split 持久化；旧 checkpoint 反序列化后为 {@code null}，行为与仅使用
     * {@link #startRowKey} 一致。
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
     * 在读路径中更新片内进度；与 {@code checkpointLock} 在同一把锁内调用，保证快照能看到已 emit 的最后行。
     *
     * @param rowKeyUtf8 当前行的 rowkey（UTF-8）
     */
    void setLastReadRowKey(String rowKeyUtf8) {
        this.lastReadRowKey = rowKeyUtf8;
    }

    /**
     * 构造 {@link com.google.cloud.bigtable.data.v2.models.Query} 时使用的起始 rowkey。
     *
     * <p>有续读进度时返回 {@link #lastReadRowKey}，否则返回 {@link #startRowKey}。
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
