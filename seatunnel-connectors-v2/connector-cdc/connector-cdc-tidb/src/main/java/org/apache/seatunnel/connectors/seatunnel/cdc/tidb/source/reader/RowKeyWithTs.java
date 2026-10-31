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

package org.apache.seatunnel.connectors.seatunnel.cdc.tidb.source.reader;

import org.tikv.common.key.RowKey;
import org.tikv.kvproto.Cdcpb;
import org.tikv.shade.com.google.protobuf.ByteString;

import java.util.Objects;

/**
 * Identifies transaction rows by timestamp and their complete immutable TiKV record keys. Comparing
 * an integer prefix would collapse different composite handles before deserialization.
 */
public class RowKeyWithTs implements Comparable<RowKeyWithTs> {
    private final long timestamp;
    private final ByteString recordKey;

    private RowKeyWithTs(long timestamp, ByteString recordKey) {
        this.timestamp = timestamp;
        this.recordKey = recordKey;
    }

    public long getTimestamp() {
        return timestamp;
    }

    /**
     * Preserves the integer-handle accessor for existing callers of this public class. Transaction
     * buffering deliberately compares opaque keys and never calls this legacy accessor.
     */
    public RowKey getRowKey() {
        return RowKey.decode(recordKey.toByteArray());
    }

    @Override
    public int compareTo(RowKeyWithTs that) {
        int result = Long.compare(timestamp, that.timestamp);
        if (result != 0) {
            return result;
        }
        // Prefix-only integer decoding truncates composite keys and loses distinct transaction
        // rows.
        int size = Math.min(recordKey.size(), that.recordKey.size());
        for (int i = 0; i < size; i++) {
            result = Integer.compare(recordKey.byteAt(i) & 0xff, that.recordKey.byteAt(i) & 0xff);
            if (result != 0) {
                return result;
            }
        }
        return Integer.compare(recordKey.size(), that.recordKey.size());
    }

    @Override
    public int hashCode() {
        return Objects.hash(timestamp, recordKey);
    }

    @Override
    public boolean equals(Object object) {
        if (!(object instanceof RowKeyWithTs)) {
            return false;
        }
        RowKeyWithTs that = (RowKeyWithTs) object;
        return timestamp == that.timestamp && recordKey.equals(that.recordKey);
    }

    static RowKeyWithTs ofStart(Cdcpb.Event.Row row) {
        return new RowKeyWithTs(row.getStartTs(), row.getKey());
    }

    static RowKeyWithTs ofCommit(Cdcpb.Event.Row row) {
        return new RowKeyWithTs(row.getCommitTs(), row.getKey());
    }
}
