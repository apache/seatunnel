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

import lombok.Data;

import java.util.Objects;

@Data
public class RowKeyWithTs implements Comparable<RowKeyWithTs> {
    private final long timestamp;
    private final RowKey rowKey;

    private RowKeyWithTs(final long timestamp, final RowKey rowKey) {
        this.timestamp = timestamp;
        this.rowKey = rowKey;
    }

    private RowKeyWithTs(final long timestamp, final byte[] key) {
        this(timestamp, RowKey.decode(key));
    }

    @Override
    public int compareTo(final RowKeyWithTs that) {
        int res = Long.compare(this.timestamp, that.timestamp);
        if (res == 0) {
            res = Long.compare(this.rowKey.getTableId(), that.rowKey.getTableId());
        }
        if (res == 0) {
            res = Long.compare(this.rowKey.getHandle(), that.rowKey.getHandle());
        }
        return res;
    }

    @Override
    public int hashCode() {
        return Objects.hash(this.timestamp, this.rowKey.getTableId(), this.rowKey.getHandle());
    }

    @Override
    public boolean equals(final Object thatObj) {
        if (thatObj instanceof RowKeyWithTs) {
            final RowKeyWithTs that = (RowKeyWithTs) thatObj;
            return this.timestamp == that.timestamp && this.rowKey.equals(that.rowKey);
        }
        return false;
    }

    static RowKeyWithTs ofStart(final Cdcpb.Event.Row row) {
        return new RowKeyWithTs(row.getStartTs(), row.getKey().toByteArray());
    }

    static RowKeyWithTs ofCommit(final Cdcpb.Event.Row row) {
        return new RowKeyWithTs(row.getCommitTs(), row.getKey().toByteArray());
    }
}
