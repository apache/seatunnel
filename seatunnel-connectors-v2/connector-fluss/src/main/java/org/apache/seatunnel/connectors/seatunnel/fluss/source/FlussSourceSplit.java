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
package org.apache.seatunnel.connectors.seatunnel.fluss.source;

import org.apache.seatunnel.api.source.SourceSplit;
import org.apache.seatunnel.api.table.catalog.TablePath;

import lombok.Getter;

import java.util.Objects;

@Getter
public class FlussSourceSplit implements SourceSplit {

    private static final long serialVersionUID = 1L;

    private final TablePath tablePath;
    private final long tableId;
    private final int bucketId;
    private final long startOffset;
    private final long endOffset;

    public FlussSourceSplit(
            TablePath tablePath, long tableId, int bucketId, long startOffset, long endOffset) {
        this.tablePath = tablePath;
        this.tableId = tableId;
        this.bucketId = bucketId;
        this.startOffset = startOffset;
        this.endOffset = endOffset;
    }

    @Override
    public String splitId() {
        return tablePath.getFullName() + "-" + bucketId;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        FlussSourceSplit that = (FlussSourceSplit) o;
        return tableId == that.tableId && bucketId == that.bucketId;
    }

    @Override
    public int hashCode() {
        return Objects.hash(tableId, bucketId);
    }

    @Override
    public String toString() {
        return "FlussSourceSplit{"
                + "tablePath="
                + tablePath.getFullName()
                + ", bucketId="
                + bucketId
                + ", startOffset="
                + startOffset
                + ", endOffset="
                + endOffset
                + '}';
    }
}
