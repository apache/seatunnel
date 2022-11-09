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

package org.apache.seatunnel.connectors.seatunnel.cdc.base.source.meta.split;

import org.apache.seatunnel.api.source.SourceSplit;

import io.debezium.relational.TableId;
import io.debezium.relational.history.TableChanges.TableChange;

import java.util.Map;
import java.util.Objects;

/** The split of table comes from a Table that splits by primary key. */

public abstract class SourceSplitBase implements SourceSplit {

    protected final String splitId;

    public SourceSplitBase(String splitId) {
        this.splitId = splitId;
    }

    /** Checks whether this split is a snapshot split. */
    public final boolean isSnapshotSplit() {
        return getClass() == SnapshotSplit.class || getClass() == SchemalessSnapshotSplit.class;
    }

    /** Checks whether this split is a stream split. */
    public final boolean isStreamSplit() {
        return getClass() == StreamSplit.class;
    }

    /** Casts this split into a {@link SnapshotSplit}. */
    public final SnapshotSplit asSnapshotSplit() {
        return (SnapshotSplit) this;
    }

    /** Casts this split into a {@link StreamSplit}. */
    public final StreamSplit asStreamSplit() {
        return (StreamSplit) this;
    }

    @Override
    public String splitId() {
        return splitId;
    }

    public abstract Map<TableId, TableChange> getTableSchemas();

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        SourceSplitBase that = (SourceSplitBase) o;
        return Objects.equals(splitId, that.splitId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(splitId);
    }
}
