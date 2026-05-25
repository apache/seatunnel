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

package org.apache.seatunnel.api.sink;

import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.schema.event.SchemaChangeEvent;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;

import java.io.IOException;
import java.util.List;
import java.util.Optional;

public final class DirtyDataAwareSinkWriter<T, CommitInfoT, StateT>
        implements SinkWriter<T, CommitInfoT, StateT> {

    private final SinkWriter<T, CommitInfoT, StateT> delegate;
    private final DirtyRecordCollector collector;
    private final int subtaskIndex;
    private final CatalogTable catalogTable;

    public DirtyDataAwareSinkWriter(
            SinkWriter<T, CommitInfoT, StateT> delegate,
            DirtyRecordCollector collector,
            int subtaskIndex) {
        this(delegate, collector, subtaskIndex, null);
    }

    public DirtyDataAwareSinkWriter(
            SinkWriter<T, CommitInfoT, StateT> delegate,
            DirtyRecordCollector collector,
            int subtaskIndex,
            CatalogTable catalogTable) {
        this.delegate = delegate;
        this.collector = collector;
        this.subtaskIndex = subtaskIndex;
        this.catalogTable = catalogTable;
    }

    @Override
    public void write(T element) throws IOException {
        if (element instanceof SeaTunnelRow
                && collector.validateAndCollectIfDirty(
                        subtaskIndex, (SeaTunnelRow) element, catalogTable)) {
            return;
        }
        try {
            delegate.write(element);
        } catch (Exception e) {
            if (!tryCollect(element, e)) {
                if (e instanceof IOException) {
                    throw (IOException) e;
                }
                throw new IOException(e);
            }
        }
    }

    private boolean tryCollect(T element, Exception e) {
        if (collector == null || collector instanceof NoOpDirtyRecordCollector) {
            return false;
        }
        collector.collect(
                subtaskIndex, element, e, "Write failed: " + e.getMessage(), catalogTable);
        return true;
    }

    @Override
    public void applySchemaChange(SchemaChangeEvent event) throws IOException {
        delegate.applySchemaChange(event);
    }

    @Override
    public Optional<CommitInfoT> prepareCommit() throws IOException {
        return delegate.prepareCommit();
    }

    @Override
    public Optional<CommitInfoT> prepareCommit(long checkpointId) throws IOException {
        return delegate.prepareCommit(checkpointId);
    }

    @Override
    public List<StateT> snapshotState(long checkpointId) throws IOException {
        return delegate.snapshotState(checkpointId);
    }

    @Override
    public void abortPrepare() {
        delegate.abortPrepare();
    }

    @Override
    public void close() throws IOException {
        IOException closeException = null;
        try {
            delegate.close();
        } catch (IOException e) {
            closeException = e;
            throw e;
        } finally {
            try {
                collector.close();
            } catch (Exception e) {
                if (closeException != null) {
                    closeException.addSuppressed(e);
                } else {
                    throw new IOException("Failed to close dirty record collector", e);
                }
            }
        }
    }
}
