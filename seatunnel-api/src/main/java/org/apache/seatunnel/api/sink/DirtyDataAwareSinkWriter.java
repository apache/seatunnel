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

import org.apache.seatunnel.api.common.error.RowErrorClassification;
import org.apache.seatunnel.api.common.error.SupportRowLevelErrorClassifier;
import org.apache.seatunnel.api.sink.multitablesink.MultiTableSinkWriter;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.schema.event.SchemaChangeEvent;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;

import java.io.IOException;
import java.util.List;
import java.util.Optional;

/**
 * Applies explicit dirty validation and collects classified row errors while preserving fatal
 * writer failures, checkpoint state and commit boundaries.
 */
public class DirtyDataAwareSinkWriter<T, CommitInfoT, StateT>
        implements SinkWriter<T, CommitInfoT, StateT> {

    private final SinkWriter<T, CommitInfoT, StateT> delegate;
    private final DirtyRecordCollector collector;
    private final int subtaskIndex;
    private final CatalogTable catalogTable;

    DirtyDataAwareSinkWriter(
            SinkWriter<T, CommitInfoT, StateT> delegate,
            DirtyRecordCollector collector,
            int subtaskIndex) {
        this(delegate, collector, subtaskIndex, null);
    }

    DirtyDataAwareSinkWriter(
            SinkWriter<T, CommitInfoT, StateT> delegate,
            DirtyRecordCollector collector,
            int subtaskIndex,
            CatalogTable catalogTable) {
        this.delegate = delegate;
        this.collector = collector;
        this.subtaskIndex = subtaskIndex;
        this.catalogTable = catalogTable;
    }

    /**
     * Selects a synchronous wrapper while retaining the delegate's schema-evolution capability.
     *
     * <p>The no-feature path returns the original writer. Unsupported asynchronous and multi-table
     * writers fail before accepting rows instead of hiding their terminal-outcome protocol.
     */
    public static <T, CommitInfoT, StateT> SinkWriter<T, CommitInfoT, StateT> wrap(
            SinkWriter<T, CommitInfoT, StateT> writer,
            DirtyRecordCollector collector,
            int subtaskIndex,
            CatalogTable table,
            SinkWriter.Context context) {
        if (collector == null || collector instanceof NoOpDirtyRecordCollector) {
            return writer;
        }
        if (writer instanceof MultiTableSinkWriter
                || (context != null
                        && (context.getRowErrorCollector().isPresent()
                                || context.isDeferredTerminalWriteOutcomesEnabled()))) {
            IllegalArgumentException failure =
                    new IllegalArgumentException(
                            "dirty.collector/dirty.validator requires a synchronous single-table writer "
                                    + "without row-error routing or deferred terminal outcomes");
            try {
                writer.close();
            } catch (Exception closeFailure) {
                failure.addSuppressed(closeFailure);
            }
            try {
                collector.close();
            } catch (Exception closeFailure) {
                failure.addSuppressed(closeFailure);
            }
            throw failure;
        }
        if (writer instanceof SupportSchemaEvolutionSinkWriter) {
            return new SchemaAwareWriter<>(writer, collector, subtaskIndex, table);
        }
        return new DirtyDataAwareSinkWriter<>(writer, collector, subtaskIndex, table);
    }

    @Override
    public void write(T element) throws IOException {
        writeAndReport(element);
    }

    /**
     * Writes one record and returns whether it reached the delegate successfully.
     *
     * <p>A false result means explicit validation or row-error collection rejected this record;
     * engines must count that terminal outcome as dropped rather than successfully written.
     */
    public boolean writeAndReport(T element) throws IOException {
        if (element instanceof SeaTunnelRow
                && collector.validateAndCollectIfDirty(
                        subtaskIndex, (SeaTunnelRow) element, catalogTable)) {
            return false;
        }
        try {
            delegate.write(element);
            return true;
        } catch (Exception e) {
            if (!tryCollect(element, e)) {
                if (e instanceof IOException) {
                    throw (IOException) e;
                }
                throw new IOException(e);
            }
            return false;
        }
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
        Throwable closeException = null;
        try {
            delegate.close();
        } catch (IOException | RuntimeException e) {
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

    private boolean tryCollect(T element, Exception e) {
        if (collector == null || collector instanceof NoOpDirtyRecordCollector) {
            return false;
        }
        // Unclassified transport, authentication and transaction failures must still fail
        // the job. A successful collector call cannot make those records durably written.
        if (!(delegate instanceof SupportRowLevelErrorClassifier)) {
            return false;
        }
        @SuppressWarnings("unchecked")
        SupportRowLevelErrorClassifier<T> classifier = (SupportRowLevelErrorClassifier<T>) delegate;
        if (classifier.classifyRowError(e, element) != RowErrorClassification.ROW_ERROR) {
            return false;
        }
        collector.collect(
                subtaskIndex, element, e, "Write failed: " + e.getMessage(), catalogTable);
        return true;
    }

    /**
     * Preserves only a schema capability the underlying writer actually implements.
     *
     * <p>The physical table identity is forwarded with schema changes so shared-destination
     * coordination cannot lose the delegate's routing identity.
     */
    private static final class SchemaAwareWriter<T, CommitInfoT, StateT>
            extends DirtyDataAwareSinkWriter<T, CommitInfoT, StateT>
            implements SupportSchemaEvolutionSinkWriter {
        private final SupportSchemaEvolutionSinkWriter schemaWriter;

        private SchemaAwareWriter(
                SinkWriter<T, CommitInfoT, StateT> writer,
                DirtyRecordCollector collector,
                int subtaskIndex,
                CatalogTable table) {
            super(writer, collector, subtaskIndex, table);
            this.schemaWriter = (SupportSchemaEvolutionSinkWriter) writer;
        }

        @Override
        public void applySchemaChange(SchemaChangeEvent event) throws IOException {
            schemaWriter.applySchemaChange(event);
        }

        @Override
        public Optional<String> getPhysicalSinkTableIdentifier() {
            return schemaWriter.getPhysicalSinkTableIdentifier();
        }
    }
}
