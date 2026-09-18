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

import org.apache.seatunnel.api.common.error.RowErrorCollector;
import org.apache.seatunnel.api.common.metrics.MetricsContext;
import org.apache.seatunnel.api.event.EventListener;
import org.apache.seatunnel.api.table.schema.event.SchemaChangeEvent;
import org.apache.seatunnel.common.utils.function.RunnableWithException;

import java.io.IOException;
import java.io.Serializable;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

/**
 * The sink writer use to write data to third party data receiver. This class will run on
 * taskManger/Worker.
 *
 * @param <T> The data class by sink accept. Only support {@link
 *     org.apache.seatunnel.api.table.type.SeaTunnelRow} at now.
 * @param <CommitInfoT> The type of commit message.
 * @param <StateT> The type of state.
 */
public interface SinkWriter<T, CommitInfoT, StateT> {

    /**
     * write data to third party data receiver.
     *
     * @param element the data need be written.
     * @throws IOException throw IOException when write data failed.
     */
    void write(T element) throws IOException;

    /** @deprecated instead by {@link SupportSchemaEvolutionSinkWriter} TODO: remove this method */
    @Deprecated
    default void applySchemaChange(SchemaChangeEvent event) throws IOException {}

    /**
     * prepare the commit, will be called before {@link #snapshotState(long checkpointId)}. If you
     * need to use 2pc, you can return the commit info in this method, and receive the commit info
     * in {@link SinkCommitter#commit(List)}. If this method failed (by throw exception), **Only**
     * Spark engine will call {@link #abortPrepare()}
     *
     * @return the commit info need to commit
     */
    @Deprecated
    Optional<CommitInfoT> prepareCommit() throws IOException;

    /**
     * prepare the commit, will be called before {@link #snapshotState(long checkpointId)}. If you
     * need to use 2pc, you can return the commit info in this method, and receive the commit info
     * in {@link SinkCommitter#commit(List)}. If this method failed (by throw exception), **Only**
     * Spark engine will call {@link #abortPrepare()}
     *
     * @param checkpointId checkpointId
     * @return the commit info need to commit
     * @throws IOException If fail to prepareCommit
     */
    default Optional<CommitInfoT> prepareCommit(long checkpointId) throws IOException {
        return prepareCommit();
    }

    /**
     * @return The writer's state.
     * @throws IOException if fail to snapshot writer's state.
     */
    default List<StateT> snapshotState(long checkpointId) throws IOException {
        return Collections.emptyList();
    }

    /**
     * Used to abort the {@link #prepareCommit()}, if the prepareCommit failed, there is no
     * CommitInfoT, so the rollback work cannot be done by {@link SinkCommitter}. But we can use
     * this method to rollback side effects of {@link #prepareCommit()}. Only use it in Spark engine
     * at now.
     */
    void abortPrepare();

    /**
     * call it when SinkWriter close
     *
     * @throws IOException if close failed
     */
    void close() throws IOException;

    interface Context extends Serializable {

        /** @return The index of this subtask. */
        int getIndexOfSubtask();

        /** @return parallelism of this writer. */
        default int getNumberOfParallelSubtasks() {
            return 1;
        }

        /** @return metricsContext of this reader. */
        MetricsContext getMetricsContext();

        /**
         * Get the {@link EventListener} of this writer.
         *
         * @return
         */
        EventListener getEventListener();

        /**
         * Row-level error collector provided by the engine for reporting errors outside write().
         */
        default Optional<RowErrorCollector> getRowErrorCollector() {
            return Optional.empty();
        }

        /**
         * Mark that this writer may report row-level errors after {@link SinkWriter#write(Object)}
         * returns, for example during timer flush, prepareCommit, or close.
         *
         * <p>Engines can use this signal to delay terminal success metrics/tracing until buffered
         * rows are flushed and no delayed row errors were reported.
         */
        default void enableDeferredTerminalWriteOutcomes() {}

        /**
         * Returns whether a writer requested deferred terminal success reporting through {@link
         * #enableDeferredTerminalWriteOutcomes()}.
         */
        default boolean isDeferredTerminalWriteOutcomesEnabled() {
            return false;
        }

        /**
         * Register an action to be invoked by the engine when a periodic flush signal arrives.
         *
         * <p>This is the opt-in point for engine-level timer flush. A writer that wants to be
         * flushed on a schedule should call this method during its initialization, typically with a
         * method reference like {@code context.registerFlushAction(this::flush)}.
         *
         * @param action the action to invoke on each flush signal, must not be {@code null}
         */
        default void registerFlushAction(RunnableWithException action) {}

        /**
         * Return the flush action previously registered via {@link
         * #registerFlushAction(RunnableWithException)}, or {@code null} if the writer has not opted
         * in to engine-level timer flush.
         *
         * <p>Callers must null-check the return value; a {@code null} return means the writer will
         * silently ignore flush signals.
         */
        default RunnableWithException getFlushAction() {
            return null;
        }
    }
}
