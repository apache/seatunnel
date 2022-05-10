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

package org.apache.seatunnel.translation.flink.source;

import org.apache.seatunnel.api.source.Source;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.translation.flink.serialization.KryoTypeInfo;
import org.apache.seatunnel.translation.flink.serialization.WrappedRow;
import org.apache.seatunnel.translation.source.ParallelSource;

import org.apache.flink.api.common.state.CheckpointListener;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.OperatorStateStore;
import org.apache.flink.api.common.typeinfo.PrimitiveArrayTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class SeaTunnelParallelSource extends RichParallelSourceFunction<WrappedRow>
        implements CheckpointListener, ResultTypeQueryable<WrappedRow>, CheckpointedFunction {
    private static final Logger LOG = LoggerFactory.getLogger(SeaTunnelParallelSource.class);
    protected static final String PARALLEL_SOURCE_STATE_NAME = "parallel-source-states";

    protected final Source<SeaTunnelRow, ?, ?> source;
    protected volatile ParallelSource<SeaTunnelRow, ?, ?> parallelSource;

    protected transient ListState<byte[]> sourceState;
    private transient volatile List<byte[]> restoredState;

    /**
     * Flag indicating whether the consumer is still running.
     */
    private volatile boolean running = true;

    public SeaTunnelParallelSource(Source<SeaTunnelRow, ?, ?> source) {
        // TODO: Make sure the source is uncoordinated.
        this.source = source;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        this.parallelSource = new ParallelSource<>(source,
                restoredState,
                getRuntimeContext().getNumberOfParallelSubtasks(),
                getRuntimeContext().getIndexOfThisSubtask());
        this.parallelSource.open();
    }

    @Override
    public void run(SourceFunction.SourceContext<WrappedRow> sourceContext) throws Exception {
        parallelSource.run(new WrappedRowCollector(sourceContext));
    }

    @Override
    public void cancel() {
        running = false;
        try {
            parallelSource.close();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void notifyCheckpointComplete(long checkpointId) throws Exception {
        parallelSource.notifyCheckpointComplete(checkpointId);
    }

    @Override
    public void notifyCheckpointAborted(long checkpointId) throws Exception {
        parallelSource.notifyCheckpointAborted(checkpointId);
    }

    @Override
    public TypeInformation<WrappedRow> getProducedType() {
        return new KryoTypeInfo<>(WrappedRow.class);
    }

    @Override
    public void snapshotState(FunctionSnapshotContext snapshotContext) throws Exception {
        if (!running) {
            LOG.debug("snapshotState() called on closed source");
        } else {
            sourceState.update(parallelSource.snapshotState(snapshotContext.getCheckpointId()));
        }
    }

    @Override
    public void initializeState(FunctionInitializationContext initializeContext) throws Exception {
        OperatorStateStore stateStore = initializeContext.getOperatorStateStore();
        this.sourceState = stateStore.getListState(new ListStateDescriptor<>(PARALLEL_SOURCE_STATE_NAME, PrimitiveArrayTypeInfo.BYTE_PRIMITIVE_ARRAY_TYPE_INFO));
        if (initializeContext.isRestored()) {
            restoredState = new ArrayList<>();
            // populate actual holder for restored state
            sourceState.get().forEach(restoredState::add);

            LOG.info("Consumer subtask {} restored state", getRuntimeContext().getIndexOfThisSubtask());
        } else {
            LOG.info("Consumer subtask {} has no restore state.", getRuntimeContext().getIndexOfThisSubtask());
        }
    }
}
