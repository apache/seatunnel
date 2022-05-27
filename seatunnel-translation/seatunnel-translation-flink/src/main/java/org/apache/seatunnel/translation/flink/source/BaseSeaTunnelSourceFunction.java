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

import org.apache.seatunnel.api.source.SeaTunnelSource;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowTypeInfo;
import org.apache.seatunnel.translation.flink.utils.TypeConverterUtils;
import org.apache.seatunnel.translation.source.BaseSourceFunction;

import org.apache.flink.api.common.state.CheckpointListener;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.PrimitiveArrayTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.types.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public abstract class BaseSeaTunnelSourceFunction extends RichSourceFunction<Row>
    implements CheckpointListener, ResultTypeQueryable<Row>, CheckpointedFunction {
    private static final Logger LOG = LoggerFactory.getLogger(BaseSeaTunnelSourceFunction.class);

    protected final SeaTunnelSource<SeaTunnelRow, ?, ?> source;
    protected transient volatile BaseSourceFunction<SeaTunnelRow> internalSource;

    protected transient MapState<Integer, List<byte[]>> sourceState;
    protected transient volatile Map<Integer, List<byte[]>> restoredState = new HashMap<>();

    /**
     * Flag indicating whether the consumer is still running.
     */
    private volatile boolean running = true;

    public BaseSeaTunnelSourceFunction(SeaTunnelSource<SeaTunnelRow, ?, ?> source) {
        this.source = source;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        this.internalSource = createInternalSource();
        this.internalSource.open();
    }

    protected abstract BaseSourceFunction<SeaTunnelRow> createInternalSource();

    @Override
    public void run(SourceFunction.SourceContext<Row> sourceContext) throws Exception {
        internalSource.run(new RowCollector(sourceContext, sourceContext.getCheckpointLock()));
    }

    @Override
    public void cancel() {
        running = false;
        try {
            internalSource.close();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void notifyCheckpointComplete(long checkpointId) throws Exception {
        internalSource.notifyCheckpointComplete(checkpointId);
    }

    @Override
    public void notifyCheckpointAborted(long checkpointId) throws Exception {
        internalSource.notifyCheckpointAborted(checkpointId);
    }

    @Override
    public TypeInformation<Row> getProducedType() {
        SeaTunnelRowTypeInfo rowTypeInfo = source.getRowTypeInfo();
        TypeInformation<?>[] typeInformation = Arrays.stream(rowTypeInfo.getSeaTunnelDataTypes())
            .map(TypeConverterUtils::convertType).toArray(TypeInformation[]::new);
        return new RowTypeInfo(typeInformation, rowTypeInfo.getFieldNames());
    }

    @Override
    public void snapshotState(FunctionSnapshotContext snapshotContext) throws Exception {
        if (!running) {
            LOG.debug("snapshotState() called on closed source");
        } else {
            sourceState.clear();
            sourceState.putAll(internalSource.snapshotState(snapshotContext.getCheckpointId()));
        }
    }

    @Override
    public void initializeState(FunctionInitializationContext initializeContext) throws Exception {
        this.sourceState = initializeContext.getKeyedStateStore()
            .getMapState(new MapStateDescriptor<>(
                getStateName(),
                BasicTypeInfo.INT_TYPE_INFO,
                Types.LIST(PrimitiveArrayTypeInfo.BYTE_PRIMITIVE_ARRAY_TYPE_INFO)));
        if (initializeContext.isRestored()) {
            // populate actual holder for restored state
            sourceState.entries().forEach(entry -> restoredState.put(entry.getKey(), entry.getValue()));
            LOG.info("Consumer subtask {} restored state", getRuntimeContext().getIndexOfThisSubtask());
        } else {
            LOG.info("Consumer subtask {} has no restore state.", getRuntimeContext().getIndexOfThisSubtask());
        }
    }

    protected abstract String getStateName();
}
