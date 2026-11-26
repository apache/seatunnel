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

package org.apache.seatunnel.translation.flink.sink;

import org.apache.flink.metrics.groups.OperatorMetricGroup;
import org.apache.flink.runtime.checkpoint.CheckpointOptions;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.state.CheckpointStreamFactory;
import org.apache.flink.streaming.api.connector.sink2.CommittableMessage;
import org.apache.flink.streaming.api.connector.sink2.CommittableWithLineage;
import org.apache.flink.streaming.api.operators.BoundedOneInput;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.operators.OperatorSnapshotFutures;
import org.apache.flink.streaming.api.operators.StreamOperatorStateContext;
import org.apache.flink.streaming.api.operators.StreamTaskStateInitializer;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.LatencyMarker;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.watermarkstatus.WatermarkStatus;

/** Decorates the existing global operator, preserving its graph, state and operator identity. */
final class RestoredCommittableOperator<T, R>
        implements OneInputStreamOperator<CommittableMessage<CommitWrapper<T>>, R>,
                BoundedOneInput {
    private final OneInputStreamOperator<CommittableMessage<CommitWrapper<T>>, R> delegate;
    private long restoredCheckpoint = -1;

    RestoredCommittableOperator(
            OneInputStreamOperator<CommittableMessage<CommitWrapper<T>>, R> delegate) {
        this.delegate = delegate;
    }

    public void initializeState(StreamTaskStateInitializer initializer) throws Exception {
        delegate.initializeState(
                (id, name, time, key, serializer, registry, metrics, fraction, managed) -> {
                    StreamOperatorStateContext context =
                            initializer.streamOperatorStateContext(
                                    id,
                                    name,
                                    time,
                                    key,
                                    serializer,
                                    registry,
                                    metrics,
                                    fraction,
                                    managed);
                    restoredCheckpoint = context.getRestoredCheckpointId().orElse(-1);
                    return context;
                });
    }

    public void processElement(StreamRecord<CommittableMessage<CommitWrapper<T>>> element)
            throws Exception {
        markRecovered(element.getValue(), restoredCheckpoint);
        delegate.processElement(element);
    }

    static <T> void markRecovered(
            CommittableMessage<CommitWrapper<T>> message, long restoredCheckpoint) {
        if (restoredCheckpoint >= 0
                && message instanceof CommittableWithLineage
                && message.getCheckpointId().isPresent()
                && message.getCheckpointId().getAsLong() <= restoredCheckpoint) {
            ((CommittableWithLineage<CommitWrapper<T>>) message).getCommittable().markRecovered();
        }
    }

    public void open() throws Exception {
        delegate.open();
    }

    public void finish() throws Exception {
        delegate.finish();
    }

    public void close() throws Exception {
        delegate.close();
    }

    public void endInput() throws Exception {
        ((BoundedOneInput) delegate).endInput();
    }

    public void prepareSnapshotPreBarrier(long checkpoint) throws Exception {
        delegate.prepareSnapshotPreBarrier(checkpoint);
    }

    public OperatorSnapshotFutures snapshotState(
            long checkpoint,
            long timestamp,
            CheckpointOptions options,
            CheckpointStreamFactory factory)
            throws Exception {
        return delegate.snapshotState(checkpoint, timestamp, options, factory);
    }

    public void notifyCheckpointComplete(long checkpoint) throws Exception {
        delegate.notifyCheckpointComplete(checkpoint);
    }

    public void notifyCheckpointAborted(long checkpoint) throws Exception {
        delegate.notifyCheckpointAborted(checkpoint);
    }

    public void setKeyContextElement1(StreamRecord<?> record) throws Exception {
        delegate.setKeyContextElement1(record);
    }

    public void setKeyContextElement2(StreamRecord<?> record) throws Exception {
        delegate.setKeyContextElement2(record);
    }

    public void setCurrentKey(Object key) {
        delegate.setCurrentKey(key);
    }

    public Object getCurrentKey() {
        return delegate.getCurrentKey();
    }

    public OperatorMetricGroup getMetricGroup() {
        return delegate.getMetricGroup();
    }

    public OperatorID getOperatorID() {
        return delegate.getOperatorID();
    }

    public void processWatermark(Watermark watermark) throws Exception {
        delegate.processWatermark(watermark);
    }

    public void processWatermarkStatus(WatermarkStatus status) throws Exception {
        delegate.processWatermarkStatus(status);
    }

    public void processLatencyMarker(LatencyMarker marker) throws Exception {
        delegate.processLatencyMarker(marker);
    }
}
