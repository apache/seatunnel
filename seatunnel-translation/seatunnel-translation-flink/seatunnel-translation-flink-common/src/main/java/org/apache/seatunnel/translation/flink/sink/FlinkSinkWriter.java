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

import org.apache.seatunnel.api.common.metrics.Counter;
import org.apache.seatunnel.api.common.metrics.Meter;
import org.apache.seatunnel.api.common.metrics.MetricNames;
import org.apache.seatunnel.api.common.metrics.MetricsContext;
import org.apache.seatunnel.api.sink.MultiTableResourceManager;
import org.apache.seatunnel.api.sink.SupportCheckpointIdDownStream;
import org.apache.seatunnel.api.sink.SupportResourceShare;
import org.apache.seatunnel.api.sink.event.WriterCloseEvent;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;

import org.apache.flink.api.connector.sink.Sink;
import org.apache.flink.api.connector.sink.SinkWriter;

import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.io.InvalidClassException;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * The sink writer implementation of {@link SinkWriter}, which is created by {@link
 * Sink#createWriter}
 *
 * @param <InputT> The generic type of input data
 * @param <CommT> The generic type of commit message
 * @param <WriterStateT> The generic type of writer state
 */
@Slf4j
public class FlinkSinkWriter<InputT, CommT, WriterStateT>
        implements SinkWriter<InputT, CommitWrapper<CommT>, FlinkWriterState<WriterStateT>> {

    private final org.apache.seatunnel.api.sink.SinkWriter<SeaTunnelRow, CommT, WriterStateT>
            sinkWriter;

    private final org.apache.seatunnel.api.sink.SinkWriter.Context context;

    private final Counter sinkWriteCount;

    private final Counter sinkWriteBytes;

    private final Meter sinkWriterQPS;

    private long checkpointId;

    private MultiTableResourceManager resourceManager;

    FlinkSinkWriter(
            org.apache.seatunnel.api.sink.SinkWriter<SeaTunnelRow, CommT, WriterStateT> sinkWriter,
            long checkpointId,
            org.apache.seatunnel.api.sink.SinkWriter.Context context) {
        this.context = context;
        this.sinkWriter = sinkWriter;
        this.checkpointId = checkpointId;
        MetricsContext metricsContext = context.getMetricsContext();
        this.sinkWriteCount = metricsContext.counter(MetricNames.SINK_WRITE_COUNT);
        this.sinkWriteBytes = metricsContext.counter(MetricNames.SINK_WRITE_BYTES);
        this.sinkWriterQPS = metricsContext.meter(MetricNames.SINK_WRITE_QPS);
        if (sinkWriter instanceof SupportResourceShare) {
            resourceManager =
                    ((SupportResourceShare) sinkWriter).initMultiTableResourceManager(1, 1);
            ((SupportResourceShare) sinkWriter).setMultiTableResourceManager(resourceManager, 0);
        }
    }

    @Override
    public void write(InputT element, SinkWriter.Context context) throws IOException {
        if (element == null) {
            return;
        }
        if (element instanceof SeaTunnelRow) {
            sinkWriter.write((SeaTunnelRow) element);
            sinkWriteCount.inc();
            sinkWriteBytes.inc(((SeaTunnelRow) element).getBytesSize());
            sinkWriterQPS.markEvent();
        } else {
            throw new InvalidClassException(
                    "only support SeaTunnelRow at now, the element Class is " + element.getClass());
        }
    }

    @Override
    public List<CommitWrapper<CommT>> prepareCommit(boolean flush) throws IOException {
        Optional<CommT> commTOptional =
                (sinkWriter instanceof SupportCheckpointIdDownStream)
                        ? sinkWriter.prepareCommit(checkpointId)
                        : sinkWriter.prepareCommit();
        return commTOptional
                .map(CommitWrapper::new)
                .map(Collections::singletonList)
                .orElse(Collections.emptyList());
    }

    @Override
    public List<FlinkWriterState<WriterStateT>> snapshotState() throws IOException {
        List<FlinkWriterState<WriterStateT>> states =
                sinkWriter.snapshotState(this.checkpointId).stream()
                        .map(state -> new FlinkWriterState<>(this.checkpointId, state))
                        .collect(Collectors.toList());
        this.checkpointId++;
        return states;
    }

    @Override
    public void close() throws Exception {
        sinkWriter.close();
        context.getEventListener().onEvent(new WriterCloseEvent());
        try {
            if (resourceManager != null) {
                resourceManager.close();
            }
        } catch (Throwable e) {
            log.error("close resourceManager error", e);
        }
    }
}
