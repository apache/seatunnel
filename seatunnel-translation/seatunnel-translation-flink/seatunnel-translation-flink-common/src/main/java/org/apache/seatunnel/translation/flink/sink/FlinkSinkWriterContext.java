/*
 *  Licensed to the Apache Software Foundation (ASF) under one or more
 *  contributor license agreements.  See the NOTICE file distributed with
 *  this work for additional information regarding copyright ownership.
 *  The ASF licenses this file to You under the Apache License, Version 2.0
 *  (the "License"); you may not use this file except in compliance with
 *  the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.seatunnel.translation.flink.sink;

import org.apache.seatunnel.api.common.metrics.MetricsContext;
import org.apache.seatunnel.api.event.DefaultEventProcessor;
import org.apache.seatunnel.api.event.EventListener;
import org.apache.seatunnel.api.sink.SinkWriter;
import org.apache.seatunnel.translation.flink.metric.FlinkMetricContext;

import org.apache.flink.api.connector.sink.Sink;
import org.apache.flink.api.connector.sink.Sink.InitContext;
import org.apache.flink.streaming.api.operators.StreamingRuntimeContext;

import lombok.extern.slf4j.Slf4j;

import java.lang.reflect.Field;

@Slf4j
public class FlinkSinkWriterContext implements SinkWriter.Context {

    private final Sink.InitContext writerContext;
    private final EventListener eventListener;
    private final int parallelism;

    public FlinkSinkWriterContext(InitContext writerContext, int parallelism) {
        this.writerContext = writerContext;
        this.eventListener = new DefaultEventProcessor(getFlinkJobId(writerContext));
        this.parallelism = parallelism;
    }

    @Override
    public int getIndexOfSubtask() {
        return writerContext.getSubtaskId();
    }

    @Override
    public int getNumberOfParallelSubtasks() {
        return writerContext.getNumberOfParallelSubtasks();
    }

    @Override
    public MetricsContext getMetricsContext() {
        return new FlinkMetricContext(getStreamingRuntimeContextForV15(writerContext));
    }

    @Override
    public EventListener getEventListener() {
        return eventListener;
    }

    private static String getFlinkJobId(Sink.InitContext writerContext) {
        try {
            return getStreamingRuntimeContextForV15(writerContext).getJobId().toString();
        } catch (Exception e) {
            // ignore
            log.warn("Get flink job id failed", e);
            return null;
        }
    }

    private static StreamingRuntimeContext getStreamingRuntimeContextForV15(
            Sink.InitContext writerContext) {
        try {
            Field contextImplField = writerContext.getClass().getDeclaredField("context");
            contextImplField.setAccessible(true);
            Object contextImpl = contextImplField.get(writerContext);
            Field runtimeContextField = contextImpl.getClass().getDeclaredField("runtimeContext");
            runtimeContextField.setAccessible(true);
            return (StreamingRuntimeContext) runtimeContextField.get(contextImpl);
        } catch (Exception e) {
            throw new IllegalStateException("Initialize flink context failed", e);
        }
    }
}
