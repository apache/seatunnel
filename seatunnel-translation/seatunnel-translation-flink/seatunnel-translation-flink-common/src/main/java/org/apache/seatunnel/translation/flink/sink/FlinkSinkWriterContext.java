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
import org.apache.seatunnel.api.sink.SinkWriter;
import org.apache.seatunnel.translation.flink.metric.FlinkMetricContext;

import org.apache.flink.api.connector.sink.Sink;
import org.apache.flink.api.connector.sink.Sink.InitContext;
import org.apache.flink.streaming.api.operators.StreamingRuntimeContext;

import java.lang.reflect.Field;

public class FlinkSinkWriterContext implements SinkWriter.Context {

    private final Sink.InitContext writerContext;

    public FlinkSinkWriterContext(InitContext writerContext) {
        this.writerContext = writerContext;
    }

    @Override
    public int getIndexOfSubtask() {
        return writerContext.getSubtaskId();
    }

    @Override
    public MetricsContext getMetricsContext() {
        try {
            Field contextImplField = writerContext.getClass().getDeclaredField("context");
            contextImplField.setAccessible(true);
            Object contextImpl = contextImplField.get(writerContext);
            Field runtimeContextField = contextImpl.getClass().getDeclaredField("runtimeContext");
            runtimeContextField.setAccessible(true);
            StreamingRuntimeContext runtimeContext =
                    (StreamingRuntimeContext) runtimeContextField.get(contextImpl);
            return new FlinkMetricContext(runtimeContext);
        } catch (Exception e) {
            throw new IllegalStateException("Initialize sink metrics failed", e);
        }
    }
}
