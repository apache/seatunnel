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
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.streaming.api.operators.StreamingRuntimeContext;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Field;

public class FlinkSinkWriterContext implements SinkWriter.Context {

    private static final Logger LOGGER = LoggerFactory.getLogger(FlinkMetricContext.class);

    private final InitContext writerContext;
    private final EventListener eventListener;
    private final int parallelism;

    public FlinkSinkWriterContext(InitContext writerContext, int parallelism) {
        this.writerContext = writerContext;
        this.eventListener = new DefaultEventProcessor(getJobIdForV14(writerContext));
        this.parallelism = parallelism;
    }

    @Override
    public int getIndexOfSubtask() {
        return writerContext.getSubtaskId();
    }

    @Override
    public int getNumberOfParallelSubtasks() {
        return parallelism;
    }

    @Override
    public MetricsContext getMetricsContext() {
        try {
            StreamingRuntimeContext runtimeContext =
                    getStreamingRuntimeContextForV14(writerContext);
            return new FlinkMetricContext(runtimeContext);
        } catch (Exception e) {
            LOGGER.info(
                    "Flink version is not 1.14.x, will initial MetricsContext using metricGroup");
        }
        // Why use reflection to obtain metrics group?
        // Because the value types returned by flink 1.13 and 1.14 InitContext.getMetricGroup()
        // are inconsistent
        try {
            Field field = writerContext.getClass().getDeclaredField("metricGroup");
            field.setAccessible(true);
            MetricGroup metricGroup = (MetricGroup) field.get(writerContext);
            return new FlinkMetricContext(metricGroup);
        } catch (Exception e) {
            throw new IllegalStateException("Initial sink metrics failed", e);
        }
    }

    @Override
    public EventListener getEventListener() {
        return eventListener;
    }

    private static StreamingRuntimeContext getStreamingRuntimeContextForV14(
            Sink.InitContext writerContext) throws NoSuchFieldException, IllegalAccessException {
        // In flink 1.14, it has contained runtimeContext in InitContext, so first step to
        // detect if
        // it is existed
        Field field = writerContext.getClass().getDeclaredField("runtimeContext");
        field.setAccessible(true);
        return (StreamingRuntimeContext) field.get(writerContext);
    }

    private static String getJobIdForV14(Sink.InitContext writerContext) {
        try {
            StreamingRuntimeContext runtimeContext =
                    getStreamingRuntimeContextForV14(writerContext);
            return runtimeContext != null ? runtimeContext.getJobId().toString() : null;
        } catch (Exception e) {
            LOGGER.info("Flink version is not 1.14.x, will not initial job id");
            return null;
        }
    }
}
