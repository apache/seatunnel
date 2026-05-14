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

import org.apache.seatunnel.api.common.metrics.Counter;
import org.apache.seatunnel.api.common.metrics.MetricsContext;
import org.apache.seatunnel.api.event.DefaultEventProcessor;
import org.apache.seatunnel.api.event.EventListener;
import org.apache.seatunnel.api.sink.DirtyRecordCollector;
import org.apache.seatunnel.api.sink.DistributedCounter;
import org.apache.seatunnel.api.sink.NoOpDirtyRecordCollector;
import org.apache.seatunnel.api.sink.SinkWriter;
import org.apache.seatunnel.translation.flink.metric.FlinkMetricContext;

import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.connector.sink2.WriterInitContext;

import lombok.extern.slf4j.Slf4j;

import java.lang.reflect.Method;

@Slf4j
public class FlinkSinkWriterContext implements SinkWriter.Context {

    private static final String DIRTY_RECORD_COUNT_METRIC = "dirtyRecordCount";

    private final WriterInitContext initContext;
    private final int parallelism;
    private final EventListener eventListener;
    private final DirtyRecordCollector dirtyRecordCollector;
    private MetricsContext metricsContext;

    public FlinkSinkWriterContext(WriterInitContext initContext, int parallelism) {
        this(initContext, parallelism, NoOpDirtyRecordCollector.INSTANCE);
    }

    public FlinkSinkWriterContext(
            WriterInitContext initContext,
            int parallelism,
            DirtyRecordCollector dirtyRecordCollector) {
        this.initContext = initContext;
        this.parallelism = parallelism;
        this.eventListener = new DefaultEventProcessor(getFlinkJobId(initContext));
        this.dirtyRecordCollector = dirtyRecordCollector;

        // initialize metrics context and set up distributed dirty record counter
        initMetricsContext();
        setupDistributedDirtyRecordCounter();
    }

    private void initMetricsContext() {
        RuntimeContext runtimeContext = getRuntimeContext();
        if (runtimeContext != null) {
            this.metricsContext = new FlinkMetricContext(runtimeContext);
        }
    }

    private void setupDistributedDirtyRecordCounter() {
        if (dirtyRecordCollector == null
                || dirtyRecordCollector instanceof NoOpDirtyRecordCollector
                || metricsContext == null) {
            return;
        }
        try {
            Counter dirtyCounter = metricsContext.counter(DIRTY_RECORD_COUNT_METRIC);
            dirtyRecordCollector.setDistributedCounter(
                    new DistributedCounter() {
                        private static final long serialVersionUID = 1L;

                        @Override
                        public void add(long delta) {
                            dirtyCounter.inc(delta);
                        }

                        @Override
                        public long value() {
                            return dirtyCounter.getCount();
                        }
                    });
            log.info(
                    "Set up Flink distributed counter for dirty record counting (subtask {})",
                    initContext.getTaskInfo().getIndexOfThisSubtask());
        } catch (Exception e) {
            log.warn("Failed to set up Flink distributed counter for dirty record counting", e);
        }
    }

    @Override
    public int getIndexOfSubtask() {
        return initContext.getTaskInfo().getIndexOfThisSubtask();
    }

    @Override
    public int getNumberOfParallelSubtasks() {
        return parallelism;
    }

    @Override
    public MetricsContext getMetricsContext() {
        if (metricsContext == null) {
            initMetricsContext();
        }
        return metricsContext;
    }

    @Override
    public EventListener getEventListener() {
        return eventListener;
    }

    @Override
    public DirtyRecordCollector getDirtyRecordCollector() {
        return dirtyRecordCollector;
    }

    public RuntimeContext getRuntimeContext() {
        try {
            return tryGetFromInitContextBase(initContext);
        } catch (Exception e) {
            return null;
        }
    }

    private RuntimeContext tryGetFromInitContextBase(Object context) {
        try {
            Class<?> initContextBaseClass =
                    Class.forName(
                            "org.apache.flink.streaming.runtime.operators.sink.InitContextBase");
            if (initContextBaseClass.isInstance(context)) {
                Method getRuntimeContextMethod =
                        initContextBaseClass.getDeclaredMethod("getRuntimeContext");
                getRuntimeContextMethod.setAccessible(true);
                RuntimeContext runtimeContext =
                        (RuntimeContext) getRuntimeContextMethod.invoke(context);
                log.info(
                        "Successfully obtained RuntimeContext from InitContextBase: {}",
                        runtimeContext.getClass().getName());
                return runtimeContext;
            }
        } catch (Exception e) {
            log.debug("Failed to get RuntimeContext from InitContextBase", e);
        }
        return null;
    }

    private static String getFlinkJobId(WriterInitContext context) {
        try {
            return context.getJobInfo().getJobId().toString();
        } catch (Exception e) {
            log.warn("Get flink job id failed", e);
            return null;
        }
    }
}
