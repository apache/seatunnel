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

import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.connector.sink2.WriterInitContext;

import lombok.extern.slf4j.Slf4j;

import java.lang.reflect.Method;

@Slf4j
public class FlinkSinkWriterContext implements SinkWriter.Context {

    private final WriterInitContext initContext;
    private final int parallelism;
    private final EventListener eventListener;

    public FlinkSinkWriterContext(WriterInitContext initContext, int parallelism) {
        this.initContext = initContext;
        this.parallelism = parallelism;
        this.eventListener = new DefaultEventProcessor(getFlinkJobId(initContext));
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
        return new FlinkMetricContext(getRuntimeContext());
    }

    @Override
    public EventListener getEventListener() {
        return eventListener;
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
