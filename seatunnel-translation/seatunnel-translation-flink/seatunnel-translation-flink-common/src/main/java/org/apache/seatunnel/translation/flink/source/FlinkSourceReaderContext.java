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

import org.apache.seatunnel.api.common.metrics.MetricsContext;
import org.apache.seatunnel.api.event.DefaultEventProcessor;
import org.apache.seatunnel.api.event.EventListener;
import org.apache.seatunnel.api.source.SeaTunnelSource;
import org.apache.seatunnel.api.source.SourceEvent;
import org.apache.seatunnel.api.source.SourceReader;
import org.apache.seatunnel.translation.flink.metric.FlinkMetricContext;

import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.StreamingRuntimeContext;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import lombok.extern.slf4j.Slf4j;

import java.lang.reflect.Field;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * The implementation of {@link org.apache.seatunnel.api.source.SourceReader.Context} for flink
 * engine.
 */
@Slf4j
public class FlinkSourceReaderContext implements SourceReader.Context {

    private static final Logger LOGGER = LoggerFactory.getLogger(FlinkSourceReaderContext.class);

    private final AtomicBoolean isSendNoMoreElementEvent = new AtomicBoolean(false);

    private final SourceReaderContext readerContext;

    private final SeaTunnelSource source;
    protected final EventListener eventListener;

    public FlinkSourceReaderContext(SourceReaderContext readerContext, SeaTunnelSource source) {
        this.readerContext = readerContext;
        this.source = source;
        this.eventListener = new DefaultEventProcessor(getFlinkJobId(readerContext));
    }

    @Override
    public int getIndexOfSubtask() {
        return readerContext.getIndexOfSubtask();
    }

    @Override
    public org.apache.seatunnel.api.source.Boundedness getBoundedness() {
        return source.getBoundedness();
    }

    @Override
    public void signalNoMoreElement() {
        // only send once
        if (!isSendNoMoreElementEvent.get()) {
            LOGGER.info(
                    "Reader [{}] send no more element event to enumerator",
                    readerContext.getIndexOfSubtask());
            isSendNoMoreElementEvent.compareAndSet(false, true);
            readerContext.sendSourceEventToCoordinator(
                    new NoMoreElementEvent(readerContext.getIndexOfSubtask()));
        }
    }

    @Override
    public void sendSplitRequest() {
        readerContext.sendSplitRequest();
    }

    @Override
    public void sendSourceEventToEnumerator(SourceEvent sourceEvent) {
        readerContext.sendSourceEventToCoordinator(new SourceEventWrapper(sourceEvent));
    }

    @Override
    public MetricsContext getMetricsContext() {
        return new FlinkMetricContext(getStreamingRuntimeContext(readerContext));
    }

    public boolean isSendNoMoreElementEvent() {
        return isSendNoMoreElementEvent.get();
    }

    @Override
    public EventListener getEventListener() {
        return eventListener;
    }

    private static String getFlinkJobId(SourceReaderContext readerContext) {
        try {
            return getStreamingRuntimeContext(readerContext).getJobId().toString();
        } catch (Exception e) {
            // ignore
            log.warn("Get flink job id failed", e);
            return null;
        }
    }

    private static StreamingRuntimeContext getStreamingRuntimeContext(
            SourceReaderContext readerContext) {
        try {
            Field field = readerContext.getClass().getDeclaredField("this$0");
            field.setAccessible(true);
            AbstractStreamOperator<?> operator =
                    (AbstractStreamOperator<?>) field.get(readerContext);
            return operator.getRuntimeContext();
        } catch (Exception e) {
            throw new IllegalStateException("Initialize flink context failed", e);
        }
    }
}
