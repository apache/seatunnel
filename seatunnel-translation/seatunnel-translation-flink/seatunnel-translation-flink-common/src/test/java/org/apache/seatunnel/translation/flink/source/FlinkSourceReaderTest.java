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

import org.apache.seatunnel.api.common.metrics.Counter;
import org.apache.seatunnel.api.common.metrics.Meter;
import org.apache.seatunnel.api.common.metrics.MetricsContext;
import org.apache.seatunnel.api.event.EventListener;
import org.apache.seatunnel.api.source.SourceSplit;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;

import org.apache.flink.api.connector.source.ReaderOutput;
import org.apache.flink.core.io.InputStatus;

import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.lang.reflect.Proxy;
import java.util.Collections;
import java.util.concurrent.CompletableFuture;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class FlinkSourceReaderTest {

    @Test
    void testKeepAliveLogicallyClosesReaderAndReactivatesItForNewSplit() throws Exception {
        ReaderTestContext context = createReader(true);
        try {
            Mockito.when(context.flinkContext.isSendNoMoreElementEvent()).thenReturn(true);

            context.reader.handleSourceEvents(new NoMoreElementEvent(0));

            assertEquals(
                    InputStatus.NOTHING_AVAILABLE,
                    context.reader.pollNext(Mockito.mock(ReaderOutput.class)));
            Mockito.verify(context.sourceReader, Mockito.never()).pollNext(Mockito.any());
            CompletableFuture<Void> availability = context.reader.isAvailable();
            assertFalse(availability.isDone());

            context.reader.snapshotState(1L);
            context.reader.notifyCheckpointComplete(1L);
            context.reader.notifyCheckpointAborted(2L);
            Mockito.verify(context.sourceReader).snapshotState(1L);
            Mockito.verify(context.sourceReader).notifyCheckpointComplete(1L);
            Mockito.verify(context.sourceReader).notifyCheckpointAborted(2L);
            assertFalse(availability.isDone());

            DummySplit split = new DummySplit();
            context.reader.addSplits(
                    Collections.singletonList(new SplitWrapper<DummySplit>(split)));

            Mockito.verify(context.flinkContext).resetNoMoreElementEvent();
            Mockito.verify(context.sourceReader).addSplits(Collections.singletonList(split));
            assertTrue(availability.isDone());
        } finally {
            context.reader.close();
        }
    }

    @Test
    void testFinishedReaderEndsWhenKeepAliveIsDisabled() throws Exception {
        ReaderTestContext context = createReader(false);
        try {
            Mockito.when(context.flinkContext.isSendNoMoreElementEvent()).thenReturn(true);
            context.reader.handleSourceEvents(new NoMoreElementEvent(0));

            assertEquals(
                    InputStatus.END_OF_INPUT,
                    context.reader.pollNext(Mockito.mock(ReaderOutput.class)));
            Mockito.verify(context.sourceReader, Mockito.never()).pollNext(Mockito.any());
        } finally {
            context.reader.close();
        }
    }

    @Test
    void testStaleNoMoreElementAcknowledgementDoesNotCloseReactivatedReader() throws Exception {
        ReaderTestContext context = createReader(true);
        try {
            Mockito.when(context.flinkContext.isSendNoMoreElementEvent())
                    .thenReturn(true)
                    .thenReturn(false);

            DummySplit split = new DummySplit();
            context.reader.addSplits(
                    Collections.singletonList(new SplitWrapper<DummySplit>(split)));
            context.reader.handleSourceEvents(new NoMoreElementEvent(0));
            context.reader.pollNext(Mockito.mock(ReaderOutput.class));

            Mockito.verify(context.flinkContext).resetNoMoreElementEvent();
            Mockito.verify(context.sourceReader).pollNext(Mockito.any());
        } finally {
            context.reader.close();
        }
    }

    @SuppressWarnings("unchecked")
    private static ReaderTestContext createReader(boolean keepAliveEnabled) throws Exception {
        org.apache.seatunnel.api.source.SourceReader<SeaTunnelRow, DummySplit> sourceReader =
                Mockito.mock(org.apache.seatunnel.api.source.SourceReader.class);
        FlinkSourceReaderContext flinkContext = Mockito.mock(FlinkSourceReaderContext.class);
        MetricsContext metricsContext = Mockito.mock(MetricsContext.class);
        Mockito.when(flinkContext.getIndexOfSubtask()).thenReturn(0);
        Mockito.when(flinkContext.getMetricsContext()).thenReturn(metricsContext);
        Mockito.when(flinkContext.getEventListener()).thenReturn(Mockito.mock(EventListener.class));
        Mockito.when(sourceReader.snapshotState(Mockito.anyLong()))
                .thenReturn(Collections.emptyList());
        Mockito.when(metricsContext.counter(Mockito.anyString()))
                .thenReturn(Mockito.mock(Counter.class));
        Mockito.when(metricsContext.meter(Mockito.anyString()))
                .thenReturn(Mockito.mock(Meter.class));
        Class<?> configClass =
                Class.forName("org.apache.seatunnel.shade.com.typesafe.config.Config");
        Object config =
                Proxy.newProxyInstance(
                        configClass.getClassLoader(),
                        new Class<?>[] {configClass},
                        (proxy, method, args) -> {
                            if ("hasPath".equals(method.getName())) {
                                return "schema-changes.source-keep-alive".equals(args[0]);
                            }
                            if ("getBoolean".equals(method.getName())) {
                                return keepAliveEnabled;
                            }
                            return null;
                        });
        @SuppressWarnings("unchecked")
        FlinkSourceReader<DummySplit> reader =
                (FlinkSourceReader<DummySplit>)
                        FlinkSourceReader.class
                                .getConstructor(
                                        org.apache.seatunnel.api.source.SourceReader.class,
                                        org.apache.seatunnel.api.source.SourceReader.Context.class,
                                        configClass)
                                .newInstance(sourceReader, flinkContext, config);
        return new ReaderTestContext(reader, sourceReader, flinkContext);
    }

    private static final class DummySplit implements SourceSplit {
        private static final long serialVersionUID = 1L;

        @Override
        public String splitId() {
            return "dummy";
        }
    }

    private static final class ReaderTestContext {
        private final FlinkSourceReader<DummySplit> reader;
        private final org.apache.seatunnel.api.source.SourceReader<SeaTunnelRow, DummySplit>
                sourceReader;
        private final FlinkSourceReaderContext flinkContext;

        private ReaderTestContext(
                FlinkSourceReader<DummySplit> reader,
                org.apache.seatunnel.api.source.SourceReader<SeaTunnelRow, DummySplit> sourceReader,
                FlinkSourceReaderContext flinkContext) {
            this.reader = reader;
            this.sourceReader = sourceReader;
            this.flinkContext = flinkContext;
        }
    }
}
