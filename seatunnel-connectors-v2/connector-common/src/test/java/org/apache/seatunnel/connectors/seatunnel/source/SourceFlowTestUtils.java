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

package org.apache.seatunnel.connectors.seatunnel.source;

import org.apache.seatunnel.api.common.JobContext;
import org.apache.seatunnel.api.common.metrics.AbstractMetricsContext;
import org.apache.seatunnel.api.common.metrics.MetricsContext;
import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.event.EventListener;
import org.apache.seatunnel.api.source.Boundedness;
import org.apache.seatunnel.api.source.Collector;
import org.apache.seatunnel.api.source.SeaTunnelSource;
import org.apache.seatunnel.api.source.SourceEvent;
import org.apache.seatunnel.api.source.SourceReader;
import org.apache.seatunnel.api.source.SourceSplit;
import org.apache.seatunnel.api.source.SourceSplitEnumerator;
import org.apache.seatunnel.api.table.factory.TableSourceFactory;
import org.apache.seatunnel.api.table.factory.TableSourceFactoryContext;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.common.constants.JobMode;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class SourceFlowTestUtils {

    public static List<SeaTunnelRow> runBatchWithCheckpointDisabled(
            ReadonlyConfig options, TableSourceFactory factory) throws Exception {
        JobContext context = new JobContext(System.currentTimeMillis());
        context.setJobMode(JobMode.BATCH);
        context.setEnableCheckpoint(false);
        return runWithContext(options, factory, context, Boundedness.BOUNDED, 1);
    }

    public static List<SeaTunnelRow> runBatchWithCheckpointEnabled(
            ReadonlyConfig options, TableSourceFactory factory) throws Exception {
        JobContext context = new JobContext(System.currentTimeMillis());
        context.setJobMode(JobMode.BATCH);
        context.setEnableCheckpoint(true);
        // TODO trigger checkpoint with interval
        return runWithContext(options, factory, context, Boundedness.BOUNDED, 1);
    }

    public static List<SeaTunnelRow> runParallelSubtasksBatchWithCheckpointDisabled(
            ReadonlyConfig options, TableSourceFactory factory, int parallelism) throws Exception {
        JobContext context = new JobContext(System.currentTimeMillis());
        context.setJobMode(JobMode.BATCH);
        context.setEnableCheckpoint(false);
        return runWithContext(options, factory, context, Boundedness.BOUNDED, parallelism);
    }

    private static List<SeaTunnelRow> runWithContext(
            ReadonlyConfig options,
            TableSourceFactory factory,
            JobContext context,
            Boundedness boundedness,
            int parallelism)
            throws Exception {
        SeaTunnelSource<Object, SourceSplit, Serializable> source =
                factory.createSource(
                                new TableSourceFactoryContext(
                                        options, Thread.currentThread().getContextClassLoader()))
                        .createSource();
        source.setJobContext(context);
        Set<Integer> registeredReaders = new HashSet<>();
        List<SourceReader> readers = new ArrayList<>();
        Set<Integer> unfinishedReaders = new HashSet<>();
        SourceSplitEnumerator enumerator =
                source.createEnumerator(
                        new SourceSplitEnumerator.Context<SourceSplit>() {
                            @Override
                            public int currentParallelism() {
                                return parallelism;
                            }

                            @Override
                            public Set<Integer> registeredReaders() {
                                return registeredReaders;
                            }

                            @Override
                            public void assignSplit(int subtaskId, List<SourceSplit> splits) {
                                if (registeredReaders().isEmpty()) {
                                    return;
                                }
                                SourceReader reader = readers.get(subtaskId);
                                if (splits.isEmpty()) {
                                    reader.handleNoMoreSplits();
                                } else {
                                    reader.addSplits(splits);
                                }
                            }

                            @Override
                            public void signalNoMoreSplits(int subtask) {
                                SourceReader reader = readers.get(subtask);
                                reader.handleNoMoreSplits();
                            }

                            @Override
                            public void sendEventToSourceReader(int subtaskId, SourceEvent event) {
                                SourceReader reader = readers.get(subtaskId);
                                reader.handleSourceEvent(event);
                            }

                            @Override
                            public MetricsContext getMetricsContext() {
                                return new AbstractMetricsContext() {};
                            }

                            @Override
                            public EventListener getEventListener() {
                                return event -> {};
                            }
                        });
        for (int i = 0; i < parallelism; i++) {
            int finalI = i;
            SourceReader<Object, SourceSplit> reader =
                    source.createReader(
                            new SourceReader.Context() {
                                @Override
                                public int getIndexOfSubtask() {
                                    return finalI;
                                }

                                @Override
                                public Boundedness getBoundedness() {
                                    return boundedness;
                                }

                                @Override
                                public void signalNoMoreElement() {
                                    unfinishedReaders.remove(finalI);
                                }

                                @Override
                                public void sendSplitRequest() {
                                    enumerator.handleSplitRequest(finalI);
                                }

                                @Override
                                public void sendSourceEventToEnumerator(SourceEvent sourceEvent) {
                                    enumerator.handleSourceEvent(finalI, sourceEvent);
                                }

                                @Override
                                public MetricsContext getMetricsContext() {
                                    return new AbstractMetricsContext() {};
                                }

                                @Override
                                public EventListener getEventListener() {
                                    return event -> {};
                                }
                            });
            unfinishedReaders.add(i);
            registeredReaders.add(i);
            readers.add(reader);
            enumerator.registerReader(i);
        }

        List<SeaTunnelRow> rows = new ArrayList<>();
        while (!unfinishedReaders.isEmpty()) {
            for (int i = 0; i < parallelism; i++) {
                SourceReader reader = readers.get(i);
                if (unfinishedReaders.contains(i)) {
                    reader.pollNext(
                            new Collector() {
                                @Override
                                public void collect(Object record) {
                                    rows.add((SeaTunnelRow) record);
                                }

                                @Override
                                public Object getCheckpointLock() {
                                    return reader;
                                }
                            });
                }
            }
        }
        enumerator.close();
        for (SourceReader reader : readers) {
            reader.close();
        }

        return rows;
    }
}
