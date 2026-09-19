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

package org.apache.seatunnel.connectors.seatunnel.jdbc.source;

import org.apache.seatunnel.api.common.metrics.MetricsContext;
import org.apache.seatunnel.api.event.EventListener;
import org.apache.seatunnel.api.source.Boundedness;
import org.apache.seatunnel.api.source.SourceEvent;
import org.apache.seatunnel.api.source.SourceReader;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.connectors.seatunnel.jdbc.config.JdbcConnectionConfig;
import org.apache.seatunnel.connectors.seatunnel.jdbc.config.JdbcSourceConfig;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

class JdbcSourceReaderTest {

    @Test
    void testOpenRequestsSplitsWhenLocalQueueIsEmpty() throws Exception {
        AtomicInteger splitRequestCount = new AtomicInteger();
        SourceReader.Context context =
                new SourceReader.Context() {
                    @Override
                    public int getIndexOfSubtask() {
                        return 0;
                    }

                    @Override
                    public Boundedness getBoundedness() {
                        return Boundedness.BOUNDED;
                    }

                    @Override
                    public void signalNoMoreElement() {}

                    @Override
                    public void sendSplitRequest() {
                        splitRequestCount.incrementAndGet();
                    }

                    @Override
                    public void sendSourceEventToEnumerator(SourceEvent sourceEvent) {}

                    @Override
                    public MetricsContext getMetricsContext() {
                        return null;
                    }

                    @Override
                    public EventListener getEventListener() {
                        return null;
                    }
                };

        JdbcSourceConfig config =
                JdbcSourceConfig.builder()
                        .jdbcConnectionConfig(
                                JdbcConnectionConfig.builder()
                                        .url("jdbc:generic://localhost:0/test")
                                        .driverName("org.example.Driver")
                                        .build())
                        .splitAssignBatchSize(4)
                        .build();

        JdbcSourceReader reader =
                new JdbcSourceReader(
                        context,
                        config,
                        Collections
                                .<TablePath, org.apache.seatunnel.api.table.catalog.CatalogTable>
                                        emptyMap());
        reader.open();

        Assertions.assertEquals(1, splitRequestCount.get());

        // Watermark is max(1, batchSize/2) = 2; fill the local queue to at least that level.
        List<JdbcSourceSplit> splits = new ArrayList<>();
        for (int i = 0; i < 2; i++) {
            splits.add(
                    new JdbcSourceSplit(
                            TablePath.of("db", "schema", "table"),
                            "split-" + i,
                            "SELECT 1",
                            "id",
                            null,
                            i,
                            i + 1));
        }
        reader.addSplits(splits);
        // Local queue is at/above the watermark; no additional request until drained.
        Assertions.assertEquals(1, splitRequestCount.get());

        reader.handleNoMoreSplits();
        Assertions.assertEquals(1, splitRequestCount.get());
        reader.close();
    }
}
