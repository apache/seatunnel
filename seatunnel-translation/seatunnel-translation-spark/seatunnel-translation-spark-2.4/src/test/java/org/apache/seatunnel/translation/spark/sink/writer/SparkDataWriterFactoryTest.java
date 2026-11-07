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

package org.apache.seatunnel.translation.spark.sink.writer;

import org.apache.seatunnel.api.common.JobContext;
import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.serialization.DefaultSerializer;
import org.apache.seatunnel.api.sink.SeaTunnelSink;
import org.apache.seatunnel.api.sink.SinkWriter;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.CatalogTableUtil;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.connectors.doris.sink.DorisSink;
import org.apache.seatunnel.connectors.doris.sink.writer.DorisStreamLoad;
import org.apache.seatunnel.connectors.doris.sink.writer.LabelGenerator;

import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.MockedConstruction;

import java.io.Closeable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockConstruction;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class SparkDataWriterFactoryTest {
    @Test
    public void shouldUseDistinctDorisLabelsAcrossStreamingEpochs() throws Exception {
        SeaTunnelRowType rowType =
                new SeaTunnelRowType(
                        new String[] {"value"}, new SeaTunnelDataType[] {BasicType.STRING_TYPE});
        CatalogTable table =
                CatalogTableUtil.getCatalogTable("doris", "test", null, "epochs", rowType);
        DorisSink sink =
                spy(
                        new DorisSink(
                                ReadonlyConfig.fromMap(
                                        Map.of(
                                                "fenodes",
                                                "localhost:8030",
                                                "username",
                                                "root",
                                                "password",
                                                "",
                                                "database",
                                                "test",
                                                "table",
                                                "epochs",
                                                "sink.enable-2pc",
                                                true,
                                                "doris.config",
                                                Map.of("format", "json"))),
                                table));
        sink.setJobContext(new JobContext(13060L));
        List<SinkWriter<?, ?, ?>> writers = new ArrayList<>();
        doAnswer(
                        invocation -> {
                            SinkWriter<?, ?, ?> writer =
                                    (SinkWriter<?, ?, ?>) invocation.callRealMethod();
                            writers.add(writer);
                            return writer;
                        })
                .when(sink)
                .createWriter(any());
        List<String> labels = new ArrayList<>();
        List<Closeable> clients = new ArrayList<>();
        // Mock only the HTTP boundary; construct the production Doris writer and label generator.
        try (MockedConstruction<DorisStreamLoad> loads =
                mockConstruction(
                        DorisStreamLoad.class,
                        (load, context) -> {
                            labels.add(
                                    ((LabelGenerator) context.arguments().get(3))
                                            .generateLabel(1L));
                            clients.add((Closeable) context.arguments().get(4));
                        })) {
            SparkDataWriterFactory<?, ?> factory =
                    new SparkDataWriterFactory<>(sink, new CatalogTable[] {table}, "13060", 1);
            factory.createDataWriter(0, 99L, 0L);
            factory.createDataWriter(0, 100L, 1L);
            assertEquals(2, labels.size());
            assertNotEquals(
                    labels.get(0),
                    labels.get(1),
                    "A completed Doris label must not be reused in the next epoch");
        } finally {
            for (SinkWriter<?, ?, ?> writer : writers) {
                writer.close();
            }
            for (Closeable client : clients) {
                client.close();
            }
        }
    }

    @SuppressWarnings("unchecked")
    @Test
    public void shouldSeparateTaskIdentityFromStablePartitionAcrossRetriesAndEpochs()
            throws Exception {
        SeaTunnelSink<SeaTunnelRow, Object, Object, Object> sink = mock(SeaTunnelSink.class);
        SinkWriter<SeaTunnelRow, Object, Object> writer = mock(SinkWriter.class);
        when(sink.createWriter(any())).thenReturn(writer);
        when(sink.createCommitter()).thenReturn(Optional.empty());
        SeaTunnelRowType rowType =
                new SeaTunnelRowType(
                        new String[] {"value"}, new SeaTunnelDataType[] {BasicType.STRING_TYPE});
        CatalogTable catalogTable =
                CatalogTableUtil.getCatalogTable("test", "test", "test", "test", rowType);
        SparkDataWriterFactory<Object, Object> factory =
                new SparkDataWriterFactory<>(sink, new CatalogTable[] {catalogTable}, "job-id", 4);

        factory.createDataWriter(2, 99L, 0L);
        factory.createDataWriter(2, 100L, 0L);
        factory.createDataWriter(2, 101L, 0L);
        factory.createDataWriter(2, 102L, 1L);
        factory.createDataWriter(3, 103L, 1L);

        ArgumentCaptor<SinkWriter.Context> contextCaptor =
                ArgumentCaptor.forClass(SinkWriter.Context.class);
        verify(sink, times(5)).createWriter(contextCaptor.capture());
        DefaultSerializer<SinkWriter.Context> serializer = new DefaultSerializer<>();
        for (int i = 0; i < 5; i++) {
            SinkWriter.Context context = contextCaptor.getAllValues().get(i);
            context = serializer.deserialize(serializer.serialize(context));
            assertEquals(99 + i, context.getIndexOfSubtask());
            assertEquals(i == 4 ? 3 : 2, context.getStableSubtaskIndex());
            assertEquals(4, context.getNumberOfParallelSubtasks());
        }
    }
}
