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

package org.apache.seatunnel.connectors.seatunnel.starrocks.sink;

import org.apache.seatunnel.api.sink.SinkWriter;
import org.apache.seatunnel.api.table.catalog.PhysicalColumn;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.common.utils.function.RunnableWithException;
import org.apache.seatunnel.connectors.seatunnel.starrocks.client.StarRocksSinkManager;
import org.apache.seatunnel.connectors.seatunnel.starrocks.config.SinkConfig;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.MockedConstruction;
import org.mockito.Mockito;

import java.io.IOException;

import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

public class StarRocksSinkWriterTest {

    @Test
    void shouldRegisterTimerFlushAction() throws Exception {
        SinkWriter.Context context = mock(SinkWriter.Context.class);
        ArgumentCaptor<RunnableWithException> actionCaptor =
                ArgumentCaptor.forClass(RunnableWithException.class);

        try (MockedConstruction<StarRocksSinkManager> mockedManager =
                Mockito.mockConstruction(StarRocksSinkManager.class)) {
            createWriter(context);

            verify(context, times(1)).registerFlushAction(actionCaptor.capture());
            actionCaptor.getValue().run();
            verify(mockedManager.constructed().get(0), times(1)).flush();
        }
    }

    @Test
    void shouldPropagateTimerFlushFailure() throws Exception {
        SinkWriter.Context context = mock(SinkWriter.Context.class);
        ArgumentCaptor<RunnableWithException> actionCaptor =
                ArgumentCaptor.forClass(RunnableWithException.class);

        try (MockedConstruction<StarRocksSinkManager> mockedManager =
                Mockito.mockConstruction(StarRocksSinkManager.class)) {
            createWriter(context);
            StarRocksSinkManager manager = mockedManager.constructed().get(0);
            IOException expected = new IOException("timer flush failed");
            doThrow(expected).when(manager).flush();

            verify(context).registerFlushAction(actionCaptor.capture());
            IOException actual =
                    Assertions.assertThrows(IOException.class, actionCaptor.getValue()::run);
            Assertions.assertSame(expected, actual);
        }
    }

    private StarRocksSinkWriter createWriter(SinkWriter.Context context) {
        SinkConfig sinkConfig = new SinkConfig();
        sinkConfig.setLoadFormat(SinkConfig.StreamLoadFormat.JSON);
        TableSchema tableSchema =
                TableSchema.builder()
                        .column(
                                PhysicalColumn.of(
                                        "id", BasicType.INT_TYPE, (Long) null, false, null, null))
                        .build();
        return new StarRocksSinkWriter(
                context, sinkConfig, tableSchema, TablePath.of("test", "timer_flush"));
    }
}
