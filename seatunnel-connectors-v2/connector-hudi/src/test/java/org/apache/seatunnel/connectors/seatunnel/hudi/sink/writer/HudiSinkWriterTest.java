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

package org.apache.seatunnel.connectors.seatunnel.hudi.sink.writer;

import org.apache.seatunnel.api.sink.MultiTableResourceManager;
import org.apache.seatunnel.api.sink.SinkWriter;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.common.utils.function.RunnableWithException;
import org.apache.seatunnel.connectors.seatunnel.hudi.config.HudiSinkConfig;
import org.apache.seatunnel.connectors.seatunnel.hudi.config.HudiTableConfig;
import org.apache.seatunnel.connectors.seatunnel.hudi.exception.HudiConnectorException;
import org.apache.seatunnel.connectors.seatunnel.hudi.exception.HudiErrorCode;
import org.apache.seatunnel.connectors.seatunnel.hudi.sink.HudiClientManager;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.MockedConstruction;
import org.mockito.Mockito;

import java.util.Optional;

import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class HudiSinkWriterTest {

    @Test
    void shouldRegisterAndExecuteTimerFlush() throws Exception {
        SinkWriter.Context context = mock(SinkWriter.Context.class);
        ArgumentCaptor<RunnableWithException> actionCaptor =
                ArgumentCaptor.forClass(RunnableWithException.class);

        try (MockedConstruction<HudiRecordWriter> recordWriters = createWriter(context)) {
            verify(context, times(1)).registerFlushAction(actionCaptor.capture());

            actionCaptor.getValue().run();

            verify(recordWriters.constructed().get(0), times(1)).flush();
        }
    }

    @Test
    void shouldPropagateTimerFlushFailure() throws Exception {
        SinkWriter.Context context = mock(SinkWriter.Context.class);
        ArgumentCaptor<RunnableWithException> actionCaptor =
                ArgumentCaptor.forClass(RunnableWithException.class);

        try (MockedConstruction<HudiRecordWriter> recordWriters = createWriter(context)) {
            HudiConnectorException expected =
                    new HudiConnectorException(
                            HudiErrorCode.FLUSH_DATA_FAILED, "timer flush failed");
            doThrow(expected).when(recordWriters.constructed().get(0)).flush();
            verify(context).registerFlushAction(actionCaptor.capture());

            HudiConnectorException actual =
                    Assertions.assertThrows(
                            HudiConnectorException.class, actionCaptor.getValue()::run);

            Assertions.assertSame(expected, actual);
        }
    }

    @Test
    void shouldFlushCurrentRecordWriterAfterResourceManagerReplacement() throws Exception {
        SinkWriter.Context context = mock(SinkWriter.Context.class);
        ArgumentCaptor<RunnableWithException> actionCaptor =
                ArgumentCaptor.forClass(RunnableWithException.class);
        MultiTableResourceManager<HudiClientManager> resourceManager =
                mock(MultiTableResourceManager.class);
        when(resourceManager.getSharedResource())
                .thenReturn(Optional.of(mock(HudiClientManager.class)));

        try (MockedConstruction<HudiRecordWriter> recordWriters =
                Mockito.mockConstruction(HudiRecordWriter.class)) {
            HudiSinkWriter writer =
                    new HudiSinkWriter(
                            context,
                            mock(SeaTunnelRowType.class),
                            mock(HudiSinkConfig.class),
                            mock(HudiTableConfig.class));
            verify(context).registerFlushAction(actionCaptor.capture());

            writer.setMultiTableResourceManager(resourceManager, 0);
            actionCaptor.getValue().run();

            Assertions.assertEquals(2, recordWriters.constructed().size());
            verify(recordWriters.constructed().get(0), never()).flush();
            verify(recordWriters.constructed().get(1)).flush();
        }
    }

    private MockedConstruction<HudiRecordWriter> createWriter(SinkWriter.Context context) {
        MockedConstruction<HudiRecordWriter> recordWriters =
                Mockito.mockConstruction(HudiRecordWriter.class);
        new HudiSinkWriter(
                context,
                mock(SeaTunnelRowType.class),
                mock(HudiSinkConfig.class),
                mock(HudiTableConfig.class));
        return recordWriters;
    }
}
