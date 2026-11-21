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

package org.apache.seatunnel.connectors.seatunnel.openmldb.source;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.source.Boundedness;
import org.apache.seatunnel.api.source.Collector;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.CatalogTableUtil;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.connectors.seatunnel.common.source.SingleSplitReaderContext;
import org.apache.seatunnel.connectors.seatunnel.openmldb.config.OpenMldbParameters;
import org.apache.seatunnel.connectors.seatunnel.openmldb.exception.OpenMldbConnectorException;

import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.MockedConstruction;

import java.sql.SQLException;
import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockConstruction;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

class OpenMldbSourceReaderTest {
    private final CatalogTable first = table("first", "id = STRING");
    private final CatalogTable second = table("second", "value = INT");
    private final SingleSplitReaderContext context = mock(SingleSplitReaderContext.class);
    private final Collector<SeaTunnelRow> output = mock(Collector.class);

    @Test
    void readsDifferentSchemasAndCompletesOnlyOnce() throws Exception {
        OpenMldbReadClient.Query firstQuery = query(new SeaTunnelRow(new Object[] {"key"}));
        OpenMldbReadClient.Query secondQuery = query(new SeaTunnelRow(new Object[] {null}));
        when(context.getBoundedness()).thenReturn(Boundedness.BOUNDED);
        try (MockedConstruction<OpenMldbReadClient> clients =
                mockConstruction(
                        OpenMldbReadClient.class,
                        (client, ignored) -> {
                            when(client.execute(
                                            "one",
                                            "select * from first",
                                            first.getSeaTunnelRowType(),
                                            true))
                                    .thenReturn(firstQuery);
                            when(client.execute(
                                            "two",
                                            "select * from second",
                                            second.getSeaTunnelRowType(),
                                            true))
                                    .thenReturn(secondQuery);
                        })) {
            OpenMldbSourceReader reader = reader();
            reader.open();
            reader.pollNext(output);
            reader.pollNext(output);
            ArgumentCaptor<SeaTunnelRow> rows = ArgumentCaptor.forClass(SeaTunnelRow.class);
            verify(output, times(2)).collect(rows.capture());
            assertEquals(
                    first.getTableId().toTablePath().toString(),
                    rows.getAllValues().get(0).getTableId());
            assertEquals(
                    second.getTableId().toTablePath().toString(),
                    rows.getAllValues().get(1).getTableId());
            assertNull(rows.getAllValues().get(1).getField(0));
            verify(context).signalNoMoreElement();
            verify(firstQuery).close();
            verify(secondQuery).close();
            reader.close();
            reader.close();
            verify(clients.constructed().get(0)).close();
        }
    }

    @Test
    void emptyFirstTableDoesNotSkipSecondTable() throws Exception {
        OpenMldbReadClient.Query empty = mock(OpenMldbReadClient.Query.class);
        OpenMldbReadClient.Query populated = query(new SeaTunnelRow(new Object[] {7}));
        try (MockedConstruction<OpenMldbReadClient> ignored =
                mockConstruction(
                        OpenMldbReadClient.class,
                        (client, construction) -> {
                            when(client.execute(anyString(), anyString(), any(), anyBoolean()))
                                    .thenReturn(empty, populated);
                        })) {
            try (OpenMldbSourceReader reader = reader()) {
                reader.open();
                reader.pollNext(output);
            }
            verify(output).collect(any(SeaTunnelRow.class));
            verify(empty).close();
            verify(populated).close();
        }
    }

    @Test
    void queryFailureDoesNotReportSuccessfulCompletion() throws Exception {
        when(context.getBoundedness()).thenReturn(Boundedness.BOUNDED);
        try (MockedConstruction<OpenMldbReadClient> ignored =
                mockConstruction(
                        OpenMldbReadClient.class,
                        (client, construction) -> {
                            when(client.execute(anyString(), anyString(), any(), anyBoolean()))
                                    .thenThrow(new SQLException("query rejected"));
                        })) {
            try (OpenMldbSourceReader reader = reader()) {
                reader.open();
                OpenMldbConnectorException error =
                        assertThrows(
                                OpenMldbConnectorException.class, () -> reader.pollNext(output));
                assertTrue(error.getMessage().contains("first"));
                assertInstanceOf(SQLException.class, error.getCause());
            }
            verify(context, never()).signalNoMoreElement();
            verifyNoInteractions(output);
        }
    }

    @Test
    void collectorFailureClosesQueryAndDoesNotComplete() throws Exception {
        OpenMldbReadClient.Query query = query(new SeaTunnelRow(new Object[] {"key"}));
        doThrow(new IllegalStateException("collector failed"))
                .when(output)
                .collect(any(SeaTunnelRow.class));
        try (MockedConstruction<OpenMldbReadClient> ignored =
                mockConstruction(
                        OpenMldbReadClient.class,
                        (client, construction) -> {
                            when(client.execute(anyString(), anyString(), any(), anyBoolean()))
                                    .thenReturn(query);
                        })) {
            try (OpenMldbSourceReader reader = reader()) {
                reader.open();
                assertThrows(OpenMldbConnectorException.class, () -> reader.pollNext(output));
            }
            verify(query).close();
            verify(context, never()).signalNoMoreElement();
        }
    }

    @Test
    void streamingRepeatsQueriesWithoutCompleting() throws Exception {
        when(context.getBoundedness()).thenReturn(Boundedness.UNBOUNDED);
        try (MockedConstruction<OpenMldbReadClient> clients =
                mockConstruction(
                        OpenMldbReadClient.class,
                        (client, construction) -> {
                            when(client.execute(anyString(), anyString(), any(), anyBoolean()))
                                    .thenAnswer(invocation -> mock(OpenMldbReadClient.Query.class));
                        })) {
            try (OpenMldbSourceReader reader = reader()) {
                reader.open();
                reader.pollNext(output);
                reader.pollNext(output);
                verify(clients.constructed().get(0), times(4))
                        .execute(anyString(), anyString(), any(), anyBoolean());
            }
            verify(context, never()).signalNoMoreElement();
        }
    }

    @Test
    void closingOneReaderDoesNotCloseAnother() throws Exception {
        try (MockedConstruction<OpenMldbReadClient> clients =
                mockConstruction(OpenMldbReadClient.class)) {
            OpenMldbSourceReader firstReader = reader();
            OpenMldbSourceReader secondReader = reader();
            firstReader.open();
            secondReader.open();
            firstReader.close();
            verify(clients.constructed().get(0)).close();
            verifyNoInteractions(clients.constructed().get(1));
            secondReader.close();
            verify(clients.constructed().get(1)).close();
        }
    }

    private OpenMldbSourceReader reader() {
        return new OpenMldbSourceReader(
                Arrays.asList(parameters("one", "first"), parameters("two", "second")),
                Arrays.asList(first, second),
                true,
                context);
    }

    private static OpenMldbReadClient.Query query(SeaTunnelRow row) {
        OpenMldbReadClient.Query query = mock(OpenMldbReadClient.Query.class);
        when(query.next()).thenReturn(true, false);
        when(query.readRow()).thenReturn(row);
        return query;
    }

    private static CatalogTable table(String name, String fields) {
        return CatalogTableUtil.buildWithConfig(
                ReadonlyConfig.fromConfig(
                        org.apache.seatunnel.shade.com.typesafe.config.ConfigFactory.parseString(
                                "schema { table = " + name + ", fields { " + fields + " } }")));
    }

    private static OpenMldbParameters parameters(String database, String table) {
        return OpenMldbParameters.buildWithConfig(
                org.apache.seatunnel.shade.com.typesafe.config.ConfigFactory.parseString(
                        "cluster_mode = false\nhost = localhost\nport = 6527\ndatabase = "
                                + database
                                + "\nsql = \"select * from "
                                + table
                                + "\""));
    }
}
