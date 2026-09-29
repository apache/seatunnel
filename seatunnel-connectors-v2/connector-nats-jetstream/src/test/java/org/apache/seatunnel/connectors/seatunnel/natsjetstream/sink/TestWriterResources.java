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

package org.apache.seatunnel.connectors.seatunnel.natsjetstream.sink;

import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.Column;
import org.apache.seatunnel.api.table.catalog.PhysicalColumn;
import org.apache.seatunnel.api.table.catalog.TableIdentifier;
import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;

import org.mockito.MockedStatic;
import org.mockito.Mockito;

import io.nats.client.Connection;
import io.nats.client.JetStream;
import io.nats.client.Nats;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

final class TestWriterResources implements AutoCloseable {

    private final MockedStatic<Nats> mockedNats;
    private final Connection connection;
    private final JetStream jetStream;

    private TestWriterResources(
            MockedStatic<Nats> mockedNats, Connection connection, JetStream jetStream) {
        this.mockedNats = mockedNats;
        this.connection = connection;
        this.jetStream = jetStream;
    }

    static TestWriterResources open(TestContext context) throws Exception {
        Connection connection = Mockito.mock(Connection.class);
        JetStream jetStream = Mockito.mock(JetStream.class);
        Mockito.when(connection.jetStream()).thenReturn(jetStream);
        MockedStatic<Nats> mockedNats = Mockito.mockStatic(Nats.class);
        mockedNats
                .when(() -> Nats.connect(Mockito.any(io.nats.client.Options.class)))
                .thenReturn(connection);
        return new TestWriterResources(mockedNats, connection, jetStream);
    }

    static CatalogTable catalogTable(SeaTunnelRowType rowType) {
        List<Column> columns = new ArrayList<>();
        for (int i = 0; i < rowType.getTotalFields(); i++) {
            SeaTunnelDataType<?> fieldType = rowType.getFieldType(i);
            columns.add(
                    PhysicalColumn.builder()
                            .name(rowType.getFieldName(i))
                            .dataType(fieldType)
                            .nullable(true)
                            .build());
        }
        return CatalogTable.of(
                TableIdentifier.of("default", "default", "nats_test"),
                TableSchema.builder().columns(columns).build(),
                new HashMap<>(),
                new ArrayList<>(),
                "nats test table");
    }

    Connection getConnection() {
        return connection;
    }

    JetStream getJetStream() {
        return jetStream;
    }

    @Override
    public void close() {
        mockedNats.close();
    }
}
