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

package org.apache.seatunnel.connectors.seatunnel.neo4j;

import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.connectors.seatunnel.neo4j.config.DriverBuilder;
import org.apache.seatunnel.connectors.seatunnel.neo4j.config.Neo4jSinkQueryInfo;
import org.apache.seatunnel.connectors.seatunnel.neo4j.config.Neo4jSourceQueryInfo;
import org.apache.seatunnel.connectors.seatunnel.neo4j.exception.Neo4jConnectorException;
import org.apache.seatunnel.connectors.seatunnel.neo4j.sink.Neo4jSinkWriter;
import org.apache.seatunnel.connectors.seatunnel.neo4j.source.Neo4jSourceReader;

import org.junit.jupiter.api.Test;
import org.neo4j.driver.Driver;
import org.neo4j.driver.Session;
import org.neo4j.driver.SessionConfig;
import org.neo4j.driver.TransactionWork;
import org.neo4j.driver.exceptions.ServiceUnavailableException;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Both the sink writer and the source reader release the session and the driver as consecutive
 * statements, so anything that throws part way through leaks the rest. The sink is the sharper
 * case: {@code close()} flushes the last batch first, and {@code writeByQuery} deliberately
 * rethrows, so a failing final flush leaves a whole {@link Driver} — and its Netty event loop —
 * behind.
 */
class Neo4jCloseTest {

    private static final SeaTunnelRowType ROW_TYPE =
            new SeaTunnelRowType(
                    new String[] {"name"}, new SeaTunnelDataType[] {BasicType.STRING_TYPE});

    @Test
    void sinkWriterClosesSessionAndDriverWhenTheFinalFlushFails() throws Exception {
        Session session = mock(Session.class);
        Driver driver = mock(Driver.class);
        DriverBuilder driverBuilder = mock(DriverBuilder.class);
        Neo4jSinkQueryInfo queryInfo = mock(Neo4jSinkQueryInfo.class);

        when(driverBuilder.build()).thenReturn(driver);
        // SessionConfig.forDatabase rejects null and empty, so this has to be a real name.
        when(driverBuilder.getDatabase()).thenReturn("neo4j");
        when(driver.session(any(SessionConfig.class))).thenReturn(session);
        when(queryInfo.getDriverBuilder()).thenReturn(driverBuilder);
        when(queryInfo.batchMode()).thenReturn(true);
        // Two rows per batch, one row written: the buffer is still full at close() time.
        when(queryInfo.getMaxBatchSize()).thenReturn(2);
        when(queryInfo.getQuery()).thenReturn("UNWIND $batch AS row CREATE (n) SET n = row");
        when(session.writeTransaction(any(TransactionWork.class)))
                .thenThrow(new ServiceUnavailableException("connection refused"));

        Neo4jSinkWriter writer = new Neo4jSinkWriter(queryInfo, ROW_TYPE);
        writer.write(new SeaTunnelRow(new Object[] {"a"}));

        assertThrows(Neo4jConnectorException.class, writer::close);

        verify(session).close();
        verify(driver).close();
    }

    @Test
    void sourceReaderClosesDriverWhenTheSessionFailsToClose() throws Exception {
        Session session = mock(Session.class);
        Driver driver = mock(Driver.class);
        DriverBuilder driverBuilder = mock(DriverBuilder.class);
        Neo4jSourceQueryInfo queryInfo = mock(Neo4jSourceQueryInfo.class);

        when(driverBuilder.build()).thenReturn(driver);
        when(driverBuilder.getDatabase()).thenReturn("neo4j");
        when(driver.session(any(SessionConfig.class))).thenReturn(session);
        when(queryInfo.getDriverBuilder()).thenReturn(driverBuilder);
        when(queryInfo.getQuery()).thenReturn("MATCH (n) RETURN n");
        doThrowOnClose(session);

        Neo4jSourceReader reader = new Neo4jSourceReader(null, queryInfo, ROW_TYPE);
        reader.open();

        assertThrows(ServiceUnavailableException.class, reader::close);

        verify(driver).close();
    }

    private static void doThrowOnClose(Session session) {
        org.mockito.Mockito.doThrow(new ServiceUnavailableException("connection refused"))
                .when(session)
                .close();
    }
}
