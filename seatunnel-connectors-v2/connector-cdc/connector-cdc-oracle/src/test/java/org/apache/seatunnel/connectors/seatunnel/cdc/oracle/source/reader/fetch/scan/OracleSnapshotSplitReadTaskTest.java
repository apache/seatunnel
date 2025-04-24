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

package org.apache.seatunnel.connectors.seatunnel.cdc.oracle.source.reader.fetch.scan;

import org.apache.seatunnel.connectors.cdc.base.relational.JdbcSourceEventDispatcher;
import org.apache.seatunnel.connectors.cdc.base.source.split.SnapshotSplit;
import org.apache.seatunnel.connectors.seatunnel.cdc.oracle.utils.OracleConnectionUtils;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;

import io.debezium.connector.oracle.OracleConnection;
import io.debezium.connector.oracle.OracleConnectorConfig;
import io.debezium.connector.oracle.OracleDatabaseSchema;
import io.debezium.connector.oracle.OracleOffsetContext;
import io.debezium.connector.oracle.OraclePartition;
import io.debezium.jdbc.JdbcConnection;
import io.debezium.pipeline.source.AbstractSnapshotChangeEventSource.SnapshotContext;
import io.debezium.pipeline.source.spi.SnapshotProgressListener;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
public class OracleSnapshotSplitReadTaskTest {

    @Mock private OracleConnectorConfig connectorConfig;
    @Mock private OracleOffsetContext previousOffset;
    @Mock private SnapshotProgressListener<OraclePartition> snapshotProgressListener;
    @Mock private OracleDatabaseSchema databaseSchema;
    @Mock private OracleConnection jdbcConnection;
    @Mock private JdbcSourceEventDispatcher<OraclePartition> dispatcher;
    @Mock private SnapshotSplit snapshotSplit;

    @Test
    public void testPrepareWithPdbName() throws Exception {
        // Prepare test data
        String pdbName = "PDB1";
        String currentContainerName = "CDB$ROOT";
        OraclePartition partition = new OraclePartition("test");

        // Configure mock behavior
        when(connectorConfig.getPdbName()).thenReturn(pdbName);
        Mockito.mockStatic(OracleConnectionUtils.class);
        when(OracleConnectionUtils.getCurrentContainerName(Mockito.any(JdbcConnection.class)))
                .thenReturn(currentContainerName);

        // Create test instance
        OracleSnapshotSplitReadTask task =
                new OracleSnapshotSplitReadTask(
                        connectorConfig,
                        previousOffset,
                        snapshotProgressListener,
                        databaseSchema,
                        jdbcConnection,
                        dispatcher,
                        snapshotSplit);

        // Execute test
        SnapshotContext<OraclePartition, OracleOffsetContext> context = task.prepare(partition);

        // Verify results
        assertNotNull(context);
        assertEquals(partition, context.partition);
    }

    @Test
    public void testPrepareWithoutPdbName() throws Exception {
        // Prepare test data
        OraclePartition partition = new OraclePartition("test");

        // Configure mock behavior
        when(connectorConfig.getPdbName()).thenReturn(null);

        // Create test instance
        OracleSnapshotSplitReadTask task =
                new OracleSnapshotSplitReadTask(
                        connectorConfig,
                        previousOffset,
                        snapshotProgressListener,
                        databaseSchema,
                        jdbcConnection,
                        dispatcher,
                        snapshotSplit);

        // Execute test
        SnapshotContext<OraclePartition, OracleOffsetContext> context = task.prepare(partition);

        // Verify results
        assertNotNull(context);
        assertEquals(partition, context.partition);
    }
}
