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

package org.apache.seatunnel.connectors.seatunnel.cdc.mysql.source.reader.fetch;

import org.apache.seatunnel.connectors.cdc.base.option.StartupMode;
import org.apache.seatunnel.connectors.cdc.base.source.split.IncrementalSplit;
import org.apache.seatunnel.connectors.cdc.base.source.split.state.IncrementalSplitState;
import org.apache.seatunnel.connectors.seatunnel.cdc.mysql.config.CustomMySqlConnectionConfiguration;
import org.apache.seatunnel.connectors.seatunnel.cdc.mysql.source.offset.BinlogOffset;
import org.apache.seatunnel.connectors.seatunnel.cdc.mysql.utils.MySqlConnectionUtils;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.mockito.Mockito;

import io.debezium.config.Configuration;
import io.debezium.connector.mysql.MySqlConnection;
import io.debezium.jdbc.JdbcConnection;
import io.debezium.relational.TableId;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collections;

public class MySqlTimestampStartupOffsetTest {

    @Test
    public void testTimestampStartupResolvesConfiguredTimestampOnlyForBootstrapOffset() {
        BinlogOffset timestampOffset = new BinlogOffset(1716076800L);
        BinlogOffset restoredOffset = new BinlogOffset("mysql-bin.000021", 4096L);

        Assertions.assertTrue(
                MySqlSourceFetchTaskContext.shouldResolveTimestampStartupOffset(
                        StartupMode.TIMESTAMP, timestampOffset));
        Assertions.assertFalse(
                MySqlSourceFetchTaskContext.shouldResolveTimestampStartupOffset(
                        StartupMode.TIMESTAMP, restoredOffset));
    }

    @Test
    public void testCheckpointRestoreUsesPersistedBinlogOffsetAfterTimestampBootstrap() {
        BinlogOffset timestampOffset = new BinlogOffset(1716076800L);
        IncrementalSplit incrementalSplit =
                new IncrementalSplit(
                        "incremental-split",
                        Collections.singletonList(TableId.parse("test.orders")),
                        timestampOffset,
                        BinlogOffset.NO_STOPPING_OFFSET,
                        Collections.emptyList());

        IncrementalSplitState splitState = new IncrementalSplitState(incrementalSplit);
        BinlogOffset checkpointOffset = new BinlogOffset("mysql-bin.000021", 4096L);
        splitState.setStartupOffset(checkpointOffset);

        IncrementalSplit restoredSplit = splitState.toSourceSplit();

        Assertions.assertEquals(checkpointOffset, restoredSplit.getStartupOffset());
        Assertions.assertFalse(
                MySqlSourceFetchTaskContext.shouldResolveTimestampStartupOffset(
                        StartupMode.TIMESTAMP, restoredSplit.getStartupOffset()));
    }

    @ParameterizedTest
    @CsvSource({
        "true, SHOW MASTER STATUS, SHOW BINARY LOGS",
        "false, SHOW MASTER STATUS, SHOW MASTER LOGS",
        "false, SHOW BINARY LOG STATUS, SHOW BINARY LOGS"
    })
    public void testBinlogListingForEarliestAndTimestamp(
            boolean preferBinaryLogListing, String statusStatement, String listingStatement)
            throws SQLException {
        try (RecordingConnection jdbc =
                new RecordingConnection(preferBinaryLogListing, statusStatement)) {
            BinlogOffset earliest = MySqlConnectionUtils.earliestBinlogOffset(jdbc);
            Assertions.assertEquals("binlog.000001", earliest.getFilename());
            Assertions.assertEquals(4L, earliest.getPosition());
            Assertions.assertEquals(listingStatement, jdbc.lastStatement);

            BinlogOffset timestamp =
                    MySqlConnectionUtils.findBinlogOffsetBytimestamp(jdbc, null, 1234L);
            Assertions.assertEquals(BinlogOffset.INITIAL_OFFSET, timestamp);
            Assertions.assertEquals(listingStatement, jdbc.lastStatement);
            Assertions.assertEquals(statusStatement, jdbc.binaryLogStatusStatement());
        }
    }

    private static class RecordingConnection extends MySqlConnection {
        private final String statusStatement;
        private String lastStatement;

        RecordingConnection(boolean preferBinaryLogListing, String statusStatement) {
            super(
                    new CustomMySqlConnectionConfiguration(
                            Configuration.create().with("database.hostname", "localhost").build()),
                    preferBinaryLogListing);
            this.statusStatement = statusStatement;
        }

        @Override
        public String binaryLogStatusStatement() {
            return statusStatement;
        }

        @Override
        public JdbcConnection query(String statement, ResultSetConsumer consumer)
                throws SQLException {
            lastStatement = statement;
            consumer.accept(Mockito.mock(ResultSet.class));
            return this;
        }

        @Override
        public <T> T queryAndMap(String statement, ResultSetMapper<T> mapper) throws SQLException {
            lastStatement = statement;
            ResultSet result = Mockito.mock(ResultSet.class);
            Mockito.when(result.next()).thenReturn(true);
            Mockito.when(result.getString(1)).thenReturn("binlog.000001");
            return mapper.apply(result);
        }
    }
}
