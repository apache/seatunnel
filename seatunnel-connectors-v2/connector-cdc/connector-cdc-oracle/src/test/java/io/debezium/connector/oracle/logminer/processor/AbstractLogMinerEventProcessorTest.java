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

package io.debezium.connector.oracle.logminer.processor;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import io.debezium.connector.oracle.CommitScn;
import io.debezium.connector.oracle.OracleConnectorConfig;
import io.debezium.connector.oracle.OracleDatabaseSchema;
import io.debezium.connector.oracle.OracleOffsetContext;
import io.debezium.connector.oracle.OraclePartition;
import io.debezium.connector.oracle.OracleStreamingChangeEventSourceMetrics;
import io.debezium.connector.oracle.Scn;
import io.debezium.connector.oracle.logminer.events.LogMinerEventRow;
import io.debezium.connector.oracle.logminer.processor.memory.MemoryTransaction;
import io.debezium.pipeline.EventDispatcher;
import io.debezium.pipeline.source.spi.ChangeEventSource;

import static org.mockito.Mockito.CALLS_REAL_METHODS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.withSettings;

public class AbstractLogMinerEventProcessorTest {

    @Test
    void testCommitWithNoEventAndUpdateCommitScn() throws InterruptedException {

        OracleOffsetContext offsetContext = mock(OracleOffsetContext.class);
        OraclePartition partition = new OraclePartition("test");
        AbstractLogMinerEventProcessor<MemoryTransaction> processor =
                mock(
                        AbstractLogMinerEventProcessor.class,
                        withSettings()
                                .useConstructor(
                                        mock(ChangeEventSource.ChangeEventSourceContext.class),
                                        mock(OracleConnectorConfig.class),
                                        mock(OracleDatabaseSchema.class),
                                        partition,
                                        offsetContext,
                                        mock(EventDispatcher.class),
                                        mock(OracleStreamingChangeEventSourceMetrics.class))
                                .defaultAnswer(CALLS_REAL_METHODS));

        when(processor.getTransactionEventCount(mock(MemoryTransaction.class))).thenReturn(0);
        when(processor.getAndRemoveTransactionFromCache(Mockito.any()))
                .thenReturn(mock(MemoryTransaction.class));
        when(processor.getTransactionCacheMinimumScn()).thenReturn(Scn.valueOf(1));
        CommitScn commitScn = CommitScn.valueOf(1L);
        when(offsetContext.getCommitScn()).thenReturn(commitScn);
        LogMinerEventRow row = mock(LogMinerEventRow.class);
        when(row.getThread()).thenReturn(1);
        when(row.getScn()).thenReturn(Scn.valueOf(2));
        when(row.getTransactionId()).thenReturn("2");

        processor.handleCommit(partition, row);

        Assertions.assertEquals(commitScn.getMaxCommittedScn(), Scn.valueOf(2));
    }
}
