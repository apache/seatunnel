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

package org.apache.seatunnel.connectors.seatunnel.bigtable.sink;

import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.connectors.seatunnel.bigtable.client.BigtableClient;
import org.apache.seatunnel.connectors.seatunnel.bigtable.config.BigtableParameters;
import org.apache.seatunnel.connectors.seatunnel.bigtable.config.BigtableSinkOptions;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

public class BigtableSinkWriterTest {

    private BigtableClient mockClient;
    private SeaTunnelRowType rowType;
    private BigtableParameters parameters;

    @BeforeEach
    void setUp() {
        mockClient = Mockito.mock(BigtableClient.class);

        rowType =
                new SeaTunnelRowType(
                        new String[] {"id", "name", "age"},
                        new org.apache.seatunnel.api.table.type.SeaTunnelDataType[] {
                            BasicType.STRING_TYPE, BasicType.STRING_TYPE, BasicType.INT_TYPE
                        });

        Map<String, String> familyMap = new HashMap<>();
        familyMap.put("all_columns", "cf");

        parameters =
                BigtableParameters.builder()
                        .projectId("test-project")
                        .instanceId("test-instance")
                        .table("test-table")
                        .rowkeyColumns(Arrays.asList("id"))
                        .columnFamily(familyMap)
                        .batchMutationSize(10)
                        .nullMode(BigtableSinkOptions.NullMode.SKIP)
                        .build();
    }

    @Test
    void testWriteFlushesOnBatchSize() throws IOException {
        List<Integer> rowkeyIndexes = Arrays.asList(0);
        BigtableSinkWriter writer =
                new BigtableSinkWriter(rowType, parameters, rowkeyIndexes, -1, mockClient);

        // Write batchMutationSize rows to trigger a flush
        for (int i = 0; i < parameters.getBatchMutationSize(); i++) {
            SeaTunnelRow row = new SeaTunnelRow(3);
            row.setField(0, "key-" + i);
            row.setField(1, "name-" + i);
            row.setField(2, i);
            writer.write(row);
        }

        verify(mockClient, times(1)).bulkMutate(anyList());
    }

    @Test
    void testCloseFlushesRemainingRows() throws IOException {
        List<Integer> rowkeyIndexes = Arrays.asList(0);
        BigtableSinkWriter writer =
                new BigtableSinkWriter(rowType, parameters, rowkeyIndexes, -1, mockClient);

        // Write fewer rows than batch size
        SeaTunnelRow row = new SeaTunnelRow(3);
        row.setField(0, "key-1");
        row.setField(1, "alice");
        row.setField(2, 30);
        writer.write(row);

        // No flush yet
        verify(mockClient, times(0)).bulkMutate(anyList());

        // Close should flush
        writer.close();
        verify(mockClient, times(1)).bulkMutate(anyList());
        verify(mockClient, times(1)).close();
    }

    @Test
    void testNullFieldSkipped() throws IOException {
        List<Integer> rowkeyIndexes = Arrays.asList(0);
        BigtableSinkWriter writer =
                new BigtableSinkWriter(rowType, parameters, rowkeyIndexes, -1, mockClient);

        SeaTunnelRow row = new SeaTunnelRow(3);
        row.setField(0, "key-null");
        row.setField(1, null); // should be skipped
        row.setField(2, 25);
        writer.write(row);
        writer.close();

        // Verify a mutation was still produced (null field just omitted)
        verify(mockClient, times(1)).bulkMutate(anyList());
    }
}
