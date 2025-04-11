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

package org.apache.seatunnel.connectors.doris.split;

import org.apache.seatunnel.shade.com.google.common.collect.Maps;

import org.apache.seatunnel.api.source.SourceSplitEnumerator;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.connectors.doris.config.DorisSourceConfig;
import org.apache.seatunnel.connectors.doris.rest.PartitionDefinition;
import org.apache.seatunnel.connectors.doris.rest.RestService;
import org.apache.seatunnel.connectors.doris.source.DorisSourceTable;
import org.apache.seatunnel.connectors.doris.source.split.DorisSourceSplit;
import org.apache.seatunnel.connectors.doris.source.split.DorisSourceSplitEnumerator;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.mockito.ArgumentMatchers.any;

@Slf4j
public class DorisSourceSplitEnumeratorTest {

    private static final String DATABASE = "default";
    private static final String TABLE = "default_table";
    private static final String BE_ADDRESS_PREFIX = "doris-be-";
    private static final String QUERY_PLAN = "DAABDAACDwABDAAAAAEIAA";

    private static final int PARALLELISM = 4;

    private static final int PARTITION_NUMS = 10;

    @Test
    public void dorisSourceSplitEnumeratorTest() {
        DorisSourceConfig dorisSourceConfig = Mockito.mock(DorisSourceConfig.class);
        DorisSourceTable dorisSourceTable = Mockito.mock(DorisSourceTable.class);

        SourceSplitEnumerator.Context<DorisSourceSplit> context =
                Mockito.mock(SourceSplitEnumerator.Context.class);

        Mockito.when(context.registeredReaders())
                .thenReturn(IntStream.range(0, PARALLELISM).boxed().collect(Collectors.toSet()));
        Mockito.when(context.currentParallelism()).thenReturn(PARALLELISM);

        Map<TablePath, DorisSourceTable> dorisSourceTableMap = Maps.newHashMap();
        dorisSourceTableMap.put(new TablePath(DATABASE, null, TABLE), dorisSourceTable);

        DorisSourceSplitEnumerator dorisSourceSplitEnumerator =
                new DorisSourceSplitEnumerator(context, dorisSourceConfig, dorisSourceTableMap);

        MockedStatic<RestService> restServiceMockedStatic = Mockito.mockStatic(RestService.class);

        restServiceMockedStatic
                .when(() -> RestService.findPartitions(any(), any(), any()))
                .thenReturn(buildPartitionDefinitions());

        dorisSourceSplitEnumerator.run();

        ArgumentCaptor<Integer> subtaskId = ArgumentCaptor.forClass(Integer.class);
        ArgumentCaptor<List> split = ArgumentCaptor.forClass(List.class);

        Mockito.verify(context, Mockito.times(PARALLELISM))
                .assignSplit(subtaskId.capture(), split.capture());

        List<Integer> subTaskAllValues = subtaskId.getAllValues();
        List<List> splitAllValues = split.getAllValues();

        for (int i = 0; i < PARALLELISM; i++) {
            Assertions.assertEquals(i, subTaskAllValues.get(i));
            Assertions.assertEquals(
                    allocateFiles(i, PARALLELISM, PARTITION_NUMS), splitAllValues.get(i).size());
        }

        // check no duplicate file assigned
        Assertions.assertEquals(0, dorisSourceSplitEnumerator.currentUnassignedSplitSize());
    }

    private List<PartitionDefinition> buildPartitionDefinitions() {

        List<PartitionDefinition> partitions = new ArrayList<>();

        IntStream.range(0, PARTITION_NUMS)
                .forEach(
                        i -> {
                            PartitionDefinition partitionDefinition =
                                    new PartitionDefinition(
                                            DATABASE,
                                            TABLE,
                                            BE_ADDRESS_PREFIX + i,
                                            new HashSet<>(i),
                                            QUERY_PLAN);

                            partitions.add(partitionDefinition);
                        });

        return partitions;
    }

    /**
     * calculate the number of files assigned each time
     *
     * @param id id
     * @param parallelism parallelism
     * @param fileSize file size
     * @return
     */
    public int allocateFiles(int id, int parallelism, int fileSize) {
        int filesPerIteration = fileSize / parallelism;
        int remainder = fileSize % parallelism;

        if (id < remainder) {
            return filesPerIteration + 1;
        } else {
            return filesPerIteration;
        }
    }
}
