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

package org.apache.seatunnel.translation.source;

import org.apache.seatunnel.shade.com.google.common.collect.Maps;

import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.connectors.doris.config.DorisSourceConfig;
import org.apache.seatunnel.connectors.doris.rest.PartitionDefinition;
import org.apache.seatunnel.connectors.doris.rest.RestService;
import org.apache.seatunnel.connectors.doris.source.DorisSource;
import org.apache.seatunnel.connectors.doris.source.DorisSourceTable;
import org.apache.seatunnel.connectors.doris.source.reader.DorisSourceReader;
import org.apache.seatunnel.connectors.doris.source.split.DorisSourceSplit;
import org.apache.seatunnel.connectors.seatunnel.file.source.BaseFileSource;
import org.apache.seatunnel.connectors.seatunnel.file.source.split.FileSourceSplit;
import org.apache.seatunnel.connectors.seatunnel.file.source.split.FileSourceSplitEnumerator;

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
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.mockito.ArgumentMatchers.any;

@Slf4j
public class ParallelSourceTest {

    @Test
    void fileParallelSourceSplitEnumeratorTest() throws Exception {
        int fileSize = 15;
        int parallelism = 4;

        List<String> filePaths = new ArrayList<>();
        for (int i = 0; i < fileSize; i++) {
            filePaths.add("file" + i + ".txt");
        }
        BaseFileSource baseFileSource = Mockito.spy(BaseFileSource.class);

        Set<FileSourceSplit> splitSet = new HashSet<>();
        for (int i = 0; i < parallelism; i++) {

            ParallelEnumeratorContext<FileSourceSplit> context =
                    Mockito.mock(ParallelEnumeratorContext.class);

            Mockito.when(context.currentParallelism()).thenReturn(parallelism);

            FileSourceSplitEnumerator fileSourceSplitEnumerator =
                    new FileSourceSplitEnumerator(context, filePaths);

            Mockito.when(baseFileSource.createEnumerator(any()))
                    .thenReturn(fileSourceSplitEnumerator);

            ParallelSource parallelSource =
                    new ParallelSource(
                            baseFileSource, null, parallelism, "parallel-source-test" + i, i);

            parallelSource.open();
            parallelSource.splitEnumerator.run();

            ArgumentCaptor<Integer> subtaskId = ArgumentCaptor.forClass(Integer.class);
            ArgumentCaptor<List> split = ArgumentCaptor.forClass(List.class);

            Mockito.verify(context, Mockito.times(parallelism))
                    .assignSplit(subtaskId.capture(), split.capture());

            List<Integer> subTaskAllValues = subtaskId.getAllValues();
            List<List> splitAllValues = split.getAllValues();

            Assertions.assertEquals(i, subTaskAllValues.get(i));
            Assertions.assertEquals(
                    allocateFiles(i, parallelism, fileSize), splitAllValues.get(i).size());

            splitSet.addAll(splitAllValues.get(i));
        }

        // Check that there are no duplicate file assign
        Assertions.assertEquals(splitSet.size(), fileSize);
    }

    @Test
    public void dorisParallelSourceSplitEnumeratorTest() throws Exception {
        int parallelism = 4;
        int partitionNums = 30;

        DorisSourceConfig dorisSourceConfig = Mockito.mock(DorisSourceConfig.class);
        DorisSourceTable dorisSourceTable = Mockito.mock(DorisSourceTable.class);

        Map<TablePath, DorisSourceTable> dorisSourceTableMap = Maps.newHashMap();
        dorisSourceTableMap.put(new TablePath("default", null, "default_table"), dorisSourceTable);

        DorisSource dorisSource = new DorisSource(dorisSourceConfig, dorisSourceTableMap);

        MockedStatic<RestService> restServiceMockedStatic = Mockito.mockStatic(RestService.class);
        restServiceMockedStatic
                .when(() -> RestService.findPartitions(any(), any(), any()))
                .thenReturn(buildPartitionDefinitions(partitionNums));

        Set<DorisSourceSplit> splitSet = new HashSet<>();
        for (int i = 0; i < parallelism; i++) {
            ParallelSource parallelSource =
                    new ParallelSource(
                            dorisSource, null, parallelism, "parallel-doris-source" + i, i);
            parallelSource.open();

            // execute file allocation process
            parallelSource.splitEnumerator.run();
            List<DorisSourceSplit> sourceSplits =
                    ((DorisSourceReader) parallelSource.reader).snapshotState(0);
            log.info(
                    "parallel source{} splits => {}",
                    i + 1,
                    sourceSplits.stream()
                            .map(DorisSourceSplit::splitId)
                            .collect(Collectors.toList()));

            Assertions.assertEquals(
                    allocateFiles(i, parallelism, partitionNums), sourceSplits.size());

            // collect all splits
            splitSet.addAll(sourceSplits);
        }

        Assertions.assertEquals(splitSet.size(), partitionNums);
    }

    private List<PartitionDefinition> buildPartitionDefinitions(int partitionNUms) {

        List<PartitionDefinition> partitions = new ArrayList<>();

        String beAddressPrefix = "doris-be-";

        IntStream.range(0, partitionNUms)
                .forEach(
                        i -> {
                            PartitionDefinition partitionDefinition =
                                    new PartitionDefinition(
                                            "default",
                                            "default_table",
                                            beAddressPrefix + i,
                                            new HashSet<>(i),
                                            "QUERY_PLAN");

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
