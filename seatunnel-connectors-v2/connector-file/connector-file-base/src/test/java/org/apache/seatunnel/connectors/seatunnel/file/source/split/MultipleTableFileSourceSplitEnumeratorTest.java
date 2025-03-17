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

package org.apache.seatunnel.connectors.seatunnel.file.source.split;

import org.apache.seatunnel.shade.com.google.common.collect.Lists;
import org.apache.seatunnel.shade.com.google.common.collect.Maps;

import org.apache.seatunnel.api.source.SourceSplitEnumerator;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.TableIdentifier;
import org.apache.seatunnel.connectors.seatunnel.file.config.BaseFileSourceConfig;
import org.apache.seatunnel.connectors.seatunnel.file.config.BaseMultipleTableFileSourceConfig;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.IntStream;

@Slf4j
public class MultipleTableFileSourceSplitEnumeratorTest {

    @Test
    void assignSplitTest() {
        int parallelism = 4;
        int fileSize = 50;

        Map<String, List<String>> filePathMap = new HashMap<>();
        List<String> filePaths = new ArrayList<>();
        IntStream.range(0, fileSize).forEach(i -> filePaths.add("filePath" + i));
        filePathMap.put("table1", filePaths);

        BaseFileSourceConfig baseFileSourceConfig = Mockito.mock(BaseFileSourceConfig.class);

        Mockito.when(baseFileSourceConfig.getFilePaths()).thenReturn(filePaths);

        CatalogTable catalogTable =
                CatalogTable.of(
                        TableIdentifier.of("catalog", "test", "hive_table1"),
                        null,
                        Maps.newHashMap(),
                        Lists.newArrayList(),
                        null);
        Mockito.when(baseFileSourceConfig.getCatalogTable()).thenReturn(catalogTable);

        BaseMultipleTableFileSourceConfig baseMultipleTableFileSourceConfig =
                Mockito.mock(BaseMultipleTableFileSourceConfig.class);

        Mockito.when(baseMultipleTableFileSourceConfig.getFileSourceConfigs())
                .thenReturn(Arrays.asList(baseFileSourceConfig));

        SourceSplitEnumerator.Context<FileSourceSplit> context =
                Mockito.mock(SourceSplitEnumerator.Context.class);

        Mockito.when(context.currentParallelism()).thenReturn(parallelism);
        MultipleTableFileSourceSplitEnumerator enumerator =
                new MultipleTableFileSourceSplitEnumerator(
                        context, baseMultipleTableFileSourceConfig);

        AtomicInteger unAssignedSplitSize = new AtomicInteger(fileSize);
        IntStream.range(0, parallelism)
                .forEach(
                        id -> {
                            enumerator.registerReader(id);

                            // check the number of files assigned each time
                            Assertions.assertEquals(
                                    enumerator.currentUnassignedSplitSize(),
                                    unAssignedSplitSize.get()
                                            - allocateFiles(id, parallelism, fileSize));
                            unAssignedSplitSize.set(enumerator.currentUnassignedSplitSize());

                            log.info(
                                    "unAssigned splits => {}, allocate files => {}",
                                    enumerator.currentUnassignedSplitSize(),
                                    allocateFiles(id, parallelism, fileSize));
                        });

        // check no duplicate file assigned
        Assertions.assertEquals(0, enumerator.currentUnassignedSplitSize());
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
