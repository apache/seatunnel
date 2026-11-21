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
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.common.utils.ReflectionUtils;
import org.apache.seatunnel.connectors.doris.backend.BackendClient;
import org.apache.seatunnel.connectors.doris.config.DorisSourceConfig;
import org.apache.seatunnel.connectors.doris.exception.DorisConnectorErrorCode;
import org.apache.seatunnel.connectors.doris.exception.DorisConnectorException;
import org.apache.seatunnel.connectors.doris.rest.PartitionDefinition;
import org.apache.seatunnel.connectors.doris.rest.RestService;
import org.apache.seatunnel.connectors.doris.source.DorisSourceTable;
import org.apache.seatunnel.connectors.doris.source.reader.DorisValueReader;
import org.apache.seatunnel.connectors.doris.source.split.DorisSourceSplit;
import org.apache.seatunnel.connectors.doris.source.split.DorisSourceSplitEnumerator;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import lombok.extern.slf4j.Slf4j;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantLock;
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

    /**
     * The asynchronous fetch thread cannot throw to the reader, so it records the failure and
     * {@code hasNext()} must report it instead of returning an end of stream. Before the fix the
     * reader polled forever in this case.
     */
    @Test
    public void dorisValueReaderReportsAsyncFetchFailure() {
        BackendClient client = Mockito.mock(BackendClient.class);
        Mockito.when(client.getNext(Mockito.any()))
                .thenThrow(
                        new DorisConnectorException(
                                DorisConnectorErrorCode.BACKEND_CLIENT_FAILED,
                                "simulated backend failure"));
        DorisValueReader reader = newAsyncValueReader(client);

        // The asynchronous fetch fails while reading the first batch.
        ReflectionUtils.invoke(reader, "asyncFetchBatches");

        DorisConnectorException exception =
                Assertions.assertThrows(
                        DorisConnectorException.class,
                        () ->
                                Assertions.assertTimeoutPreemptively(
                                        Duration.ofSeconds(10), reader::hasNext));
        Assertions.assertTrue(
                String.valueOf(exception.getMessage()).contains("simulated backend failure"),
                "the backend failure should be reported: " + exception);
    }

    /**
     * {@code close()} must stop the asynchronous fetch, otherwise a reader waiting for its first
     * batch keeps polling and {@code hasNext()} never returns.
     */
    @Test
    public void dorisValueReaderCloseStopsAsyncFetch() {
        BackendClient client = Mockito.mock(BackendClient.class);
        DorisValueReader reader = newAsyncValueReader(client);
        Thread asyncThread = Mockito.mock(Thread.class);
        ReflectionUtils.setField(reader, DorisValueReader.class, "asyncThread", asyncThread);

        reader.close();

        Mockito.verify(asyncThread).interrupt();
        Mockito.verify(client).closeScanner(Mockito.any());
        Assertions.assertFalse(
                Assertions.assertTimeoutPreemptively(Duration.ofSeconds(10), reader::hasNext));
    }

    /**
     * Builds a reader with the asynchronous read path enabled and a mocked backend, without running
     * the constructor, which opens a real scan on a Doris backend.
     */
    private DorisValueReader newAsyncValueReader(BackendClient client) {
        DorisValueReader reader = Mockito.mock(DorisValueReader.class, Mockito.CALLS_REAL_METHODS);
        ReflectionUtils.setField(reader, DorisValueReader.class, "client", client);
        ReflectionUtils.setField(reader, DorisValueReader.class, "clientLock", new ReentrantLock());
        ReflectionUtils.setField(reader, DorisValueReader.class, "eos", new AtomicBoolean(false));
        ReflectionUtils.setField(
                reader,
                DorisValueReader.class,
                "rowBatchBlockingQueue",
                new ArrayBlockingQueue<>(2));
        ReflectionUtils.setField(
                reader,
                DorisValueReader.class,
                "seaTunnelRowType",
                new SeaTunnelRowType(
                        new String[] {"id"}, new SeaTunnelDataType<?>[] {BasicType.LONG_TYPE}));
        ReflectionUtils.setField(
                reader, DorisValueReader.class, "deserializeArrowToRowBatchAsync", true);
        ReflectionUtils.setField(reader, DorisValueReader.class, "asyncThreadStarted", true);
        ReflectionUtils.setField(reader, DorisValueReader.class, "contextId", "test-context");
        return reader;
    }
}
