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

package org.apache.seatunnel.connectors.seatunnel.milvus.source;

import org.apache.seatunnel.api.common.metrics.MetricsContext;
import org.apache.seatunnel.api.event.EventListener;
import org.apache.seatunnel.api.source.SourceEvent;
import org.apache.seatunnel.api.source.SourceSplitEnumerator;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.TablePath;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import io.milvus.client.MilvusClient;
import io.milvus.grpc.CollectionSchema;
import io.milvus.grpc.DataType;
import io.milvus.grpc.DescribeCollectionResponse;
import io.milvus.grpc.FieldSchema;
import io.milvus.param.R;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class MilvusSourceSplitEnumeratorTest {

    @Test
    public void shouldBalanceSplitsEvenlyAcrossReaders() throws Exception {
        TestingContext context = new TestingContext(4);

        MilvusSourceSplitEnumerator enumerator =
                new MilvusSourceSplitEnumerator(context, null, Collections.emptyMap(), null);

        Method addPendingSplit =
                MilvusSourceSplitEnumerator.class.getDeclaredMethod(
                        "addPendingSplit", java.util.Collection.class);
        addPendingSplit.setAccessible(true);
        addPendingSplit.invoke(enumerator, buildSplits(10));

        MilvusSourceState state = enumerator.snapshotState(1L);
        Map<Integer, List<MilvusSourceSplit>> pendingSplits = state.getPendingSplits();

        Assertions.assertEquals(4, pendingSplits.size());
        Assertions.assertEquals(3, pendingSplits.get(0).size());
        Assertions.assertEquals(3, pendingSplits.get(1).size());
        Assertions.assertEquals(2, pendingSplits.get(2).size());
        Assertions.assertEquals(2, pendingSplits.get(3).size());
    }

    @Test
    public void shouldBalanceSingleSplitCollectionsAcrossReadersInRun() throws Exception {
        TestingContext context = new TestingContext(3);
        Map<TablePath, CatalogTable> tables = new LinkedHashMap<>();
        tables.put(
                TablePath.of("db", null, "collection_0"),
                createCatalogTable(TablePath.of("db", null, "collection_0")));
        tables.put(
                TablePath.of("db", null, "collection_1"),
                createCatalogTable(TablePath.of("db", null, "collection_1")));
        tables.put(
                TablePath.of("db", null, "collection_2"),
                createCatalogTable(TablePath.of("db", null, "collection_2")));

        MilvusSourceSplitEnumerator enumerator =
                new MilvusSourceSplitEnumerator(context, null, tables, null);
        setClient(enumerator, mockSingleSplitMilvusClient());

        enumerator.run();

        Assertions.assertEquals(1, context.getAssignmentSize(0));
        Assertions.assertEquals(1, context.getAssignmentSize(1));
        Assertions.assertEquals(1, context.getAssignmentSize(2));
    }

    @Test
    public void shouldContinueRoundRobinAfterRestore() throws Exception {
        TestingContext context = new TestingContext(3);
        TablePath remainingTable = TablePath.of("db", null, "collection_after_restore");
        Map<TablePath, CatalogTable> tables = new LinkedHashMap<>();
        tables.put(remainingTable, createCatalogTable(remainingTable));

        MilvusSourceState restoredState =
                new MilvusSourceState(
                        Collections.singletonList(remainingTable), new HashMap<>(), 1);
        MilvusSourceSplitEnumerator enumerator =
                new MilvusSourceSplitEnumerator(context, null, tables, restoredState);
        setClient(enumerator, mockSingleSplitMilvusClient());

        enumerator.run();

        Assertions.assertEquals(0, context.getAssignmentSize(0));
        Assertions.assertEquals(1, context.getAssignmentSize(1));
        Assertions.assertEquals(0, context.getAssignmentSize(2));
    }

    private List<MilvusSourceSplit> buildSplits(int size) {
        List<MilvusSourceSplit> splits = new ArrayList<>();
        for (int i = 0; i < size; i++) {
            splits.add(MilvusSourceSplit.builder().splitId("split-" + i).build());
        }
        return splits;
    }

    private CatalogTable createCatalogTable(TablePath tablePath) {
        CatalogTable catalogTable = mock(CatalogTable.class);
        when(catalogTable.getTablePath()).thenReturn(tablePath);
        return catalogTable;
    }

    private MilvusClient mockSingleSplitMilvusClient() {
        MilvusClient client = mock(MilvusClient.class);
        FieldSchema partitionKeyField =
                FieldSchema.newBuilder()
                        .setName("partition_key")
                        .setDataType(DataType.VarChar)
                        .setIsPartitionKey(true)
                        .build();
        CollectionSchema schema =
                CollectionSchema.newBuilder().addFields(partitionKeyField).build();
        DescribeCollectionResponse response =
                DescribeCollectionResponse.newBuilder().setSchema(schema).build();

        @SuppressWarnings("unchecked")
        R<DescribeCollectionResponse> describeResponse = mock(R.class);
        when(describeResponse.getData()).thenReturn(response);
        when(client.describeCollection(any())).thenReturn(describeResponse);
        return client;
    }

    private void setClient(MilvusSourceSplitEnumerator enumerator, MilvusClient client)
            throws Exception {
        Field clientField = MilvusSourceSplitEnumerator.class.getDeclaredField("client");
        clientField.setAccessible(true);
        clientField.set(enumerator, client);
    }

    private static class TestingContext
            implements SourceSplitEnumerator.Context<MilvusSourceSplit> {
        private final int parallelism;
        private final Map<Integer, List<MilvusSourceSplit>> assignments = new HashMap<>();
        private final Set<Integer> readers;

        private TestingContext(int parallelism) {
            this.parallelism = parallelism;
            this.readers = new LinkedHashSet<>();
            for (int i = 0; i < parallelism; i++) {
                readers.add(i);
            }
        }

        @Override
        public int currentParallelism() {
            return parallelism;
        }

        @Override
        public Set<Integer> registeredReaders() {
            return readers;
        }

        @Override
        public void assignSplit(int subtaskId, List<MilvusSourceSplit> splits) {
            assignments.computeIfAbsent(subtaskId, ignored -> new ArrayList<>()).addAll(splits);
        }

        @Override
        public void signalNoMoreSplits(int subtask) {}

        @Override
        public void sendEventToSourceReader(int subtaskId, SourceEvent event) {}

        @Override
        public MetricsContext getMetricsContext() {
            return null;
        }

        @Override
        public EventListener getEventListener() {
            return null;
        }

        private int getAssignmentSize(int subtaskId) {
            return assignments.getOrDefault(subtaskId, Collections.emptyList()).size();
        }
    }
}
