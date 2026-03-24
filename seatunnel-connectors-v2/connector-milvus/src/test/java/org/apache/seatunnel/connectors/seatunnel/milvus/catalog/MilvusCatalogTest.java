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
package org.apache.seatunnel.connectors.seatunnel.milvus.catalog;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.table.catalog.TablePath;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import io.milvus.client.MilvusServiceClient;
import io.milvus.grpc.ShowPartitionsResponse;
import io.milvus.param.R;
import io.milvus.param.RpcStatus;
import io.milvus.param.partition.CreatePartitionParam;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.Collections;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class MilvusCatalogTest {

    @Test
    void createPartitionInternalSkipsEmptyString() throws Exception {
        MilvusCatalog catalog = createCatalogWithClient(mockClientWithDefaultPartitions());
        invokeCreatePartitionInternal(catalog, "", TablePath.of("db", null, "coll"));
        verify(getClient(catalog), never()).createPartition(any());
    }

    @Test
    void createPartitionInternalSkipsOnlyCommas() throws Exception {
        MilvusCatalog catalog = createCatalogWithClient(mockClientWithDefaultPartitions());
        invokeCreatePartitionInternal(catalog, ",,,", TablePath.of("db", null, "coll"));
        verify(getClient(catalog), never()).createPartition(any());
    }

    @Test
    void createPartitionInternalSkipsSpaces() throws Exception {
        MilvusCatalog catalog = createCatalogWithClient(mockClientWithDefaultPartitions());
        invokeCreatePartitionInternal(catalog, "   ", TablePath.of("db", null, "coll"));
        verify(getClient(catalog), never()).createPartition(any());
    }

    @Test
    void createPartitionInternalSkipsDefaultPartitionName() throws Exception {
        MilvusServiceClient client = mockClientWithDefaultPartitions();
        R<RpcStatus> successRpcStatusR = mock(R.class);
        when(successRpcStatusR.getStatus()).thenReturn(R.Status.Success.getCode());
        when(successRpcStatusR.getMessage()).thenReturn("OK");
        when(client.createPartition(any()))
                .thenAnswer(
                        invocation -> {
                            CreatePartitionParam param = invocation.getArgument(0);
                            String partitionName = extractPartitionName(param);
                            if (partitionName == null
                                    || partitionName.trim().isEmpty()
                                    || "_default".equals(partitionName)) {
                                throw new RuntimeException(
                                        "invalid partitionName: " + partitionName);
                            }
                            return successRpcStatusR;
                        });

        MilvusCatalog catalog = createCatalogWithClient(client);
        invokeCreatePartitionInternal(catalog, "_default, p1", TablePath.of("db", null, "coll"));

        verify(client, times(1)).createPartition(any());
    }

    private MilvusCatalog createCatalogWithClient(MilvusServiceClient client) throws Exception {
        MilvusCatalog catalog =
                new MilvusCatalog("milvus", ReadonlyConfig.fromMap(Collections.emptyMap()));
        Field clientField = MilvusCatalog.class.getDeclaredField("client");
        clientField.setAccessible(true);
        clientField.set(catalog, client);
        return catalog;
    }

    private MilvusServiceClient mockClientWithDefaultPartitions() {
        MilvusServiceClient client = mock(MilvusServiceClient.class);
        @SuppressWarnings("unchecked")
        R<ShowPartitionsResponse> showPartitionsR = mock(R.class);
        when(showPartitionsR.getStatus()).thenReturn(R.Status.Success.getCode());
        when(showPartitionsR.getData())
                .thenReturn(
                        ShowPartitionsResponse.newBuilder().addPartitionNames("_default").build());
        when(showPartitionsR.getMessage()).thenReturn("OK");
        when(client.showPartitions(any())).thenReturn(showPartitionsR);

        @SuppressWarnings("unchecked")
        R<RpcStatus> createPartitionR = mock(R.class);
        when(createPartitionR.getStatus()).thenReturn(R.Status.Success.getCode());
        when(createPartitionR.getMessage()).thenReturn("OK");
        when(client.createPartition(any())).thenReturn(createPartitionR);
        return client;
    }

    private void invokeCreatePartitionInternal(
            MilvusCatalog catalog, String partitionNames, TablePath tablePath) throws Exception {
        Method method =
                MilvusCatalog.class.getDeclaredMethod(
                        "createPartitionInternal", String.class, TablePath.class);
        method.setAccessible(true);
        Assertions.assertDoesNotThrow(() -> method.invoke(catalog, partitionNames, tablePath));
    }

    private MilvusServiceClient getClient(MilvusCatalog catalog) throws Exception {
        Field clientField = MilvusCatalog.class.getDeclaredField("client");
        clientField.setAccessible(true);
        return (MilvusServiceClient) clientField.get(catalog);
    }

    private String extractPartitionName(CreatePartitionParam param) {
        try {
            Method getter = param.getClass().getMethod("getPartitionName");
            Object v = getter.invoke(param);
            return v == null ? null : v.toString();
        } catch (Exception ignored) {
        }
        try {
            Field f = param.getClass().getDeclaredField("partitionName");
            f.setAccessible(true);
            Object v = f.get(param);
            return v == null ? null : v.toString();
        } catch (Exception ignored) {
        }
        return null;
    }
}
