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
package org.apache.seatunnel.connectors.seatunnel.milvus.utils;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.connectors.seatunnel.milvus.catalog.MilvusOptions;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import io.milvus.client.MilvusServiceClient;
import io.milvus.grpc.CollectionSchema;
import io.milvus.grpc.DataType;
import io.milvus.grpc.DescribeCollectionResponse;
import io.milvus.grpc.DescribeIndexResponse;
import io.milvus.grpc.FieldSchema;
import io.milvus.grpc.ShowPartitionsResponse;
import io.milvus.param.R;

import java.util.Collections;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class MilvusConvertUtilsTest {

    @Test
    void getCatalogTableDoesNotSetPartitionNamesWhenOnlyDefaultPartition() {
        ReadonlyConfig config = ReadonlyConfig.fromMap(Collections.emptyMap());
        MilvusConvertUtils utils = new MilvusConvertUtils(config);
        MilvusServiceClient client = mock(MilvusServiceClient.class);

        mockDescribeCollection(client);
        mockDescribeIndex(client);
        mockShowPartitions(
                client, ShowPartitionsResponse.newBuilder().addPartitionNames("_default").build());

        CatalogTable table = utils.getCatalogTable(client, "db", "coll");
        Assertions.assertFalse(table.getOptions().containsKey(MilvusOptions.PARTITION_NAMES));
    }

    @Test
    void getCatalogTableSetsPartitionNamesExcludingDefaultPartition() {
        ReadonlyConfig config = ReadonlyConfig.fromMap(Collections.emptyMap());
        MilvusConvertUtils utils = new MilvusConvertUtils(config);
        MilvusServiceClient client = mock(MilvusServiceClient.class);

        mockDescribeCollection(client);
        mockDescribeIndex(client);
        mockShowPartitions(
                client,
                ShowPartitionsResponse.newBuilder()
                        .addPartitionNames("_default")
                        .addPartitionNames("p1")
                        .addPartitionNames("p2")
                        .build());

        CatalogTable table = utils.getCatalogTable(client, "db", "coll");
        Assertions.assertEquals("p1,p2", table.getOptions().get(MilvusOptions.PARTITION_NAMES));
    }

    private void mockDescribeCollection(MilvusServiceClient client) {
        FieldSchema idField =
                FieldSchema.newBuilder()
                        .setName("id")
                        .setDataType(DataType.Int64)
                        .setIsPrimaryKey(true)
                        .build();
        CollectionSchema schema =
                CollectionSchema.newBuilder()
                        .addFields(idField)
                        .setEnableDynamicField(false)
                        .setDescription("desc")
                        .build();
        DescribeCollectionResponse describeCollectionResponse =
                DescribeCollectionResponse.newBuilder().setSchema(schema).setShardsNum(1).build();

        @SuppressWarnings("unchecked")
        R<DescribeCollectionResponse> response = mock(R.class);
        when(response.getStatus()).thenReturn(R.Status.Success.getCode());
        when(response.getData()).thenReturn(describeCollectionResponse);
        when(client.describeCollection(any())).thenReturn(response);
    }

    private void mockDescribeIndex(MilvusServiceClient client) {
        DescribeIndexResponse describeIndexResponse = DescribeIndexResponse.newBuilder().build();

        @SuppressWarnings("unchecked")
        R<DescribeIndexResponse> response = mock(R.class);
        when(response.getStatus()).thenReturn(R.Status.Success.getCode());
        when(response.getData()).thenReturn(describeIndexResponse);
        when(client.describeIndex(any())).thenReturn(response);
    }

    private void mockShowPartitions(
            MilvusServiceClient client, ShowPartitionsResponse showPartitionsResponse) {
        @SuppressWarnings("unchecked")
        R<ShowPartitionsResponse> response = mock(R.class);
        when(response.getStatus()).thenReturn(R.Status.Success.getCode());
        when(response.getData()).thenReturn(showPartitionsResponse);
        when(client.showPartitions(any())).thenReturn(response);
    }
}
