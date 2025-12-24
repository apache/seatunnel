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

import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.Column;
import org.apache.seatunnel.api.table.type.CommonOptions;

import io.milvus.v2.client.MilvusClientV2;
import io.milvus.v2.service.collection.request.CreateCollectionReq;
import io.milvus.v2.service.collection.request.DescribeCollectionReq;
import io.milvus.v2.service.collection.response.DescribeCollectionResp;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.List;

@Slf4j
public class MilvusConnectorUtils {

    public static Boolean hasPartitionKey(MilvusClientV2 milvusClient, String collectionName) {

        DescribeCollectionResp describeCollectionResp =
                milvusClient.describeCollection(
                        DescribeCollectionReq.builder().collectionName(collectionName).build());
        return describeCollectionResp.getCollectionSchema().getFieldSchemaList().stream()
                .anyMatch(CreateCollectionReq.FieldSchema::getIsPartitionKey);
    }

    public static String getDynamicField(CatalogTable catalogTable) {
        List<Column> columns = catalogTable.getTableSchema().getColumns();
        Column dynamicField = null;
        for (Column column : columns) {
            if (column.getOptions() != null
                    && (Boolean)
                            column.getOptions()
                                    .getOrDefault(CommonOptions.METADATA.getName(), false)) {
                // skip dynamic field
                dynamicField = column;
            }
        }
        return dynamicField == null ? null : dynamicField.getName();
    }

    public static List<String> getJsonField(CatalogTable catalogTable) {
        List<Column> columns = catalogTable.getTableSchema().getColumns();
        List<String> jsonColumn = new ArrayList<>();
        for (Column column : columns) {
            if (column.getOptions() != null
                    && column.getOptions().containsKey(CommonOptions.JSON.getName())
                    && (Boolean) column.getOptions().get(CommonOptions.JSON.getName())) {
                // skip dynamic field
                jsonColumn.add(column.getName());
            }
        }
        return jsonColumn;
    }
}
