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
import org.apache.seatunnel.api.table.catalog.Column;
import org.apache.seatunnel.api.table.catalog.ConstraintKey;
import org.apache.seatunnel.api.table.catalog.PhysicalColumn;
import org.apache.seatunnel.api.table.catalog.PrimaryKey;
import org.apache.seatunnel.api.table.catalog.TableIdentifier;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.api.table.catalog.VectorIndex;
import org.apache.seatunnel.common.constants.CommonOptions;
import org.apache.seatunnel.connectors.seatunnel.milvus.catalog.MilvusOptions;
import org.apache.seatunnel.connectors.seatunnel.milvus.config.MilvusSourceConfig;
import org.apache.seatunnel.connectors.seatunnel.milvus.exception.MilvusConnectionErrorCode;
import org.apache.seatunnel.connectors.seatunnel.milvus.exception.MilvusConnectorException;
import org.apache.seatunnel.connectors.seatunnel.milvus.utils.source.MilvusSourceConverter;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.util.Lists;

import com.google.protobuf.ProtocolStringList;
import io.milvus.client.MilvusServiceClient;
import io.milvus.grpc.CollectionSchema;
import io.milvus.grpc.DescribeCollectionResponse;
import io.milvus.grpc.DescribeIndexResponse;
import io.milvus.grpc.FieldSchema;
import io.milvus.grpc.IndexDescription;
import io.milvus.grpc.KeyValuePair;
import io.milvus.grpc.ShowCollectionsResponse;
import io.milvus.grpc.ShowPartitionsResponse;
import io.milvus.grpc.ShowType;
import io.milvus.param.ConnectParam;
import io.milvus.param.R;
import io.milvus.param.collection.DescribeCollectionParam;
import io.milvus.param.collection.ShowCollectionsParam;
import io.milvus.param.index.DescribeIndexParam;
import io.milvus.param.partition.ShowPartitionsParam;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.apache.seatunnel.api.table.type.BasicType.STRING_TYPE;

@Slf4j
public class MilvusConvertUtils {
    private final ReadonlyConfig config;

    public MilvusConvertUtils(ReadonlyConfig config) {
        this.config = config;
    }

    public Map<TablePath, CatalogTable> getSourceTables() {
        MilvusServiceClient client =
                new MilvusServiceClient(
                        ConnectParam.newBuilder()
                                .withUri(config.get(MilvusSourceConfig.URL))
                                .withToken(config.get(MilvusSourceConfig.TOKEN))
                                .build());

        String database = config.get(MilvusSourceConfig.DATABASE);
        List<String> collectionList = new ArrayList<>();
        if (StringUtils.isNotEmpty(config.get(MilvusSourceConfig.COLLECTION))) {
            collectionList.add(config.get(MilvusSourceConfig.COLLECTION));
        } else {
            R<ShowCollectionsResponse> response =
                    client.showCollections(
                            ShowCollectionsParam.newBuilder()
                                    .withDatabaseName(database)
                                    .withShowType(ShowType.All)
                                    .build());
            if (response.getStatus() != R.Status.Success.getCode()) {
                throw new MilvusConnectorException(
                        MilvusConnectionErrorCode.SHOW_COLLECTIONS_ERROR);
            }

            ProtocolStringList collections = response.getData().getCollectionNamesList();
            if (CollectionUtils.isEmpty(collections)) {
                throw new MilvusConnectorException(
                        MilvusConnectionErrorCode.DATABASE_NO_COLLECTIONS, database);
            }
            collectionList.addAll(collections);
        }

        Map<TablePath, CatalogTable> map = new HashMap<>();
        for (String collection : collectionList) {
            CatalogTable catalogTable = getCatalogTable(client, database, collection);
            TablePath tablePath = TablePath.of(database, null, collection);
            map.put(tablePath, catalogTable);
        }
        client.close();
        return map;
    }

    public CatalogTable getCatalogTable(
            MilvusServiceClient client, String database, String collection) {
        R<DescribeCollectionResponse> response =
                client.describeCollection(
                        DescribeCollectionParam.newBuilder()
                                .withDatabaseName(database)
                                .withCollectionName(collection)
                                .build());

        if (response.getStatus() != R.Status.Success.getCode()) {
            throw new MilvusConnectorException(
                    MilvusConnectionErrorCode.DESC_COLLECTION_ERROR, response.getMessage());
        }
        log.info(
                "describe collection database: {}, collection: {}, response: {}",
                database,
                collection,
                response);
        // collection column
        DescribeCollectionResponse collectionResponse = response.getData();
        CollectionSchema schema = collectionResponse.getSchema();
        List<Column> columns = new ArrayList<>();
        boolean existPartitionKeyField = false;
        String partitionKeyField = null;
        for (FieldSchema fieldSchema : schema.getFieldsList()) {
            PhysicalColumn physicalColumn = MilvusSourceConverter.convertColumn(fieldSchema);
            columns.add(physicalColumn);
            if (fieldSchema.getIsPartitionKey()) {
                existPartitionKeyField = true;
                partitionKeyField = fieldSchema.getName();
            }
        }
        if (collectionResponse.getSchema().getEnableDynamicField()) {
            Map<String, Object> options = new HashMap<>();

            options.put(CommonOptions.METADATA.getName(), true);
            PhysicalColumn dynamicColumn =
                    PhysicalColumn.builder()
                            .name(CommonOptions.METADATA.getName())
                            .dataType(STRING_TYPE)
                            .options(options)
                            .build();
            columns.add(dynamicColumn);
        }

        // primary key
        PrimaryKey primaryKey = buildPrimaryKey(schema.getFieldsList());

        // index
        R<DescribeIndexResponse> describeIndexResponseR =
                client.describeIndex(
                        DescribeIndexParam.newBuilder()
                                .withDatabaseName(database)
                                .withCollectionName(collection)
                                .build());
        if (describeIndexResponseR.getStatus() != R.Status.Success.getCode()) {
            throw new MilvusConnectorException(MilvusConnectionErrorCode.DESC_INDEX_ERROR);
        }
        DescribeIndexResponse indexResponse = describeIndexResponseR.getData();
        List<ConstraintKey.ConstraintKeyColumn> vectorIndexes = buildVectorIndexes(indexResponse);

        // build tableSchema
        TableSchema tableSchema =
                TableSchema.builder()
                        .columns(columns)
                        .primaryKey(primaryKey)
                        .constraintKey(
                                ConstraintKey.of(
                                        ConstraintKey.ConstraintType.VECTOR_INDEX_KEY,
                                        "vector_index",
                                        vectorIndexes))
                        .build();

        // build tableId
        String CATALOG_NAME = "Milvus";
        TableIdentifier tableId = TableIdentifier.of(CATALOG_NAME, database, null, collection);
        // build options info
        Map<String, String> options = new HashMap<>();
        options.put(
                MilvusOptions.ENABLE_DYNAMIC_FIELD, String.valueOf(schema.getEnableDynamicField()));
        options.put(MilvusOptions.SHARDS_NUM, String.valueOf(collectionResponse.getShardsNum()));
        if (existPartitionKeyField) {
            options.put(MilvusOptions.PARTITION_KEY_FIELD, partitionKeyField);
        } else {
            fillPartitionNames(options, client, database, collection);
        }

        return CatalogTable.of(
                tableId, tableSchema, options, new ArrayList<>(), schema.getDescription());
    }

    private static void fillPartitionNames(
            Map<String, String> options,
            MilvusServiceClient client,
            String database,
            String collection) {
        // not exist partition key, will read partition
        R<ShowPartitionsResponse> partitionsResponseR =
                client.showPartitions(
                        ShowPartitionsParam.newBuilder()
                                .withDatabaseName(database)
                                .withCollectionName(collection)
                                .build());
        if (partitionsResponseR.getStatus() != R.Status.Success.getCode()) {
            throw new MilvusConnectorException(
                    MilvusConnectionErrorCode.SHOW_PARTITION_ERROR,
                    partitionsResponseR.getMessage());
        }

        ProtocolStringList partitionNamesList =
                partitionsResponseR.getData().getPartitionNamesList();
        List<String> list = new ArrayList<>();
        for (String partition : partitionNamesList) {
            if (partition.equals("_default")) {
                continue;
            }
            list.add(partition);
        }
        if (CollectionUtils.isEmpty(partitionNamesList)) {
            return;
        }

        options.put(MilvusOptions.PARTITION_NAMES, String.join(",", list));
    }

    private static List<ConstraintKey.ConstraintKeyColumn> buildVectorIndexes(
            DescribeIndexResponse indexResponse) {
        if (CollectionUtils.isEmpty(indexResponse.getIndexDescriptionsList())) {
            return null;
        }

        List<ConstraintKey.ConstraintKeyColumn> list = new ArrayList<>();
        for (IndexDescription per : indexResponse.getIndexDescriptionsList()) {
            Map<String, String> paramsMap =
                    per.getParamsList().stream()
                            .collect(
                                    Collectors.toMap(KeyValuePair::getKey, KeyValuePair::getValue));

            VectorIndex index =
                    new VectorIndex(
                            per.getIndexName(),
                            per.getFieldName(),
                            paramsMap.get("index_type"),
                            paramsMap.get("metric_type"));

            list.add(index);
        }

        return list;
    }

    public static PrimaryKey buildPrimaryKey(List<FieldSchema> fields) {
        for (FieldSchema field : fields) {
            if (field.getIsPrimaryKey()) {
                return PrimaryKey.of(
                        field.getName(), Lists.newArrayList(field.getName()), field.getAutoID());
            }
        }

        return null;
    }
}
