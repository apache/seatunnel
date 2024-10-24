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
import org.apache.seatunnel.api.table.catalog.Catalog;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.Column;
import org.apache.seatunnel.api.table.catalog.ConstraintKey;
import org.apache.seatunnel.api.table.catalog.InfoPreviewResult;
import org.apache.seatunnel.api.table.catalog.PreviewResult;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.api.table.catalog.VectorIndex;
import org.apache.seatunnel.api.table.catalog.exception.CatalogException;
import org.apache.seatunnel.api.table.catalog.exception.DatabaseAlreadyExistException;
import org.apache.seatunnel.api.table.catalog.exception.DatabaseNotExistException;
import org.apache.seatunnel.api.table.catalog.exception.TableAlreadyExistException;
import org.apache.seatunnel.api.table.catalog.exception.TableNotExistException;
import org.apache.seatunnel.common.constants.CommonOptions;
import org.apache.seatunnel.connectors.seatunnel.milvus.config.MilvusSinkConfig;
import org.apache.seatunnel.connectors.seatunnel.milvus.exception.MilvusConnectionErrorCode;
import org.apache.seatunnel.connectors.seatunnel.milvus.exception.MilvusConnectorException;
import org.apache.seatunnel.connectors.seatunnel.milvus.utils.sink.MilvusSinkConverter;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;

import com.google.protobuf.ProtocolStringList;
import io.milvus.client.MilvusServiceClient;
import io.milvus.common.clientenum.ConsistencyLevelEnum;
import io.milvus.grpc.ListDatabasesResponse;
import io.milvus.grpc.ShowCollectionsResponse;
import io.milvus.grpc.ShowPartitionsResponse;
import io.milvus.grpc.ShowType;
import io.milvus.param.ConnectParam;
import io.milvus.param.IndexType;
import io.milvus.param.MetricType;
import io.milvus.param.R;
import io.milvus.param.RpcStatus;
import io.milvus.param.collection.CreateCollectionParam;
import io.milvus.param.collection.CreateDatabaseParam;
import io.milvus.param.collection.DropCollectionParam;
import io.milvus.param.collection.DropDatabaseParam;
import io.milvus.param.collection.FieldType;
import io.milvus.param.collection.HasCollectionParam;
import io.milvus.param.collection.ShowCollectionsParam;
import io.milvus.param.index.CreateIndexParam;
import io.milvus.param.partition.CreatePartitionParam;
import io.milvus.param.partition.ShowPartitionsParam;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkNotNull;
import static org.apache.seatunnel.connectors.seatunnel.milvus.config.MilvusSinkConfig.CREATE_INDEX;

@Slf4j
public class MilvusCatalog implements Catalog {

    private final String catalogName;
    private final ReadonlyConfig config;

    private MilvusServiceClient client;

    public MilvusCatalog(String catalogName, ReadonlyConfig config) {
        this.catalogName = catalogName;
        this.config = config;
    }

    @Override
    public void open() throws CatalogException {
        ConnectParam connectParam =
                ConnectParam.newBuilder()
                        .withUri(config.get(MilvusSinkConfig.URL))
                        .withToken(config.get(MilvusSinkConfig.TOKEN))
                        .build();
        try {
            this.client = new MilvusServiceClient(connectParam);
        } catch (Exception e) {
            throw new CatalogException(String.format("Failed to open catalog %s", catalogName), e);
        }
    }

    @Override
    public void close() throws CatalogException {
        this.client.close();
    }

    @Override
    public String name() {
        return catalogName;
    }

    @Override
    public PreviewResult previewAction(
            ActionType actionType, TablePath tablePath, Optional<CatalogTable> catalogTable) {
        if (actionType == ActionType.CREATE_TABLE) {
            return new InfoPreviewResult("create collection " + tablePath.getTableName());
        } else if (actionType == ActionType.DROP_TABLE) {
            return new InfoPreviewResult("drop collection " + tablePath.getTableName());
        } else if (actionType == ActionType.CREATE_DATABASE) {
            return new InfoPreviewResult("create database " + tablePath.getDatabaseName());
        } else if (actionType == ActionType.DROP_DATABASE) {
            return new InfoPreviewResult("drop database " + tablePath.getDatabaseName());
        } else {
            throw new UnsupportedOperationException("Unsupported action type: " + actionType);
        }
    }

    @Override
    public String getDefaultDatabase() throws CatalogException {
        return "default";
    }

    @Override
    public boolean databaseExists(String databaseName) throws CatalogException {
        List<String> databases = this.listDatabases();
        return databases.contains(databaseName);
    }

    @Override
    public List<String> listDatabases() throws CatalogException {
        R<ListDatabasesResponse> response = this.client.listDatabases();
        return response.getData().getDbNamesList();
    }

    @Override
    public List<String> listTables(String databaseName)
            throws CatalogException, DatabaseNotExistException {
        R<ShowCollectionsResponse> response =
                this.client.showCollections(
                        ShowCollectionsParam.newBuilder()
                                .withDatabaseName(databaseName)
                                .withShowType(ShowType.All)
                                .build());

        return response.getData().getCollectionNamesList();
    }

    @Override
    public boolean tableExists(TablePath tablePath) throws CatalogException {
        R<Boolean> response =
                this.client.hasCollection(
                        HasCollectionParam.newBuilder()
                                .withDatabaseName(tablePath.getDatabaseName())
                                .withCollectionName(tablePath.getTableName())
                                .build());
        if (response.getData() != null) {
            return response.getData();
        }
        throw new MilvusConnectorException(
                MilvusConnectionErrorCode.SERVER_RESPONSE_FAILED,
                response.getMessage(),
                response.getException());
    }

    @Override
    public CatalogTable getTable(TablePath tablePath)
            throws CatalogException, TableNotExistException {
        throw new RuntimeException("not implemented");
    }

    @Override
    public void createTable(TablePath tablePath, CatalogTable catalogTable, boolean ignoreIfExists)
            throws TableAlreadyExistException, DatabaseNotExistException, CatalogException {
        checkNotNull(tablePath, "Table path cannot be null");
        if (!databaseExists(tablePath.getDatabaseName())) {
            throw new DatabaseNotExistException(catalogName, tablePath.getDatabaseName());
        }
        if (tableExists(tablePath)) {
            if (ignoreIfExists) {
                return;
            }
            throw new TableAlreadyExistException(catalogName, tablePath);
        }

        checkNotNull(catalogTable, "catalogTable must not be null");
        TableSchema tableSchema = catalogTable.getTableSchema();
        checkNotNull(tableSchema, "tableSchema must not be null");
        createTableInternal(tablePath, catalogTable);

        if (CollectionUtils.isNotEmpty(tableSchema.getConstraintKeys())
                && config.get(CREATE_INDEX)) {
            for (ConstraintKey constraintKey : tableSchema.getConstraintKeys()) {
                if (constraintKey
                        .getConstraintType()
                        .equals(ConstraintKey.ConstraintType.VECTOR_INDEX_KEY)) {
                    createIndexInternal(tablePath, constraintKey.getColumnNames());
                }
            }
        }
    }

    private void createIndexInternal(
            TablePath tablePath, List<ConstraintKey.ConstraintKeyColumn> vectorIndexes) {
        for (ConstraintKey.ConstraintKeyColumn column : vectorIndexes) {
            VectorIndex index = (VectorIndex) column;
            CreateIndexParam createIndexParam =
                    CreateIndexParam.newBuilder()
                            .withDatabaseName(tablePath.getDatabaseName())
                            .withCollectionName(tablePath.getTableName())
                            .withFieldName(index.getColumnName())
                            .withIndexName(index.getIndexName())
                            .withIndexType(IndexType.valueOf(index.getIndexType().name()))
                            .withMetricType(MetricType.valueOf(index.getMetricType().name()))
                            .build();

            R<RpcStatus> response = client.createIndex(createIndexParam);
            if (!Objects.equals(response.getStatus(), R.success().getStatus())) {
                throw new MilvusConnectorException(
                        MilvusConnectionErrorCode.CREATE_INDEX_ERROR, response.getMessage());
            }
        }
    }

    public void createTableInternal(TablePath tablePath, CatalogTable catalogTable) {
        try {
            Map<String, String> options = catalogTable.getOptions();

            // partition key logic
            boolean existPartitionKeyField = options.containsKey(MilvusOptions.PARTITION_KEY_FIELD);
            String partitionKeyField =
                    existPartitionKeyField ? options.get(MilvusOptions.PARTITION_KEY_FIELD) : null;
            // if options set, will overwrite aut read
            if (StringUtils.isNotEmpty(config.get(MilvusSinkConfig.PARTITION_KEY))) {
                existPartitionKeyField = true;
                partitionKeyField = config.get(MilvusSinkConfig.PARTITION_KEY);
            }

            TableSchema tableSchema = catalogTable.getTableSchema();
            List<FieldType> fieldTypes = new ArrayList<>();
            for (Column column : tableSchema.getColumns()) {
                if (column.getOptions() != null
                        && column.getOptions().containsKey(CommonOptions.METADATA.getName())
                        && (Boolean) column.getOptions().get(CommonOptions.METADATA.getName())) {
                    // skip dynamic field
                    continue;
                }
                FieldType fieldType =
                        MilvusSinkConverter.convertToFieldType(
                                column,
                                tableSchema.getPrimaryKey(),
                                partitionKeyField,
                                config.get(MilvusSinkConfig.ENABLE_AUTO_ID));
                fieldTypes.add(fieldType);
            }

            Boolean enableDynamicField =
                    (options.containsKey(MilvusOptions.ENABLE_DYNAMIC_FIELD))
                            ? Boolean.valueOf(options.get(MilvusOptions.ENABLE_DYNAMIC_FIELD))
                            : config.get(MilvusSinkConfig.ENABLE_DYNAMIC_FIELD);
            String collectionDescription = "";
            if (config.get(MilvusSinkConfig.COLLECTION_DESCRIPTION) != null
                    && config.get(MilvusSinkConfig.COLLECTION_DESCRIPTION)
                            .containsKey(tablePath.getTableName())) {
                // use description from config first
                collectionDescription =
                        config.get(MilvusSinkConfig.COLLECTION_DESCRIPTION)
                                .get(tablePath.getTableName());
            } else if (null != catalogTable.getComment()) {
                collectionDescription = catalogTable.getComment();
            }
            CreateCollectionParam.Builder builder =
                    CreateCollectionParam.newBuilder()
                            .withDatabaseName(tablePath.getDatabaseName())
                            .withCollectionName(tablePath.getTableName())
                            .withDescription(collectionDescription)
                            .withFieldTypes(fieldTypes)
                            .withEnableDynamicField(enableDynamicField)
                            .withConsistencyLevel(ConsistencyLevelEnum.BOUNDED);
            if (StringUtils.isNotEmpty(options.get(MilvusOptions.SHARDS_NUM))) {
                builder.withShardsNum(Integer.parseInt(options.get(MilvusOptions.SHARDS_NUM)));
            }

            CreateCollectionParam createCollectionParam = builder.build();
            R<RpcStatus> response = this.client.createCollection(createCollectionParam);
            if (!Objects.equals(response.getStatus(), R.success().getStatus())) {
                throw new MilvusConnectorException(
                        MilvusConnectionErrorCode.CREATE_COLLECTION_ERROR, response.getMessage());
            }

            // not exist partition key field, will read show partitions to create
            if (!existPartitionKeyField && options.containsKey(MilvusOptions.PARTITION_KEY_FIELD)) {
                createPartitionInternal(options.get(MilvusOptions.PARTITION_KEY_FIELD), tablePath);
            }

        } catch (Exception e) {
            throw new MilvusConnectorException(
                    MilvusConnectionErrorCode.CREATE_COLLECTION_ERROR, e);
        }
    }

    private void createPartitionInternal(String partitionNames, TablePath tablePath) {
        R<ShowPartitionsResponse> showPartitionsResponseR =
                this.client.showPartitions(
                        ShowPartitionsParam.newBuilder()
                                .withDatabaseName(tablePath.getDatabaseName())
                                .withCollectionName(tablePath.getTableName())
                                .build());
        if (!Objects.equals(showPartitionsResponseR.getStatus(), R.success().getStatus())) {
            throw new MilvusConnectorException(
                    MilvusConnectionErrorCode.SHOW_PARTITION_ERROR,
                    showPartitionsResponseR.getMessage());
        }
        ProtocolStringList existPartitionNames =
                showPartitionsResponseR.getData().getPartitionNamesList();

        // start to loop create partition
        String[] partitionNameArray = partitionNames.split(",");
        for (String partitionName : partitionNameArray) {
            if (existPartitionNames.contains(partitionName)) {
                continue;
            }
            R<RpcStatus> response =
                    this.client.createPartition(
                            CreatePartitionParam.newBuilder()
                                    .withDatabaseName(tablePath.getDatabaseName())
                                    .withCollectionName(tablePath.getTableName())
                                    .withPartitionName(partitionName)
                                    .build());
            if (!R.success().getStatus().equals(response.getStatus())) {
                throw new MilvusConnectorException(
                        MilvusConnectionErrorCode.CREATE_PARTITION_ERROR, response.getMessage());
            }
        }
    }

    @Override
    public void dropTable(TablePath tablePath, boolean ignoreIfNotExists)
            throws TableNotExistException, CatalogException {
        if (!databaseExists(tablePath.getDatabaseName())) {
            throw new DatabaseNotExistException(catalogName, tablePath.getDatabaseName());
        }
        if (!tableExists(tablePath)) {
            if (!ignoreIfNotExists) {
                throw new TableNotExistException(catalogName, tablePath);
            }
            return;
        }
        this.client.dropCollection(
                DropCollectionParam.newBuilder()
                        .withDatabaseName(tablePath.getDatabaseName())
                        .withCollectionName(tablePath.getTableName())
                        .build());
    }

    @Override
    public void createDatabase(TablePath tablePath, boolean ignoreIfExists)
            throws DatabaseAlreadyExistException, CatalogException {
        if (databaseExists(tablePath.getDatabaseName())) {
            if (!ignoreIfExists) {
                throw new DatabaseAlreadyExistException(catalogName, tablePath.getDatabaseName());
            }
            return;
        }
        R<RpcStatus> response =
                this.client.createDatabase(
                        CreateDatabaseParam.newBuilder()
                                .withDatabaseName(tablePath.getDatabaseName())
                                .build());
        if (!R.success().getStatus().equals(response.getStatus())) {
            throw new MilvusConnectorException(
                    MilvusConnectionErrorCode.CREATE_DATABASE_ERROR, response.getMessage());
        }
    }

    @Override
    public void dropDatabase(TablePath tablePath, boolean ignoreIfNotExists)
            throws DatabaseNotExistException, CatalogException {
        if (!databaseExists(tablePath.getDatabaseName())) {
            if (!ignoreIfNotExists) {
                throw new DatabaseNotExistException(catalogName, tablePath.getDatabaseName());
            }
            return;
        }
        this.client.dropDatabase(
                DropDatabaseParam.newBuilder()
                        .withDatabaseName(tablePath.getDatabaseName())
                        .build());
    }
}
