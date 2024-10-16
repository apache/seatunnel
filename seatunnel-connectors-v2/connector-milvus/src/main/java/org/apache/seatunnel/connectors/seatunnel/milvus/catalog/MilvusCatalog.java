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
import org.apache.seatunnel.api.table.catalog.PrimaryKey;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.api.table.catalog.VectorIndex;
import org.apache.seatunnel.api.table.catalog.exception.CatalogException;
import org.apache.seatunnel.api.table.catalog.exception.DatabaseAlreadyExistException;
import org.apache.seatunnel.api.table.catalog.exception.DatabaseNotExistException;
import org.apache.seatunnel.api.table.catalog.exception.TableAlreadyExistException;
import org.apache.seatunnel.api.table.catalog.exception.TableNotExistException;
import org.apache.seatunnel.api.table.type.ArrayType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.connectors.seatunnel.milvus.config.MilvusSinkConfig;
import org.apache.seatunnel.connectors.seatunnel.milvus.convert.MilvusConvertUtils;
import org.apache.seatunnel.connectors.seatunnel.milvus.exception.MilvusConnectionErrorCode;
import org.apache.seatunnel.connectors.seatunnel.milvus.exception.MilvusConnectorException;

import org.apache.commons.collections4.CollectionUtils;

import io.milvus.client.MilvusServiceClient;
import io.milvus.common.clientenum.ConsistencyLevelEnum;
import io.milvus.grpc.DataType;
import io.milvus.grpc.ListDatabasesResponse;
import io.milvus.grpc.ShowCollectionsResponse;
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
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkNotNull;

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

        if (CollectionUtils.isNotEmpty(tableSchema.getConstraintKeys())) {
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
            TableSchema tableSchema = catalogTable.getTableSchema();
            List<FieldType> fieldTypes = new ArrayList<>();
            for (Column column : tableSchema.getColumns()) {
                fieldTypes.add(convertToFieldType(column, tableSchema.getPrimaryKey()));
            }

            Map<String, String> options = catalogTable.getOptions();
            Boolean enableDynamicField =
                    (options.containsKey(MilvusOptions.ENABLE_DYNAMIC_FIELD))
                            ? Boolean.valueOf(options.get(MilvusOptions.ENABLE_DYNAMIC_FIELD))
                            : config.get(MilvusSinkConfig.ENABLE_DYNAMIC_FIELD);

            CreateCollectionParam.Builder builder =
                    CreateCollectionParam.newBuilder()
                            .withDatabaseName(tablePath.getDatabaseName())
                            .withCollectionName(tablePath.getTableName())
                            .withFieldTypes(fieldTypes)
                            .withEnableDynamicField(enableDynamicField)
                            .withConsistencyLevel(ConsistencyLevelEnum.BOUNDED);
            if (null != catalogTable.getComment()) {
                builder.withDescription(catalogTable.getComment());
            }

            CreateCollectionParam createCollectionParam = builder.build();
            R<RpcStatus> response = this.client.createCollection(createCollectionParam);
            if (!Objects.equals(response.getStatus(), R.success().getStatus())) {
                throw new MilvusConnectorException(
                        MilvusConnectionErrorCode.CREATE_COLLECTION_ERROR, response.getMessage());
            }
        } catch (Exception e) {
            throw new MilvusConnectorException(
                    MilvusConnectionErrorCode.CREATE_COLLECTION_ERROR, e);
        }
    }

    private FieldType convertToFieldType(Column column, PrimaryKey primaryKey) {
        SeaTunnelDataType<?> seaTunnelDataType = column.getDataType();
        FieldType.Builder build =
                FieldType.newBuilder()
                        .withName(column.getName())
                        .withDataType(
                                MilvusConvertUtils.convertSqlTypeToDataType(
                                        seaTunnelDataType.getSqlType()));
        switch (seaTunnelDataType.getSqlType()) {
            case ROW:
                build.withMaxLength(65535);
                break;
            case DATE:
                build.withMaxLength(20);
                break;
            case INT:
                build.withDataType(DataType.Int32);
                break;
            case SMALLINT:
                build.withDataType(DataType.Int16);
                break;
            case TINYINT:
                build.withDataType(DataType.Int8);
                break;
            case FLOAT:
                build.withDataType(DataType.Float);
                break;
            case DOUBLE:
                build.withDataType(DataType.Double);
                break;
            case MAP:
                build.withDataType(DataType.JSON);
                break;
            case BOOLEAN:
                build.withDataType(DataType.Bool);
                break;
            case STRING:
                if (column.getColumnLength() == 0) {
                    build.withMaxLength(512);
                } else {
                    build.withMaxLength((int) (column.getColumnLength() / 4));
                }
                break;
            case ARRAY:
                ArrayType arrayType = (ArrayType) column.getDataType();
                SeaTunnelDataType elementType = arrayType.getElementType();
                build.withElementType(
                        MilvusConvertUtils.convertSqlTypeToDataType(elementType.getSqlType()));
                build.withMaxCapacity(4095);
                switch (elementType.getSqlType()) {
                    case STRING:
                        if (column.getColumnLength() == 0) {
                            build.withMaxLength(512);
                        } else {
                            build.withMaxLength((int) (column.getColumnLength() / 4));
                        }
                        break;
                }
                break;
            case BINARY_VECTOR:
            case FLOAT_VECTOR:
            case FLOAT16_VECTOR:
            case BFLOAT16_VECTOR:
                build.withDimension(column.getScale());
                break;
        }

        if (null != primaryKey && primaryKey.getColumnNames().contains(column.getName())) {
            build.withPrimaryKey(true);
            if (null != primaryKey.getEnableAutoId()) {
                build.withAutoID(primaryKey.getEnableAutoId());
            } else {
                build.withAutoID(config.get(MilvusSinkConfig.ENABLE_AUTO_ID));
            }
        }

        return build.build();
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
