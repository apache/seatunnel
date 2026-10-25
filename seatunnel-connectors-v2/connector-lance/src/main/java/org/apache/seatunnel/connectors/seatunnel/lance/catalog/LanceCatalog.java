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

package org.apache.seatunnel.connectors.seatunnel.lance.catalog;

import org.apache.seatunnel.shade.com.google.common.collect.Lists;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.table.catalog.Catalog;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.PhysicalColumn;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.api.table.catalog.exception.CatalogException;
import org.apache.seatunnel.api.table.catalog.exception.DatabaseAlreadyExistException;
import org.apache.seatunnel.api.table.catalog.exception.DatabaseNotExistException;
import org.apache.seatunnel.api.table.catalog.exception.TableAlreadyExistException;
import org.apache.seatunnel.api.table.catalog.exception.TableNotExistException;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.connectors.seatunnel.lance.config.LanceCommonConfig;
import org.apache.seatunnel.connectors.seatunnel.lance.exception.LanceConnectorErrorCode;
import org.apache.seatunnel.connectors.seatunnel.lance.exception.LanceConnectorException;
import org.apache.seatunnel.connectors.seatunnel.lance.utils.SchemaUtils;

import org.apache.arrow.vector.types.DateUnit;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.TimeUnit;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.commons.collections4.CollectionUtils;

import com.lancedb.lance.Dataset;
import com.lancedb.lance.namespace.LanceNamespace;
import com.lancedb.lance.namespace.model.CreateTableRequest;
import com.lancedb.lance.namespace.model.DescribeTableRequest;
import com.lancedb.lance.namespace.model.DescribeTableResponse;
import com.lancedb.lance.namespace.model.DropTableRequest;
import com.lancedb.lance.namespace.model.JsonArrowDataType;
import com.lancedb.lance.namespace.model.JsonArrowField;
import com.lancedb.lance.namespace.model.JsonArrowSchema;
import com.lancedb.lance.namespace.model.ListTablesRequest;
import com.lancedb.lance.namespace.model.ListTablesResponse;
import com.lancedb.lance.namespace.model.TableExistsRequest;
import lombok.extern.slf4j.Slf4j;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

@Slf4j
public class LanceCatalog implements Catalog {

    private final String catalogName;

    private final ReadonlyConfig readonlyConfig;

    private LanceNamespace namespace;

    private LanceCatalogLoader catalogLoader;

    public LanceCatalog(String catalogName, ReadonlyConfig readonlyConfig) {
        this.catalogName = catalogName;
        this.readonlyConfig = readonlyConfig;
        this.catalogLoader = new LanceCatalogLoader(new LanceCommonConfig(readonlyConfig));
    }

    @Override
    public void open() throws CatalogException {
        this.namespace = catalogLoader.loadNamespace();
    }

    @Override
    public void close() throws CatalogException {
        if (namespace != null && namespace instanceof Closeable) {
            try {
                ((Closeable) namespace).close();
            } catch (IOException e) {
                log.error("Error while closing LanceNamespace.", e);
                throw new CatalogException(e);
            }
        }
    }

    @Override
    public String name() {
        return this.catalogName;
    }

    @Override
    public String getDefaultDatabase() throws CatalogException {
        return "default";
    }

    @Override
    public boolean databaseExists(String databaseName) throws CatalogException {
        // lanceNamespace not support yet
        return false;
    }

    @Override
    public List<String> listDatabases() throws CatalogException {
        // lance have no database level
        return null;
    }

    @Override
    public List<String> listTables(String namespaceName)
            throws CatalogException, DatabaseNotExistException {
        ListTablesRequest request = new ListTablesRequest();
        List<String> ids = Lists.newArrayList();
        if (namespaceName != null && !namespaceName.isEmpty()) {
            ids.add(namespaceName);
        }
        request.setId(ids);

        ListTablesResponse response = namespace.listTables(request);
        return Lists.newArrayList(response.getTables());
    }

    @Override
    public boolean tableExists(TablePath tablePath) throws CatalogException {
        TableExistsRequest request = new TableExistsRequest();
        List<String> ids = Lists.newArrayList(tablePath.getTableName());
        request.setId(ids);
        try {
            namespace.tableExists(request);
            return true;
        } catch (Exception e) {
            String errorMsg = e.getMessage();
            if (errorMsg != null
                    && (errorMsg.contains("Table does not exist")
                            || errorMsg.contains("TABLE_NOT_FOUND")
                            || errorMsg.contains("404"))) {
                return false;
            } else {
                throw new LanceConnectorException(
                        LanceConnectorErrorCode.TABLE_EXISTS_EXCEPTION, e.getMessage());
            }
        }
    }

    @Override
    public CatalogTable getTable(TablePath tablePath)
            throws CatalogException, TableNotExistException {
        DescribeTableRequest request = new DescribeTableRequest();
        List<String> ids = Lists.newArrayList(tablePath.getTableName());
        request.setId(ids);
        try {
            DescribeTableResponse response = namespace.describeTable(request);
            JsonArrowSchema arrowSchema = response.getSchema();
            Schema arrowSchemaFromDataset = null;
            String datasetPath = getDatasetPath(tablePath);
            if (datasetPath != null) {
                try {
                    Dataset dataset = Dataset.open(datasetPath);
                    arrowSchemaFromDataset = dataset.getSchema();
                    if (arrowSchema == null
                            || arrowSchema.getFields() == null
                            || arrowSchema.getFields().isEmpty()) {
                        if (arrowSchemaFromDataset != null
                                && arrowSchemaFromDataset.getFields() != null
                                && !arrowSchemaFromDataset.getFields().isEmpty()) {
                            // Convert Arrow Schema to JsonArrowSchema
                            arrowSchema =
                                    convertArrowSchemaToJsonArrowSchema(arrowSchemaFromDataset);
                            log.debug(
                                    "Successfully got schema from dataset with {} fields",
                                    arrowSchema.getFields().size());
                        }
                    }
                    dataset.close();
                } catch (Exception e) {
                    log.debug(
                            "Failed to get schema from dataset at {}: {}",
                            datasetPath,
                            e.getMessage());
                }
            }

            CatalogTable catalogTable =
                    convertTableSchema(arrowSchema, tablePath, arrowSchemaFromDataset);
            if (catalogTable == null) {
                throw new TableNotExistException(
                        catalogName,
                        tablePath,
                        new CatalogException(
                                "Table schema is null or empty. DescribeTable returned: "
                                        + (arrowSchema != null ? arrowSchema : "null schema")));
            }
            return catalogTable;
        } catch (Exception e) {
            String errorMsg = e.getMessage();
            if (errorMsg != null
                    && (errorMsg.contains("Table does not exist")
                            || errorMsg.contains("TABLE_NOT_FOUND")
                            || errorMsg.contains("404"))) {
                throw new TableNotExistException(catalogName, tablePath, e);
            } else {
                throw new CatalogException("Failed to get table: " + tablePath.getTableName(), e);
            }
        }
    }

    @Override
    public void createTable(TablePath tablePath, CatalogTable table, boolean ignoreIfExists)
            throws TableAlreadyExistException, DatabaseNotExistException, CatalogException {
        CreateTableRequest request = new CreateTableRequest();
        List<String> ids = Lists.newArrayList(tablePath.getTableName());
        request.setId(ids);
        byte[] requestData = new byte[0];
        try {
            requestData = SchemaUtils.convertJsonArrowSchemaToBytes(table.getTableSchema());
        } catch (IOException e) {
            throw new LanceConnectorException(
                    LanceConnectorErrorCode.TABLE_JSON_ARROW_SCHEMA_CONVERT_EXCEPTION,
                    e.getMessage());
        }

        namespace.createTable(request, requestData);

        String datasetPath = getDatasetPath(tablePath);
        if (datasetPath != null) {
            try {
                java.io.File datasetDir = new java.io.File(datasetPath);
                if (!datasetDir.exists()) {
                    Schema arrowSchema =
                            convertJsonArrowSchemaToArrowSchema(
                                    SchemaUtils.convertJsonArrowSchema(table.getTableSchema()));
                    if (arrowSchema != null) {
                        java.util.Map<String, String> metadata = new java.util.HashMap<>();
                        if (table.getTableSchema().getPrimaryKey() != null) {
                            metadata.put(
                                    "seatunnel.primaryKey.name",
                                    table.getTableSchema().getPrimaryKey().getPrimaryKey());
                            metadata.put(
                                    "seatunnel.primaryKey.columns",
                                    String.join(
                                            ",",
                                            table.getTableSchema()
                                                    .getPrimaryKey()
                                                    .getColumnNames()));
                        }
                        if (table.getComment() != null) {
                            metadata.put("seatunnel.comment", table.getComment());
                        }
                        if (table.getOptions() != null) {
                            for (java.util.Map.Entry<String, String> entry :
                                    table.getOptions().entrySet()) {
                                metadata.put(
                                        "seatunnel.option." + entry.getKey(), entry.getValue());
                            }
                        }

                        for (org.apache.seatunnel.api.table.catalog.Column column :
                                table.getTableSchema().getColumns()) {
                            if (column.getComment() != null && !column.getComment().isEmpty()) {
                                metadata.put(
                                        "seatunnel.column." + column.getName() + ".comment",
                                        column.getComment());
                            }
                        }

                        Schema schemaWithMetadata = new Schema(arrowSchema.getFields(), metadata);

                        org.apache.arrow.memory.BufferAllocator allocator =
                                new org.apache.arrow.memory.RootAllocator();
                        try {
                            com.lancedb.lance.WriteParams writeParams =
                                    new com.lancedb.lance.WriteParams.Builder().build();
                            com.lancedb.lance.Dataset.create(
                                    allocator, datasetPath, schemaWithMetadata, writeParams);
                            log.debug("Created empty dataset at {}", datasetPath);
                        } finally {
                            allocator.close();
                        }
                    }
                }
            } catch (Exception e) {
                throw new CatalogException("Failed to create empty dataset at " + datasetPath, e);
            }
        }
    }

    @Override
    public void dropTable(TablePath tablePath, boolean ignoreIfNotExists)
            throws TableNotExistException, CatalogException {
        DropTableRequest request = new DropTableRequest();
        List<String> ids = Lists.newArrayList(tablePath.getTableName());
        request.setId(ids);
        try {
            namespace.dropTable(request);
        } catch (Exception e) {
            String errorMsg = e.getMessage();
            if (errorMsg != null
                    && (errorMsg.contains("Table does not exist")
                            || errorMsg.contains("TABLE_NOT_FOUND")
                            || errorMsg.contains("404")
                            || errorMsg.contains("Not found"))) {
                if (!ignoreIfNotExists) {
                    throw new TableNotExistException(catalogName, tablePath, e);
                }
            } else {
                throw new CatalogException("Failed to drop table: " + tablePath.getTableName(), e);
            }
        }
    }

    @Override
    public void createDatabase(TablePath tablePath, boolean ignoreIfExists)
            throws DatabaseAlreadyExistException, CatalogException {}

    @Override
    public void dropDatabase(TablePath tablePath, boolean ignoreIfNotExists)
            throws DatabaseNotExistException, CatalogException {}

    private CatalogTable convertTableSchema(
            JsonArrowSchema arrowSchema, TablePath tablePath, Schema arrowSchemaFromDataset) {
        if (Objects.isNull(arrowSchema)) {
            return null;
        }

        List<JsonArrowField> fields = arrowSchema.getFields();
        if (CollectionUtils.isEmpty(fields)) {
            return null;
        }

        java.util.Map<String, String> metadataMap = new java.util.HashMap<>();
        if (arrowSchema.getMetadata() != null) {
            metadataMap.putAll(arrowSchema.getMetadata());
        }

        if (arrowSchemaFromDataset != null) {
            java.util.Map<String, String> customMetadata =
                    arrowSchemaFromDataset.getCustomMetadata();
            if (customMetadata != null && !customMetadata.isEmpty()) {
                metadataMap.putAll(customMetadata);
            }
        }

        final java.util.Map<String, String> columnMetadata = metadataMap;

        TableSchema.Builder builder = TableSchema.builder();
        fields.forEach(
                field -> {
                    SeaTunnelDataType<?> seaTunnelType =
                            SchemaUtils.toSeaTunnelType(field.getName(), field.getType());
                    String columnComment =
                            columnMetadata.get("seatunnel.column." + field.getName() + ".comment");
                    PhysicalColumn physicalColumn =
                            PhysicalColumn.of(
                                    field.getName(),
                                    seaTunnelType,
                                    (Long) null,
                                    field.getNullable(),
                                    null,
                                    columnComment);

                    builder.column(physicalColumn);
                });

        String pkName = metadataMap.get("seatunnel.primaryKey.name");
        String pkColumns = metadataMap.get("seatunnel.primaryKey.columns");
        if (pkName != null && pkColumns != null && !pkColumns.isEmpty()) {
            java.util.List<String> pkColumnList = java.util.Arrays.asList(pkColumns.split(","));
            builder.primaryKey(
                    org.apache.seatunnel.api.table.catalog.PrimaryKey.of(pkName, pkColumnList));
        }

        String comment = metadataMap.get("seatunnel.comment");
        java.util.Map<String, String> options = new java.util.HashMap<>();
        for (java.util.Map.Entry<String, String> entry : metadataMap.entrySet()) {
            if (entry.getKey().startsWith("seatunnel.option.")) {
                String optionKey = entry.getKey().substring("seatunnel.option.".length());
                options.put(optionKey, entry.getValue());
            }
        }

        return CatalogTable.of(
                org.apache.seatunnel.api.table.catalog.TableIdentifier.of(
                        catalogName,
                        tablePath.getDatabaseName(),
                        tablePath.getSchemaName(),
                        tablePath.getTableName()),
                builder.build(),
                options,
                new java.util.ArrayList<>(),
                comment,
                catalogName);
    }

    private String getDatasetPath(TablePath tablePath) {
        LanceCommonConfig config = new LanceCommonConfig(readonlyConfig);
        String rootPath = config.getRootNamespacePath();
        String datasetPath = config.getDatasetPath();
        String tableName = tablePath.getTableName();

        if (rootPath != null && datasetPath != null && tableName != null) {
            String fullPath = rootPath;
            if (!datasetPath.startsWith("/") && !fullPath.endsWith("/")) {
                fullPath += "/";
            }
            fullPath += datasetPath;
            if (!fullPath.endsWith("/")) {
                fullPath += "/";
            }
            fullPath += tableName;
            if (!fullPath.endsWith(".lance")) {
                fullPath += ".lance";
            }
            return fullPath;
        }
        return null;
    }

    private JsonArrowSchema convertArrowSchemaToJsonArrowSchema(Schema arrowSchema) {
        if (arrowSchema == null || arrowSchema.getFields() == null) {
            return null;
        }

        JsonArrowSchema jsonArrowSchema = new JsonArrowSchema();
        List<JsonArrowField> fields = new ArrayList<>();

        for (Field field : arrowSchema.getFields()) {
            JsonArrowField jsonField = new JsonArrowField();
            jsonField.setName(field.getName());
            jsonField.setNullable(field.isNullable());

            org.apache.arrow.vector.types.pojo.ArrowType arrowType = field.getType();
            com.lancedb.lance.namespace.model.JsonArrowDataType jsonType =
                    new com.lancedb.lance.namespace.model.JsonArrowDataType();

            if (arrowType instanceof org.apache.arrow.vector.types.pojo.ArrowType.Int) {
                jsonType.setType("int32");
            } else if (arrowType instanceof org.apache.arrow.vector.types.pojo.ArrowType.Utf8) {
                jsonType.setType("utf8");
            } else if (arrowType instanceof org.apache.arrow.vector.types.pojo.ArrowType.Bool) {
                jsonType.setType("bool");
            } else if (arrowType
                    instanceof org.apache.arrow.vector.types.pojo.ArrowType.FloatingPoint) {
                org.apache.arrow.vector.types.pojo.ArrowType.FloatingPoint fp =
                        (org.apache.arrow.vector.types.pojo.ArrowType.FloatingPoint) arrowType;
                if (fp.getPrecision()
                        == org.apache.arrow.vector.types.FloatingPointPrecision.SINGLE) {
                    jsonType.setType("float32");
                } else {
                    jsonType.setType("float64");
                }
            } else if (arrowType instanceof org.apache.arrow.vector.types.pojo.ArrowType.Binary) {
                jsonType.setType("binary");
            } else if (arrowType instanceof org.apache.arrow.vector.types.pojo.ArrowType.Date) {
                jsonType.setType("date32");
            } else if (arrowType
                    instanceof org.apache.arrow.vector.types.pojo.ArrowType.Timestamp) {
                jsonType.setType("timestamp");
            } else {
                log.warn("Unknown Arrow type: {}, defaulting to utf8", arrowType);
                jsonType.setType("utf8");
            }

            jsonField.setType(jsonType);
            fields.add(jsonField);
        }

        jsonArrowSchema.setFields(fields);
        return jsonArrowSchema;
    }

    private Schema convertJsonArrowSchemaToArrowSchema(JsonArrowSchema jsonArrowSchema) {
        if (jsonArrowSchema == null || jsonArrowSchema.getFields() == null) {
            return null;
        }

        List<Field> arrowFields = new ArrayList<>();
        for (JsonArrowField jsonField : jsonArrowSchema.getFields()) {
            String fieldName = jsonField.getName();
            Boolean nullable = jsonField.getNullable() != null ? jsonField.getNullable() : true;
            JsonArrowDataType jsonType = jsonField.getType();
            if (jsonType == null || jsonType.getType() == null) {
                continue;
            }

            ArrowType arrowType = convertJsonArrowTypeToArrowType(jsonType);
            if (arrowType != null) {
                Field arrowField =
                        nullable
                                ? Field.nullable(fieldName, arrowType)
                                : Field.notNullable(fieldName, arrowType);
                arrowFields.add(arrowField);
            }
        }

        return arrowFields.isEmpty() ? null : new Schema(arrowFields);
    }

    private ArrowType convertJsonArrowTypeToArrowType(JsonArrowDataType jsonType) {
        String type = jsonType.getType();
        if (type == null) {
            return null;
        }

        switch (type) {
            case "int8":
                return new ArrowType.Int(8, true);
            case "int16":
                return new ArrowType.Int(16, true);
            case "int32":
                return new ArrowType.Int(32, true);
            case "int64":
                return new ArrowType.Int(64, true);
            case "uint8":
                return new ArrowType.Int(8, false);
            case "uint16":
                return new ArrowType.Int(16, false);
            case "uint32":
                return new ArrowType.Int(32, false);
            case "uint64":
                return new ArrowType.Int(64, false);
            case "float32":
                return new ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE);
            case "float64":
                return new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE);
            case "bool":
                return new ArrowType.Bool();
            case "utf8":
            case "string":
                return new ArrowType.Utf8();
            case "binary":
                return new ArrowType.Binary();
            case "date32":
                return new ArrowType.Date(DateUnit.DAY);
            case "date64":
                return new ArrowType.Date(DateUnit.MILLISECOND);
            case "timestamp":
                return new ArrowType.Timestamp(TimeUnit.MICROSECOND, null);
            case "list":
                return new ArrowType.List();
            case "map":
                return new ArrowType.Map(false);
            case "decimal128":
                return new ArrowType.Decimal(38, 10, 128);
            default:
                log.warn("Unknown JsonArrow type: {}, defaulting to utf8", type);
                return new ArrowType.Utf8();
        }
    }
}
