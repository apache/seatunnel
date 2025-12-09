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

import org.apache.commons.collections4.CollectionUtils;

import com.lancedb.lance.namespace.LanceNamespace;
import com.lancedb.lance.namespace.model.CreateTableRequest;
import com.lancedb.lance.namespace.model.DescribeTableRequest;
import com.lancedb.lance.namespace.model.DescribeTableResponse;
import com.lancedb.lance.namespace.model.DropTableRequest;
import com.lancedb.lance.namespace.model.JsonArrowField;
import com.lancedb.lance.namespace.model.JsonArrowSchema;
import com.lancedb.lance.namespace.model.ListTablesRequest;
import com.lancedb.lance.namespace.model.ListTablesResponse;
import com.lancedb.lance.namespace.model.TableExistsRequest;
import lombok.extern.slf4j.Slf4j;

import java.io.Closeable;
import java.io.IOException;
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
        ids.add(namespaceName);
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
        } catch (Exception e) {
            if (e instanceof UnsupportedOperationException
                    && e.getMessage().contains("Table does not exist")) {
                return false;
            } else {
                throw new LanceConnectorException(
                        LanceConnectorErrorCode.TABLE_EXISTS_EXCEPTION, e.getMessage());
            }
        }
        return true;
    }

    @Override
    public CatalogTable getTable(TablePath tablePath)
            throws CatalogException, TableNotExistException {
        DescribeTableRequest request = new DescribeTableRequest();
        List<String> ids = Lists.newArrayList(tablePath.getTableName());
        request.setId(ids);
        DescribeTableResponse response = namespace.describeTable(request);
        JsonArrowSchema arrowSchema = response.getSchema();
        return convertTableSchema(arrowSchema, tablePath.getTableName());
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
    }

    @Override
    public void dropTable(TablePath tablePath, boolean ignoreIfNotExists)
            throws TableNotExistException, CatalogException {
        DropTableRequest request = new DropTableRequest();
        List<String> ids = Lists.newArrayList(tablePath.getTableName());
        request.setId(ids);
        namespace.dropTable(request);
    }

    @Override
    public void createDatabase(TablePath tablePath, boolean ignoreIfExists)
            throws DatabaseAlreadyExistException, CatalogException {}

    @Override
    public void dropDatabase(TablePath tablePath, boolean ignoreIfNotExists)
            throws DatabaseNotExistException, CatalogException {}

    private CatalogTable convertTableSchema(JsonArrowSchema arrowSchema, String tableName) {
        if (Objects.isNull(arrowSchema)) {
            return null;
        }

        List<JsonArrowField> fields = arrowSchema.getFields();
        if (CollectionUtils.isEmpty(fields)) {
            return null;
        }

        TableSchema.Builder builder = TableSchema.builder();
        fields.forEach(
                field -> {
                    SeaTunnelDataType<?> seaTunnelType =
                            SchemaUtils.toSeaTunnelType(field.getName(), field.getType());
                    PhysicalColumn physicalColumn =
                            PhysicalColumn.of(
                                    field.getName(),
                                    seaTunnelType,
                                    (Long) null,
                                    field.getNullable(),
                                    null,
                                    null);

                    builder.column(physicalColumn);
                });

        return CatalogTable.of(
                org.apache.seatunnel.api.table.catalog.TableIdentifier.of(
                        catalogName, "", tableName),
                builder.build(),
                arrowSchema.getMetadata(),
                null,
                null,
                catalogName);
    }
}
