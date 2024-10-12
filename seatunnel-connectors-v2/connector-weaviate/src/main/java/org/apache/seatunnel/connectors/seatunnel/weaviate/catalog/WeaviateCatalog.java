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

package org.apache.seatunnel.connectors.seatunnel.weaviate.catalog;

import org.apache.seatunnel.api.table.catalog.Catalog;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.Column;
import org.apache.seatunnel.api.table.catalog.ConstraintKey;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.api.table.catalog.exception.CatalogException;
import org.apache.seatunnel.api.table.catalog.exception.DatabaseAlreadyExistException;
import org.apache.seatunnel.api.table.catalog.exception.DatabaseNotExistException;
import org.apache.seatunnel.api.table.catalog.exception.TableAlreadyExistException;
import org.apache.seatunnel.api.table.catalog.exception.TableNotExistException;
import org.apache.seatunnel.connectors.seatunnel.weaviate.config.WeaviateParameters;
import org.apache.seatunnel.connectors.seatunnel.weaviate.exception.WeaviateConnectionErrorCode;
import org.apache.seatunnel.connectors.seatunnel.weaviate.exception.WeaviateConnectorException;

import org.apache.commons.collections4.CollectionUtils;

import com.google.common.collect.Lists;
import io.weaviate.client.Config;
import io.weaviate.client.WeaviateClient;
import io.weaviate.client.v1.schema.model.Property;
import io.weaviate.client.v1.schema.model.WeaviateClass;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.checkNotNull;
import static org.apache.seatunnel.connectors.seatunnel.weaviate.convert.WeaviateConverter.convertSqlTypeToDataType;

public class WeaviateCatalog implements Catalog {

    private final String catalogName;
    private final WeaviateParameters parameters;

    private WeaviateClient client;

    public WeaviateCatalog(String catalogName, WeaviateParameters parameters) {
        this.catalogName = catalogName;
        this.parameters = parameters;
    }

    @Override
    public void open() throws CatalogException {
        // Connection configuration
        Config config =
                new Config(
                        parameters.getClassName(),
                        parameters.getUrl(),
                        parameters.getHeader(),
                        parameters.getConnectionTimeout(),
                        parameters.getReadTimeout(),
                        parameters.getWriteTimeout());
        this.client = new WeaviateClient(config);
    }

    @Override
    public void close() throws CatalogException {
        // nothing
    }

    @Override
    public String name() {
        return catalogName;
    }

    @Override
    public String getDefaultDatabase() throws CatalogException {
        return "default";
    }

    @Override
    public boolean databaseExists(String databaseName) throws CatalogException {
        return this.listDatabases().contains(databaseName);
    }

    @Override
    public List<String> listDatabases() throws CatalogException {
        List<WeaviateClass> classes = client.schema().getter().run().getResult().getClasses();
        if (classes != null) {
            return classes.stream().map(WeaviateClass::getClassName).collect(Collectors.toList());
        }
        return Collections.emptyList();
    }

    @Override
    public List<String> listTables(String databaseName)
            throws CatalogException, DatabaseNotExistException {
        if (!databaseExists(databaseName)) {
            throw new DatabaseNotExistException(catalogName, databaseName);
        }
        return Lists.newArrayList(databaseName);
    }

    @Override
    public boolean tableExists(TablePath tablePath) throws CatalogException {
        checkNotNull(tablePath);
        return databaseExists(tablePath.getTableName());
    }

    @Override
    public CatalogTable getTable(TablePath tablePath)
            throws CatalogException, TableNotExistException {
        throw new RuntimeException("not implemented");
    }

    @Override
    public void createTable(TablePath tablePath, CatalogTable table, boolean ignoreIfExists)
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

        checkNotNull(table, "catalogTable must not be null");
        TableSchema tableSchema = table.getTableSchema();
        checkNotNull(tableSchema, "tableSchema must not be null");
        // Create class
        createWeaviateClass(tablePath, table);

        // Create index
        if (CollectionUtils.isNotEmpty(tableSchema.getConstraintKeys())) {
            for (ConstraintKey constraintKey : tableSchema.getConstraintKeys()) {
                if (constraintKey
                        .getConstraintType()
                        .equals(ConstraintKey.ConstraintType.VECTOR_INDEX_KEY)) {
                    // createIndexInternal(tablePath, constraintKey.getColumnNames());
                }
            }
        }
    }

    public void createWeaviateClass(TablePath tablePath, CatalogTable catalogTable) {
        try {
            TableSchema tableSchema = catalogTable.getTableSchema();
            List<Property> properties = new ArrayList<>();

            for (Column column : tableSchema.getColumns()) {
                properties.add(convertToWeaviateProperty(column));
            }

            WeaviateClass.WeaviateClassBuilder builder =
                    WeaviateClass.builder()
                            .className(tablePath.getTableName())
                            .description(catalogTable.getComment())
                            .properties(properties);

            WeaviateClass weaviateClass = builder.build();

            this.client.schema().classCreator().withClass(weaviateClass).run();

        } catch (Exception e) {
            throw new WeaviateConnectorException(WeaviateConnectionErrorCode.CREATE_CLASS_ERROR, e);
        }
    }

    private Property convertToWeaviateProperty(Column column) {
        Property.PropertyBuilder builder = Property.builder();
        builder.name(column.getName())
                .dataType(
                        Collections.singletonList(
                                convertSqlTypeToDataType(column.getDataType().getSqlType())))
                .description(column.getComment());
        return builder.build();
    }

    @Override
    public void dropTable(TablePath tablePath, boolean ignoreIfNotExists)
            throws TableNotExistException, CatalogException {
        if (!tableExists(tablePath)) {
            if (ignoreIfNotExists) {
                return;
            }
            client.schema().classDeleter().withClassName(tablePath.getTableName()).run();
        }
    }

    @Override
    public void createDatabase(TablePath tablePath, boolean ignoreIfExists)
            throws DatabaseAlreadyExistException, CatalogException {
        createTable(tablePath, null, ignoreIfExists);
    }

    @Override
    public void dropDatabase(TablePath tablePath, boolean ignoreIfNotExists)
            throws DatabaseNotExistException, CatalogException {
        dropTable(tablePath, ignoreIfNotExists);
    }
}
