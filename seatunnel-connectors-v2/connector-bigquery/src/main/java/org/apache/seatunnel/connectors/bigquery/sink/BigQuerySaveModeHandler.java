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

package org.apache.seatunnel.connectors.bigquery.sink;

import org.apache.seatunnel.api.sink.DataSaveMode;
import org.apache.seatunnel.api.sink.DefaultSaveModeHandler;
import org.apache.seatunnel.api.sink.SchemaSaveMode;
import org.apache.seatunnel.api.table.catalog.Catalog;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.Column;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.api.table.catalog.exception.CatalogException;
import org.apache.seatunnel.api.table.catalog.exception.TableNotExistException;
import org.apache.seatunnel.api.table.type.SqlType;
import org.apache.seatunnel.common.exception.SeaTunnelRuntimeException;

import com.google.cloud.bigquery.BigQueryException;
import lombok.extern.slf4j.Slf4j;

import java.util.Optional;

/**
 * Handler for managing BigQuery schema and data save modes. This class validates schema coherence
 * between local schemas and remote BigQuery schemas, and handles database/table life cycle based on
 * configurations.
 */
@Slf4j
public class BigQuerySaveModeHandler extends DefaultSaveModeHandler {

    /**
     * Constructs a new BigQuerySaveModeHandler.
     *
     * @param schemaSaveMode the schema save mode configuration
     * @param dataSaveMode the data save mode configuration
     * @param catalog the BigQuery catalog instance
     * @param tablePath the path of the target table
     * @param catalogTable the local catalog table schema representation
     * @param customSql optional custom SQL script to run during save mode handling
     */
    public BigQuerySaveModeHandler(
            SchemaSaveMode schemaSaveMode,
            DataSaveMode dataSaveMode,
            Catalog catalog,
            TablePath tablePath,
            CatalogTable catalogTable,
            String customSql) {
        super(schemaSaveMode, dataSaveMode, catalog, tablePath, catalogTable, customSql);
    }

    /**
     * Executes the schema save mode operations (e.g., table deletion, creation, recreation) and
     * performs schema coherence check to ensure the target table is compatible.
     */
    @Override
    public void handleSchemaSaveMode() {
        // 1. Let the default handler execute table creation / recreation / deletion
        super.handleSchemaSaveMode();

        // 2. Perform schema coherence validation
        validateSchemaCoherence();
    }

    /**
     * Validates that the schema of the local table matches or is compatible with the remote
     * BigQuery table's schema.
     *
     * @throws CatalogException if schema coherence validation fails or Google SDK exception is
     *     caught
     * @throws SeaTunnelRuntimeException if any system runtime exception is encountered
     */
    private void validateSchemaCoherence() {
        if (schemaSaveMode == SchemaSaveMode.RECREATE_SCHEMA
                || schemaSaveMode == SchemaSaveMode.IGNORE) {
            return;
        }

        try {
            CatalogTable remoteTable = catalog.getTable(tablePath);
            if (remoteTable == null || remoteTable.getTableSchema() == null) {
                return;
            }

            for (Column sourceColumn : catalogTable.getTableSchema().getColumns()) {
                Optional<Column> remoteColOpt =
                        remoteTable.getTableSchema().getColumns().stream()
                                .filter(c -> c.getName().equalsIgnoreCase(sourceColumn.getName()))
                                .findFirst();

                if (!remoteColOpt.isPresent()) {
                    throw new CatalogException(
                            String.format(
                                    "Target BigQuery table '%s' is missing source column: '%s'",
                                    tablePath.getFullName(), sourceColumn.getName()));
                }

                Column remoteColumn = remoteColOpt.get();
                if (!isTypeCompatible(
                        sourceColumn.getDataType().getSqlType(),
                        remoteColumn.getDataType().getSqlType())) {
                    throw new CatalogException(
                            String.format(
                                    "Type mismatch for column '%s' inside target BigQuery table '%s'. Source type: '%s', target type: '%s'",
                                    sourceColumn.getName(),
                                    tablePath.getFullName(),
                                    sourceColumn.getDataType().getSqlType(),
                                    remoteColumn.getDataType().getSqlType()));
                }
            }
            log.info(
                    "BigQuery schema coherence check passed successfully for table: {}",
                    tablePath.getFullName());
        } catch (TableNotExistException e) {
            log.info(
                    "Target BigQuery table '{}' does not exist yet. Skipping schema coherence check.",
                    tablePath.getFullName());
            return;
        } catch (Exception e) {
            if (e instanceof CatalogException) {
                throw (CatalogException) e;
            }
            if (e instanceof SeaTunnelRuntimeException) {
                throw (SeaTunnelRuntimeException) e;
            }
            if (e instanceof BigQueryException) {
                throw new CatalogException("Failed to validate BigQuery schema coherence", e);
            }
            log.warn("Schema validation check ignored due to exception: {}", e.getMessage());
        }
    }

    private boolean isTypeCompatible(SqlType source, SqlType sink) {
        if (source == sink) {
            return true;
        }
        // Widening integer compatibility
        if ((source == SqlType.TINYINT
                        || source == SqlType.SMALLINT
                        || source == SqlType.INT
                        || source == SqlType.BIGINT)
                && (sink == SqlType.BIGINT)) {
            return true;
        }
        // Widening float compatibility
        if ((source == SqlType.FLOAT || source == SqlType.DOUBLE) && (sink == SqlType.DOUBLE)) {
            return true;
        }
        return false;
    }
}
