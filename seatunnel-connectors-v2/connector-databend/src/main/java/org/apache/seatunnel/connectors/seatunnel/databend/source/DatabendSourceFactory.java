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

package org.apache.seatunnel.connectors.seatunnel.databend.source;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.configuration.util.OptionRule;
import org.apache.seatunnel.api.source.SeaTunnelSource;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.TableIdentifier;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.api.table.connector.TableSource;
import org.apache.seatunnel.api.table.factory.Factory;
import org.apache.seatunnel.api.table.factory.TableSourceFactory;
import org.apache.seatunnel.api.table.factory.TableSourceFactoryContext;
import org.apache.seatunnel.connectors.seatunnel.databend.catalog.DatabendCatalog;
import org.apache.seatunnel.connectors.seatunnel.databend.config.DatabendOptions;
import org.apache.seatunnel.connectors.seatunnel.databend.config.DatabendSourceOptions;
import org.apache.seatunnel.connectors.seatunnel.databend.exception.DatabendConnectorErrorCode;
import org.apache.seatunnel.connectors.seatunnel.databend.exception.DatabendConnectorException;

import com.google.auto.service.AutoService;
import lombok.extern.slf4j.Slf4j;

import java.util.Collections;

import static org.apache.seatunnel.connectors.seatunnel.databend.config.DatabendOptions.DATABASE;

/** Databend source factory that creates Databend source connector. */
@AutoService(Factory.class)
@Slf4j
public class DatabendSourceFactory implements TableSourceFactory {
    @Override
    public String factoryIdentifier() {
        return "Databend";
    }

    @Override
    public OptionRule optionRule() {
        return OptionRule.builder()
                .required(DatabendOptions.URL, DatabendOptions.USERNAME, DatabendOptions.PASSWORD)
                .optional(
                        DATABASE,
                        DatabendOptions.TABLE,
                        DatabendOptions.JDBC_CONFIG,
                        DatabendOptions.FETCH_SIZE,
                        DatabendSourceOptions.SQL,
                        DatabendOptions.QUERY,
                        DatabendOptions.SSL)
                .build();
    }

    @Override
    public Class<? extends SeaTunnelSource> getSourceClass() {
        return DatabendSource.class;
    }

    @Override
    public TableSource createSource(TableSourceFactoryContext context) {
        return () -> {
            ReadonlyConfig options = context.getOptions();

            if (!options.get(DatabendOptions.URL).startsWith("jdbc:databend://")) {
                throw new DatabendConnectorException(
                        DatabendConnectorErrorCode.CONNECT_FAILED,
                        "Databend URL should start with 'jdbc:databend://'");
            }

            String url = options.get(DatabendOptions.URL);
            Boolean ssl = options.get(DatabendOptions.SSL);
            String username = options.get(DatabendOptions.USERNAME);
            String password = options.get(DatabendOptions.PASSWORD);
            Integer fetchSize = options.get(DatabendOptions.FETCH_SIZE);
            String sql = buildSqlStatement(options);

            String catalogName = "default";
            String database = options.getOptional(DATABASE).orElse("default");
            String table = options.getOptional(DatabendOptions.TABLE).orElse("default");

            // use catalog to get table schema
            DatabendCatalog catalog = new DatabendCatalog(options, catalogName);
            try {
                catalog.open();
                TablePath tablePath = TablePath.of(database, table);
                CatalogTable catalogTable = catalog.getTable(tablePath);
                log.info("Successfully retrieved catalog table: {}", catalogTable);
                return new DatabendSource(
                        catalogTable, sql, url, ssl, username, password, fetchSize);
            } catch (Exception e) {
                log.warn(
                        "Failed to get table schema from catalog, will try to infer schema from query",
                        e);
                TableSchema.Builder builder = TableSchema.builder();
                TableSchema tableSchema = builder.build();
                CatalogTable catalogTable =
                        CatalogTable.of(
                                TableIdentifier.of(catalogName, database, table),
                                tableSchema,
                                Collections.emptyMap(),
                                Collections.emptyList(),
                                "",
                                catalogName);
                return new DatabendSource(
                        catalogTable, sql, url, ssl, username, password, fetchSize);
            } finally {
                try {
                    catalog.close();
                } catch (Exception e) {
                    log.warn("Failed to close catalog", e);
                }
            }
        };
    }

    /** according to the options, build the SQL statement */
    private String buildSqlStatement(ReadonlyConfig options) {
        if (options.getOptional(DatabendSourceOptions.SQL).isPresent()) {
            return options.get(DatabendSourceOptions.SQL);
        }

        String query = options.getOptional(DatabendOptions.QUERY).orElse(null);
        if (query != null) {
            return query;
        }

        String database = options.getOptional(DATABASE).orElse(null);
        String table = options.getOptional(DatabendOptions.TABLE).orElse(null);

        if (database != null && table != null) {
            return String.format("SELECT * FROM %s.%s", database, table);
        }

        throw new DatabendConnectorException(
                DatabendConnectorErrorCode.SQL_OPERATION_FAILED,
                "Either SQL, query, or both database and table must be specified");
    }
}
