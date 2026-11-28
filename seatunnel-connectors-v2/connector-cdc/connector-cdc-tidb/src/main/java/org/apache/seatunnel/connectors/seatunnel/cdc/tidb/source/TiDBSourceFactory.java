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

package org.apache.seatunnel.connectors.seatunnel.cdc.tidb.source;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.configuration.util.OptionRule;
import org.apache.seatunnel.api.source.SeaTunnelSource;
import org.apache.seatunnel.api.source.SourceSplit;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.api.table.connector.TableSource;
import org.apache.seatunnel.api.table.factory.Factory;
import org.apache.seatunnel.api.table.factory.TableSourceFactory;
import org.apache.seatunnel.api.table.factory.TableSourceFactoryContext;
import org.apache.seatunnel.connectors.seatunnel.cdc.tidb.source.config.TiDBSourceOptions;
import org.apache.seatunnel.connectors.seatunnel.jdbc.catalog.tidb.TiDBCatalog;
import org.apache.seatunnel.connectors.seatunnel.jdbc.catalog.tidb.TiDBCatalogFactory;
import org.apache.seatunnel.connectors.seatunnel.jdbc.config.JdbcCommonOptions;

import com.google.auto.service.AutoService;
import lombok.extern.slf4j.Slf4j;

import java.io.Serializable;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

@AutoService(Factory.class)
@Slf4j
public class TiDBSourceFactory implements TableSourceFactory {
    /**
     * Returns a unique identifier among same factory interfaces.
     *
     * <p>For consistency, an identifier should be declared as one lower case word (e.g. {@code
     * kafka}). If multiple factories exist for different versions, a version should be appended
     * using "-" (e.g. {@code elasticsearch-7}).
     */
    @Override
    public String factoryIdentifier() {
        return TiDBSource.IDENTIFIER;
    }

    /**
     * Returns the rule for options.
     *
     * <p>1. Used to verify whether the parameters configured by the user conform to the rules of
     * the options;
     *
     * <p>2. Used for Web-UI to prompt user to configure option value;
     */
    @Override
    public OptionRule optionRule() {
        return OptionRule.builder()
                .required(
                        TiDBSourceOptions.DATABASE_NAME,
                        TiDBSourceOptions.TABLE_NAME,
                        TiDBSourceOptions.PD_ADDRESSES)
                .optional(
                        TiDBSourceOptions.TIKV_BATCH_GET_CONCURRENCY,
                        TiDBSourceOptions.TIKV_BATCH_SCAN_CONCURRENCY,
                        TiDBSourceOptions.TIKV_GRPC_SCAN_TIMEOUT,
                        TiDBSourceOptions.TIKV_GRPC_TIMEOUT,
                        TiDBSourceOptions.STARTUP_MODE)
                .build();
    }

    /**
     * TODO: Implement SupportParallelism in the TableSourceFactory instead of the SeaTunnelSource,
     * Then deprecated the method
     */
    @Override
    public Class<? extends SeaTunnelSource> getSourceClass() {
        return TiDBSource.class;
    }

    @SuppressWarnings("unchecked")
    @Override
    public <T, SplitT extends SourceSplit, StateT extends Serializable>
            TableSource<T, SplitT, StateT> createSource(TableSourceFactoryContext context) {
        return () -> {
            // Load the JDBC driver in to DriverManager
            try {
                Class.forName("com.mysql.cj.jdbc.Driver");
            } catch (Exception e) {
                log.warn("Failed to load JDBC driver com.mysql.cj.jdbc.Driver ", e);
            }
            ReadonlyConfig config = context.getOptions();
            TiDBCatalogFactory catalogFactory = new TiDBCatalogFactory();
            // Build tidb catalog.
            TiDBCatalog catalog =
                    (TiDBCatalog) catalogFactory.createCatalog(factoryIdentifier(), config);

            TablePath tablePath =
                    TablePath.of(
                            config.get(TiDBSourceOptions.DATABASE_NAME),
                            config.get(TiDBSourceOptions.TABLE_NAME));
            CatalogTable catalogTable = catalog.getTable(tablePath);
            return (SeaTunnelSource<T, SplitT, StateT>)
                    new TiDBSource(
                            context.getOptions(),
                            catalogTable,
                            hasClusteredPrimaryKey(config, tablePath));
        };
    }
    /**
     * Distinguishes clustered keys from hidden row IDs using explicit TiDB table metadata. Byte
     * length cannot distinguish every valid decimal common handle from an integer row ID.
     */
    static boolean hasClusteredPrimaryKey(Connection connection, String database, String table)
            throws SQLException {
        try (PreparedStatement statement =
                connection.prepareStatement(
                        "SELECT TIDB_PK_TYPE FROM information_schema.tables WHERE table_schema = ? AND table_name = ?")) {
            statement.setString(1, database);
            statement.setString(2, table);
            try (ResultSet result = statement.executeQuery()) {
                if (!result.next()) {
                    throw new SQLException(
                            "TiDB table metadata is unavailable for " + database + "." + table);
                }
                return "CLUSTERED".equalsIgnoreCase(result.getString(1));
            }
        } catch (SQLException e) {
            // Before TiDB 5.0 this column and common handles do not exist. Other failures must
            // surface.
            if (e.getErrorCode() == 1054 && "42S22".equals(e.getSQLState())) {
                return false;
            }
            throw e;
        }
    }

    /**
     * Reads the clustered-key marker once before reader configuration is serialized. The bundled
     * TiKV 3.3.5 model discards this schema field, so key bytes cannot supply it.
     */
    private static boolean hasClusteredPrimaryKey(ReadonlyConfig config, TablePath tablePath) {
        try (Connection connection =
                DriverManager.getConnection(
                        config.get(JdbcCommonOptions.URL),
                        config.get(JdbcCommonOptions.USERNAME),
                        config.get(JdbcCommonOptions.PASSWORD))) {
            return hasClusteredPrimaryKey(
                    connection, tablePath.getDatabaseName(), tablePath.getTableName());
        } catch (SQLException e) {
            throw new IllegalStateException(
                    "Cannot determine TiDB primary key type for " + tablePath, e);
        }
    }
}
