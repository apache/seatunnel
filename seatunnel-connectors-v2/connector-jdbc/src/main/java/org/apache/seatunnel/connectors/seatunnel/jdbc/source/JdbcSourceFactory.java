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

package org.apache.seatunnel.connectors.seatunnel.jdbc.source;

import org.apache.seatunnel.api.common.PrepareFailException;
import org.apache.seatunnel.api.configuration.util.OptionRule;
import org.apache.seatunnel.api.source.SeaTunnelSource;
import org.apache.seatunnel.api.source.SourceSplit;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.PrimaryKey;
import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.api.table.connector.TableSource;
import org.apache.seatunnel.api.table.factory.Factory;
import org.apache.seatunnel.api.table.factory.TableFactoryContext;
import org.apache.seatunnel.api.table.factory.TableSourceFactory;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.DecimalType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.common.constants.PluginType;
import org.apache.seatunnel.common.exception.CommonErrorCode;
import org.apache.seatunnel.connectors.seatunnel.jdbc.config.JdbcSourceConfig;
import org.apache.seatunnel.connectors.seatunnel.jdbc.exception.JdbcConnectorException;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.JdbcInputFormat;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.connection.JdbcConnectionProvider;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.connection.SimpleJdbcConnectionProvider;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.JdbcDialect;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.JdbcDialectLoader;

import com.google.auto.service.AutoService;
import lombok.extern.slf4j.Slf4j;

import java.io.Serializable;
import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import static org.apache.seatunnel.connectors.seatunnel.jdbc.config.JdbcOptions.COMPATIBLE_MODE;
import static org.apache.seatunnel.connectors.seatunnel.jdbc.config.JdbcOptions.CONNECTION_CHECK_TIMEOUT_SEC;
import static org.apache.seatunnel.connectors.seatunnel.jdbc.config.JdbcOptions.DRIVER;
import static org.apache.seatunnel.connectors.seatunnel.jdbc.config.JdbcOptions.FETCH_SIZE;
import static org.apache.seatunnel.connectors.seatunnel.jdbc.config.JdbcOptions.PARTITION_COLUMN;
import static org.apache.seatunnel.connectors.seatunnel.jdbc.config.JdbcOptions.PARTITION_LOWER_BOUND;
import static org.apache.seatunnel.connectors.seatunnel.jdbc.config.JdbcOptions.PARTITION_NUM;
import static org.apache.seatunnel.connectors.seatunnel.jdbc.config.JdbcOptions.PARTITION_UPPER_BOUND;
import static org.apache.seatunnel.connectors.seatunnel.jdbc.config.JdbcOptions.PASSWORD;
import static org.apache.seatunnel.connectors.seatunnel.jdbc.config.JdbcOptions.QUERY;
import static org.apache.seatunnel.connectors.seatunnel.jdbc.config.JdbcOptions.URL;
import static org.apache.seatunnel.connectors.seatunnel.jdbc.config.JdbcOptions.USER;

@Slf4j
@AutoService(Factory.class)
public class JdbcSourceFactory implements TableSourceFactory {
    @Override
    public String factoryIdentifier() {
        return "Jdbc";
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T, SplitT extends SourceSplit, StateT extends Serializable>
            TableSource<T, SplitT, StateT> createSource(TableFactoryContext context) {
        CatalogTable catalogTable = context.getCatalogTable();
        JdbcSourceConfig config = JdbcSourceConfig.of(context.getOptions());
        JdbcConnectionProvider connectionProvider =
                new SimpleJdbcConnectionProvider(config.getJdbcConnectionConfig());
        final String querySql = config.getQuery();
        JdbcDialect dialect =
                JdbcDialectLoader.load(
                        config.getJdbcConnectionConfig().getUrl(),
                        config.getJdbcConnectionConfig().getCompatibleMode());
        TableSchema tableSchema = catalogTable.getTableSchema();
        SeaTunnelRowType rowType = tableSchema.toPhysicalRowDataType();
        Optional<PartitionParameter> partitionParameter =
                createPartitionParameter(config, tableSchema, connectionProvider);
        JdbcInputFormat inputFormat =
                new JdbcInputFormat(
                        connectionProvider,
                        dialect,
                        rowType,
                        querySql,
                        config.getFetchSize(),
                        config.getJdbcConnectionConfig().isAutoCommit());
        return () ->
                (SeaTunnelSource<T, SplitT, StateT>)
                        new JdbcSource(
                                config,
                                rowType,
                                dialect,
                                inputFormat,
                                partitionParameter.orElse(null),
                                connectionProvider,
                                partitionParameter.isPresent()
                                        ? obtainPartitionSql(
                                                dialect, partitionParameter.get(), querySql)
                                        : querySql);
    }

    static String obtainPartitionSql(
            JdbcDialect dialect, PartitionParameter partitionParameter, String nativeSql) {
        if (isStringType(partitionParameter.getDataType())) {
            return String.format(
                    "SELECT * FROM (%s) tt where %s = ?",
                    nativeSql,
                    dialect.hashModForField(
                            partitionParameter.getPartitionColumnName(),
                            partitionParameter.getPartitionNumber()));
        }
        return String.format(
                "SELECT * FROM (%s) tt where %s >= ? AND %s <= ?",
                nativeSql,
                partitionParameter.getPartitionColumnName(),
                partitionParameter.getPartitionColumnName());
    }

    public static Optional<PartitionParameter> createPartitionParameter(
            JdbcSourceConfig config,
            TableSchema tableSchema,
            JdbcConnectionProvider connectionProvider) {
        Optional<String> partitionColumnOptional = getPartitionColumn(config, tableSchema);
        if (partitionColumnOptional.isPresent()) {
            String partitionColumn = partitionColumnOptional.get();
            SeaTunnelDataType<?> dataType =
                    validationPartitionColumn(partitionColumn, tableSchema.toPhysicalRowDataType());
            return Optional.of(
                    createPartitionParameter(
                            config, partitionColumn, dataType, connectionProvider.getConnection()));
        }
        log.info(
                "The partition_column parameter is not configured, and the source parallelism is set to 1");
        return Optional.empty();
    }

    static PartitionParameter createPartitionParameter(
            JdbcSourceConfig config,
            String columnName,
            SeaTunnelDataType<?> dataType,
            Connection connection) {
        BigDecimal max = null;
        BigDecimal min = null;

        if (dataType.equals(BasicType.STRING_TYPE)) {
            return new PartitionParameter(
                    columnName, dataType, null, null, config.getPartitionNumber().orElse(null));
        }

        if (config.getPartitionLowerBound().isPresent()
                && config.getPartitionUpperBound().isPresent()) {
            max = config.getPartitionUpperBound().get();
            min = config.getPartitionLowerBound().get();
            return new PartitionParameter(
                    columnName, dataType, min, max, config.getPartitionNumber().orElse(null));
        }
        try (ResultSet rs =
                connection
                        .createStatement()
                        .executeQuery(
                                String.format(
                                        "SELECT MAX(%s),MIN(%s) " + "FROM (%s) tt",
                                        columnName, columnName, config.getQuery()))) {
            if (rs.next()) {
                max =
                        config.getPartitionUpperBound().isPresent()
                                ? config.getPartitionUpperBound().get()
                                : rs.getBigDecimal(1);
                min =
                        config.getPartitionLowerBound().isPresent()
                                ? config.getPartitionLowerBound().get()
                                : rs.getBigDecimal(2);
            }
        } catch (SQLException e) {
            throw new PrepareFailException("jdbc", PluginType.SOURCE, e.toString());
        }
        return new PartitionParameter(
                columnName, dataType, min, max, config.getPartitionNumber().orElse(null));
    }

    private static Optional<String> getPartitionColumn(
            JdbcSourceConfig config, TableSchema tableSchema) {
        if (config.getPartitionColumn().isPresent()) {
            return config.getPartitionColumn();
        } else if (tableSchema.getPrimaryKey() != null) {
            PrimaryKey primaryKey = tableSchema.getPrimaryKey();
            return Optional.of(primaryKey.getColumnNames().get(0));
        }
        return Optional.empty();
    }

    static SeaTunnelDataType<?> validationPartitionColumn(
            String partitionColumn, SeaTunnelRowType rowType) {
        Map<String, SeaTunnelDataType<?>> fieldTypes = new HashMap<>();
        for (int i = 0; i < rowType.getFieldNames().length; i++) {
            fieldTypes.put(rowType.getFieldName(i), rowType.getFieldType(i));
        }
        if (!fieldTypes.containsKey(partitionColumn)) {
            throw new JdbcConnectorException(
                    CommonErrorCode.ILLEGAL_ARGUMENT,
                    String.format(
                            "Partitioned column(%s) don't exist in the table columns",
                            partitionColumn));
        }
        SeaTunnelDataType<?> partitionColumnType = fieldTypes.get(partitionColumn);
        if (!isNumericType(partitionColumnType) && !isStringType(partitionColumnType)) {
            throw new JdbcConnectorException(
                    CommonErrorCode.ILLEGAL_ARGUMENT,
                    String.format("%s is not numeric/string type", partitionColumn));
        } else {
            return partitionColumnType;
        }
    }

    private static boolean isNumericType(SeaTunnelDataType<?> type) {
        int scale = 1;
        if (type instanceof DecimalType) {
            scale = ((DecimalType) type).getScale() == 0 ? 0 : ((DecimalType) type).getScale();
            if (scale != 0) {
                throw new JdbcConnectorException(
                        CommonErrorCode.ILLEGAL_ARGUMENT,
                        String.format(
                                "The current field is DecimalType containing decimals: %d Unable to support",
                                scale));
            }
        }
        return type.equals(BasicType.INT_TYPE) || type.equals(BasicType.LONG_TYPE) || scale == 0;
    }

    private static boolean isStringType(SeaTunnelDataType<?> type) {
        return type.equals(BasicType.STRING_TYPE);
    }

    @Override
    public OptionRule optionRule() {
        return OptionRule.builder()
                .required(URL, DRIVER, QUERY)
                .optional(
                        USER,
                        PASSWORD,
                        CONNECTION_CHECK_TIMEOUT_SEC,
                        FETCH_SIZE,
                        PARTITION_COLUMN,
                        PARTITION_UPPER_BOUND,
                        PARTITION_LOWER_BOUND,
                        PARTITION_NUM,
                        COMPATIBLE_MODE)
                .build();
    }

    @Override
    public Class<? extends SeaTunnelSource> getSourceClass() {
        return JdbcSource.class;
    }
}
