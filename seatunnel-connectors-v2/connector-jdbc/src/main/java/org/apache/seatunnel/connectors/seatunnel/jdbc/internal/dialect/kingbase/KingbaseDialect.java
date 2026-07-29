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

package org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.kingbase;

import org.apache.seatunnel.shade.org.apache.commons.lang3.StringUtils;

import org.apache.seatunnel.api.common.SeaTunnelAPIErrorCode;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.connectors.seatunnel.jdbc.catalog.kingbase.KingbaseCatalog;
import org.apache.seatunnel.connectors.seatunnel.jdbc.exception.JdbcConnectorException;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.converter.JdbcRowConverter;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.DatabaseIdentifier;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.JdbcDialect;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.JdbcDialectTypeMapper;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.dialectenum.FieldIdeEnum;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.mysql.MySqlTypeMapper;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.mysql.MysqlDialect;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.mysql.MysqlJdbcRowConverter;

import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

public class KingbaseDialect implements JdbcDialect {
    /** Kingbase (PostgreSQL-compatible) FILLFACTOR legal range (inclusive): 10-100. */
    private static final int FILLFACTOR_MIN = 10;

    private static final int FILLFACTOR_MAX = 100;

    private static final Set<String> SUPPORTED_TABLE_OPTIONS =
            Collections.unmodifiableSet(
                    new LinkedHashSet<>(
                            Arrays.asList(
                                    KingbaseCatalog.TABLE_OPTION_TABLESPACE,
                                    KingbaseCatalog.TABLE_OPTION_FILLFACTOR)));

    /**
     * Kingbase runtime compatibility mode detected from the JDBC connection or supplied by config.
     */
    private final String compatibleLevel;

    /** Field identifier normalization strategy used when quoting Kingbase identifiers. */
    private final String fieldIde;

    public KingbaseDialect() {
        this(null, FieldIdeEnum.ORIGINAL.getValue());
    }

    public KingbaseDialect(String fieldIde) {
        this(null, fieldIde);
    }

    public KingbaseDialect(String compatibleLevel, String fieldIde) {
        this.compatibleLevel = compatibleLevel;
        this.fieldIde = fieldIde;
    }

    @Override
    public String dialectName() {
        return DatabaseIdentifier.KINGBASE;
    }

    @Override
    public JdbcRowConverter getRowConverter() {
        if (isMySQL()) {
            return new MysqlJdbcRowConverter();
        }
        return new KingbaseJdbcRowConverter();
    }

    @Override
    public JdbcDialectTypeMapper getJdbcDialectTypeMapper() {
        if (isMySQL()) {
            return new MySqlTypeMapper();
        }
        return new KingbaseTypeMapper();
    }

    @Override
    public Optional<String> getUpsertStatement(
            String database, String tableName, String[] fieldNames, String[] pkNames) {
        if (isMySQL()) {
            return new MysqlDialect().getUpsertStatement(database, tableName, fieldNames, pkNames);
        }
        String uniqueColumns =
                Arrays.stream(pkNames).map(this::quoteIdentifier).collect(Collectors.joining(", "));
        String updateClause =
                Arrays.stream(fieldNames)
                        .map(
                                fieldName ->
                                        quoteIdentifier(fieldName)
                                                + "=EXCLUDED."
                                                + quoteIdentifier(fieldName))
                        .collect(Collectors.joining(", "));
        String upsertSQL =
                String.format(
                        "%s ON CONFLICT (%s) DO UPDATE SET %s",
                        getInsertIntoStatement(database, tableName, fieldNames),
                        uniqueColumns,
                        updateClause);
        return Optional.of(upsertSQL);
    }

    @Override
    public TablePath parse(String tablePath) {
        return TablePath.of(tablePath, true);
    }

    @Override
    public String tableIdentifier(String database, String tableName) {
        // resolve pg database name upper or lower not recognised
        return quoteDatabaseIdentifier(database) + "." + quoteIdentifier(tableName);
    }

    @Override
    public String quoteIdentifier(String identifier) {
        if (identifier.contains(".")) {
            String[] parts = identifier.split("\\.");
            StringBuilder sb = new StringBuilder();
            for (int i = 0; i < parts.length - 1; i++) {
                sb.append("\"").append(parts[i]).append("\"").append(".");
            }
            return sb.append("\"")
                    .append(getFieldIde(parts[parts.length - 1], fieldIde))
                    .append("\"")
                    .toString();
        }

        return "\"" + getFieldIde(identifier, fieldIde) + "\"";
    }

    @Override
    public String quoteDatabaseIdentifier(String identifier) {
        return "\"" + identifier + "\"";
    }

    @Override
    public void validateTableOptions(Map<String, String> tableOptions) {
        if (tableOptions == null || tableOptions.isEmpty()) {
            return;
        }

        Set<String> unsupportedOptions = new LinkedHashSet<>(tableOptions.keySet());
        unsupportedOptions.removeAll(SUPPORTED_TABLE_OPTIONS);
        if (!unsupportedOptions.isEmpty()) {
            throw new JdbcConnectorException(
                    SeaTunnelAPIErrorCode.CONFIG_VALIDATION_FAILED,
                    String.format(
                            "Unsupported JDBC table_options for dialect '%s': %s. Supported keys: %s",
                            dialectName(),
                            String.join(", ", unsupportedOptions),
                            String.join(", ", SUPPORTED_TABLE_OPTIONS)));
        }

        for (Map.Entry<String, String> entry : tableOptions.entrySet()) {
            String key = entry.getKey();
            String value = entry.getValue();
            if (StringUtils.isBlank(value)) {
                throw new JdbcConnectorException(
                        SeaTunnelAPIErrorCode.CONFIG_VALIDATION_FAILED,
                        String.format(
                                "Invalid JDBC table_options for dialect '%s': key '%s' must not be blank",
                                dialectName(), key));
            }
            String trimmed = value.trim();
            if (KingbaseCatalog.TABLE_OPTION_FILLFACTOR.equals(key)) {
                validateFillfactor(trimmed);
            } else if (KingbaseCatalog.TABLE_OPTION_TABLESPACE.equals(key)) {
                validateTablespace(trimmed);
            }
        }
    }

    private void validateFillfactor(String value) {
        int fillfactor;
        try {
            fillfactor = Integer.parseInt(value);
        } catch (NumberFormatException e) {
            throw new JdbcConnectorException(
                    SeaTunnelAPIErrorCode.CONFIG_VALIDATION_FAILED,
                    String.format(
                            "Invalid JDBC table_options for dialect '%s': key '%s' must be an integer between %d and %d, but got '%s'",
                            dialectName(),
                            KingbaseCatalog.TABLE_OPTION_FILLFACTOR,
                            FILLFACTOR_MIN,
                            FILLFACTOR_MAX,
                            value));
        }
        if (fillfactor < FILLFACTOR_MIN || fillfactor > FILLFACTOR_MAX) {
            throw new JdbcConnectorException(
                    SeaTunnelAPIErrorCode.CONFIG_VALIDATION_FAILED,
                    String.format(
                            "Invalid JDBC table_options for dialect '%s': key '%s' must be an integer between %d and %d, but got '%s'",
                            dialectName(),
                            KingbaseCatalog.TABLE_OPTION_FILLFACTOR,
                            FILLFACTOR_MIN,
                            FILLFACTOR_MAX,
                            value));
        }
    }

    private void validateTablespace(String value) {
        // Always emitted as TABLESPACE "...", so reject quote / control chars that break DDL.
        if (value.indexOf('"') >= 0
                || value.indexOf('\n') >= 0
                || value.indexOf('\r') >= 0
                || value.indexOf(';') >= 0) {
            throw new JdbcConnectorException(
                    SeaTunnelAPIErrorCode.CONFIG_VALIDATION_FAILED,
                    String.format(
                            "Invalid JDBC table_options for dialect '%s': key '%s' contains illegal characters: '%s'",
                            dialectName(), KingbaseCatalog.TABLE_OPTION_TABLESPACE, value));
        }
    }

    /** Returns whether this Kingbase connection should reuse MySQL JDBC dialect behavior. */
    private boolean isMySQL() {
        return "mysql".equalsIgnoreCase(this.compatibleLevel);
    }
}
