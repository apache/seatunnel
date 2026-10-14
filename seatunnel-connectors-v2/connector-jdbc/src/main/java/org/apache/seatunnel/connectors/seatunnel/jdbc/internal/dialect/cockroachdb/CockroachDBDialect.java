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

package org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.cockroachdb;

import org.apache.seatunnel.shade.org.apache.commons.lang3.StringUtils;

import org.apache.seatunnel.api.common.SeaTunnelAPIErrorCode;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.connectors.seatunnel.jdbc.exception.JdbcConnectorException;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.converter.JdbcRowConverter;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.DatabaseIdentifier;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.SQLUtils;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.psql.PostgresDialect;
import org.apache.seatunnel.connectors.seatunnel.jdbc.source.JdbcSourceTable;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Map;
import java.util.stream.Collectors;

/** PostgreSQL-wire-compatible reads and writes against pre-created CockroachDB tables. */
public class CockroachDBDialect extends PostgresDialect {

    public CockroachDBDialect() {}

    public CockroachDBDialect(String fieldIde) {
        super(fieldIde);
    }

    @Override
    public String dialectName() {
        return DatabaseIdentifier.COCKROACHDB;
    }

    @Override
    public JdbcRowConverter getRowConverter() {
        return new CockroachDBRowConverter();
    }

    @Override
    public String quoteIdentifier(String identifier) {
        String[] parts = identifier.split("\\.");
        parts[parts.length - 1] = getFieldIde(parts[parts.length - 1], fieldIde);
        return Arrays.stream(parts)
                .map(this::quoteDatabaseIdentifier)
                .collect(Collectors.joining("."));
    }

    @Override
    public String quoteDatabaseIdentifier(String identifier) {
        return "\"" + identifier.replace("\"", "\"\"") + "\"";
    }

    @Override
    public String tableIdentifier(TablePath tablePath) {
        return java.util.stream.Stream.of(
                        tablePath.getDatabaseName(),
                        tablePath.getSchemaName(),
                        tablePath.getTableName())
                .filter(StringUtils::isNotEmpty)
                .map(this::quoteDatabaseIdentifier)
                .collect(Collectors.joining("."));
    }

    @Override
    public String hashModForField(String nativeType, String fieldName, int mod) {
        // HASHTEXT is not available in CockroachDB. FNV32 returns a non-negative INT8, and
        // casting also supports UUID partition keys exposed as STRING by the type mapper.
        // Both fixed and dynamic hash splitters use equality predicates; route NULL to one
        // bucket as well, otherwise nullable partition keys would disappear from the source.
        return "(COALESCE(FNV32(CAST("
                + quoteIdentifier(fieldName)
                + " AS STRING)), 0) % "
                + mod
                + ")";
    }

    @Override
    public Long approximateRowCntStatement(Connection connection, JdbcSourceTable table)
            throws SQLException {
        // CockroachDB exposes pg_class.reltuples as NULL. Treating it as zero would make
        // source splitting decisions from an empty-table estimate even for populated tables.
        if (StringUtils.isNotBlank(table.getQuery())) {
            return SQLUtils.countForSubquery(connection, table.getQuery());
        }
        return SQLUtils.countForTable(connection, tableIdentifier(table.getTablePath()));
    }

    @Override
    public void validateTableOptions(Map<String, String> tableOptions) {
        if (tableOptions != null && !tableOptions.isEmpty()) {
            throw new JdbcConnectorException(
                    SeaTunnelAPIErrorCode.CONFIG_VALIDATION_FAILED,
                    "CockroachDB requires pre-created tables and does not support table_options");
        }
    }
}
