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

package org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.duckdb;

import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.converter.JdbcRowConverter;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.DatabaseIdentifier;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.JdbcDialect;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.JdbcDialectTypeMapper;

import lombok.extern.slf4j.Slf4j;

import java.util.Optional;

@Slf4j
public class DuckDBDialect implements JdbcDialect {

    private static final String DEFAULT_DATABASE_NAME = "default";
    private static final String DEFAULT_SCHEMA_NAME = "main";

    @Override
    public String dialectName() {
        return DatabaseIdentifier.DUCKDB;
    }

    @Override
    public JdbcRowConverter getRowConverter() {
        return new DuckDBJdbcRowConverter();
    }

    @Override
    public String hashModForField(String fieldName, int mod) {
        return String.format("MOD(ABS(HASH(%s)), %d)", quoteIdentifier(fieldName), mod);
    }

    @Override
    public JdbcDialectTypeMapper getJdbcDialectTypeMapper() {
        return new DuckDBTypeMapper();
    }

    @Override
    public TablePath parse(String tablePath) {
        final String[] split = tablePath.split("\\.");
        if (split.length == 2) {
            return TablePath.of(DEFAULT_DATABASE_NAME, split[0], split[1]);
        } else if (split.length == 1) {
            return TablePath.of(DEFAULT_DATABASE_NAME, DEFAULT_SCHEMA_NAME, split[0]);
        }
        return TablePath.of(tablePath);
    }

    @Override
    public String quoteIdentifier(String identifier) {
        return String.format("\"%s\"", identifier);
    }

    @Override
    public String tableIdentifier(String database, String tableName) {
        return tableIdentifier(TablePath.of(database + "." + tableName));
    }

    /**
     * Returns an UPSERT statement for the target table.
     *
     * <p>This connector intentionally does not support UPSERT semantics. SeaTunnel is optimized for
     * batch-oriented ETL workloads and append-based writes. Row-level UPSERT operations may cause
     * significant performance degradation on analytical storage engines and are therefore not
     * provided.
     *
     * @param database the target database name
     * @param tableName the target table name
     * @param fieldNames all column names of the target table
     * @param uniqueKeyFields unique key columns for UPSERT
     * @return an empty Optional to indicate that UPSERT is not supported
     */
    @Override
    public Optional<String> getUpsertStatement(
            String database, String tableName, String[] fieldNames, String[] uniqueKeyFields) {
        return Optional.empty();
    }

    @Override
    public String tableIdentifier(TablePath tablePath) {
        String schemaName = tablePath.getSchemaName();
        if (schemaName == null || schemaName.trim().isEmpty()) {
            schemaName = "main";
        }
        return String.format("\"%s\".\"%s\"", schemaName, tablePath.getTableName());
    }
}
