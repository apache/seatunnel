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

package org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.psql;

import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.converter.JdbcRowConverter;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.JdbcDialect;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.JdbcDialectTypeMapper;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Optional;
import java.util.stream.Collectors;

public class PostgresDialect implements JdbcDialect {

    public static final int DEFAULT_POSTGRES_FETCH_SIZE = 128;

    @Override
    public String dialectName() {
        return "PostgreSQL";
    }

    @Override
    public JdbcRowConverter getRowConverter() {
        return new PostgresJdbcRowConverter();
    }

    @Override
    public JdbcDialectTypeMapper getJdbcDialectTypeMapper() {
        return new PostgresTypeMapper();
    }

    @Override
    public Optional<String> getUpsertStatement(String tableName, String[] fieldNames, String[] uniqueKeyFields) {
        String uniqueColumns = Arrays.stream(uniqueKeyFields)
            .map(this::quoteIdentifier)
            .collect(Collectors.joining(", "));
        String updateClause = Arrays.stream(fieldNames)
            .map(fieldName -> quoteIdentifier(fieldName) + "=EXCLUDED." + quoteIdentifier(fieldName))
            .collect(Collectors.joining(", "));
        String upsertSQL = String.format("%s ON CONFLICT (%s) DO UPDATE SET %s",
            getInsertIntoStatement(tableName, fieldNames), uniqueColumns, updateClause);
        return Optional.of(upsertSQL);
    }

    @Override
    public PreparedStatement creatPreparedStatement(Connection connection, String queryTemplate, int fetchSize) throws SQLException {
        // use cursor mode, reference: https://jdbc.postgresql.org/documentation/query/#getting-results-based-on-a-cursor
        connection.setAutoCommit(false);
        PreparedStatement statement = connection.prepareStatement(queryTemplate, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
        if (fetchSize > 0) {
            statement.setFetchSize(fetchSize);
        } else {
            statement.setFetchSize(DEFAULT_POSTGRES_FETCH_SIZE);
        }
        return statement;
    }
}
