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

package org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.sqlite;

import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.converter.JdbcRowConverter;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.JdbcDialect;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.JdbcDialectTypeMapper;

import java.util.Arrays;
import java.util.Optional;
import java.util.stream.Collectors;

public class SqliteDialect implements JdbcDialect {
    @Override
    public String dialectName() {
        return "Sqlite";
    }

    @Override
    public JdbcRowConverter getRowConverter() {
        return new SqliteJdbcRowConverter();
    }

    @Override
    public JdbcDialectTypeMapper getJdbcDialectTypeMapper() {
        return new SqliteTypeMapper();
    }

    @Override
    public String quoteIdentifier(String identifier) {
        return "`" + identifier + "`";
    }

    @Override
    public Optional<String> getUpsertStatement(String tableName, String[] fieldNames, String[] uniqueKeyFields) {
        String updateClause = Arrays.stream(fieldNames)
                .map(fieldName -> quoteIdentifier(fieldName) + "=VALUES(" + quoteIdentifier(fieldName) + ")")
                .collect(Collectors.joining(", "));

        String conflictFields = Arrays.stream(uniqueKeyFields)
                .map(this::quoteIdentifier)
                .collect(Collectors.joining(","));

        String upsertSQL = getInsertIntoStatement(tableName, fieldNames) + " ON CONFLICT(" + conflictFields + ") DO UPDATE SET " + updateClause;
        return Optional.of(upsertSQL);
    }
}
