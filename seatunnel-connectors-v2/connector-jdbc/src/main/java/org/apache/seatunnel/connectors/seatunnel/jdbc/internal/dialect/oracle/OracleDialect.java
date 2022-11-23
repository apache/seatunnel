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

package org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.oracle;

import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.converter.JdbcRowConverter;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.JdbcDialect;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.JdbcDialectTypeMapper;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

public class OracleDialect implements JdbcDialect {

    private static final int DEFAULT_ORACLE_FETCH_SIZE = 128;

    @Override
    public String dialectName() {
        return "Oracle";
    }

    @Override
    public JdbcRowConverter getRowConverter() {
        return new OracleJdbcRowConverter();
    }

    @Override
    public JdbcDialectTypeMapper getJdbcDialectTypeMapper() {
        return new OracleTypeMapper();
    }

    @Override
    public Optional<String> getUpsertStatement(String tableName, String[] fieldNames, String[] uniqueKeyFields) {
        List<String> nonUniqueKeyFields = Arrays.stream(fieldNames)
            .filter(fieldName -> !Arrays.asList(uniqueKeyFields).contains(fieldName))
            .collect(Collectors.toList());
        String valuesBinding = Arrays.stream(fieldNames)
            .map(fieldName -> "? " + quoteIdentifier(fieldName))
            .collect(Collectors.joining(", "));

        String usingClause = String.format("SELECT %s FROM DUAL", valuesBinding);
        String onConditions = Arrays.stream(uniqueKeyFields)
            .map(fieldName -> String.format(
                "TARGET.%s=SOURCE.%s", quoteIdentifier(fieldName), quoteIdentifier(fieldName)))
            .collect(Collectors.joining(" AND "));
        String updateSetClause = nonUniqueKeyFields.stream()
            .map(fieldName -> String.format(
                "TARGET.%s=SOURCE.%s", quoteIdentifier(fieldName), quoteIdentifier(fieldName)))
            .collect(Collectors.joining(", "));
        String insertFields = Arrays.stream(fieldNames)
            .map(this::quoteIdentifier)
            .collect(Collectors.joining(", "));
        String insertValues = Arrays.stream(fieldNames)
            .map(fieldName -> "SOURCE." + quoteIdentifier(fieldName))
            .collect(Collectors.joining(", "));

        String upsertSQL = String.format(
            " MERGE INTO %s TARGET"
                + " USING (%s) SOURCE"
                + " ON (%s) "
                + " WHEN MATCHED THEN"
                + " UPDATE SET %s"
                + " WHEN NOT MATCHED THEN"
                + " INSERT (%s) VALUES (%s)",
            tableName,
            usingClause,
            onConditions,
            updateSetClause,
            insertFields,
            insertValues);

        return Optional.of(upsertSQL);
    }

    @Override
    public PreparedStatement creatPreparedStatement(Connection connection, String queryTemplate, int fetchSize) throws SQLException {
        PreparedStatement statement = connection.prepareStatement(queryTemplate, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
        if (fetchSize > 0) {
            statement.setFetchSize(fetchSize);
        } else {
            statement.setFetchSize(DEFAULT_ORACLE_FETCH_SIZE);
        }
        return statement;
    }
}
