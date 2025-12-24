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

package org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect;

import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.converter.AbstractJdbcRowConverter;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.converter.JdbcRowConverter;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.dialectenum.FieldIdeEnum;

import lombok.extern.slf4j.Slf4j;

import java.util.Optional;

@Slf4j
public class GenericDialect implements JdbcDialect {

    public String fieldIde = FieldIdeEnum.ORIGINAL.getValue();

    public GenericDialect() {}

    public GenericDialect(String fieldIde) {
        this.fieldIde = fieldIde;
    }

    @Override
    public String dialectName() {
        return DatabaseIdentifier.GENERIC;
    }

    @Override
    public JdbcRowConverter getRowConverter() {
        return new AbstractJdbcRowConverter() {
            @Override
            public String converterName() {
                return DatabaseIdentifier.GENERIC;
            }
        };
    }

    @Override
    public JdbcDialectTypeMapper getJdbcDialectTypeMapper() {
        return new GenericTypeMapper();
    }

    @Override
    public String quoteIdentifier(String identifier) {
        return getFieldIde(identifier, fieldIde);
    }

    @Override
    public String quoteDatabaseIdentifier(String identifier) {
        return identifier;
    }

    @Override
    public String tableIdentifier(TablePath tablePath) {
        return tableIdentifier(tablePath.getDatabaseName(), tablePath.getTableName());
    }

    @Override
    public Optional<String> getUpsertStatement(
            String database, String tableName, String[] fieldNames, String[] uniqueKeyFields) {
        throw new UnsupportedOperationException();
    }

    @Override
    public TablePath parse(String tablePath) {
        return TablePath.of(tablePath, false);
    }
}
