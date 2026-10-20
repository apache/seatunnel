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

package org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.ingres;

import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.DatabaseIdentifier;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.GenericDialect;

import java.util.Optional;

/** Quotes Ingres owner/table paths while reusing standard JDBC conversion. */
public class IngresDialect extends GenericDialect {
    public IngresDialect() {}

    public IngresDialect(String fieldIde) {
        super(fieldIde);
    }

    @Override
    public String dialectName() {
        return DatabaseIdentifier.INGRES;
    }

    @Override
    public String quoteIdentifier(String identifier) {
        return quoteDatabaseIdentifier(getFieldIde(identifier, fieldIde));
    }

    @Override
    public String quoteDatabaseIdentifier(String identifier) {
        return "\"" + identifier.replace("\"", "\"\"") + "\"";
    }

    @Override
    public TablePath parse(String tablePath) {
        return TablePath.of(tablePath, true);
    }

    @Override
    public String tableIdentifier(TablePath tablePath) {
        String owner = tablePath.getSchemaName();
        String table = quoteDatabaseIdentifier(tablePath.getTableName());
        return owner == null || owner.isEmpty()
                ? table
                : quoteDatabaseIdentifier(owner) + "." + table;
    }

    @Override
    public String tableIdentifier(String database, String tableName) {
        // The JDBC URL selects the database; tableName supplies the owner and table.
        return tableIdentifier(parse(tableName));
    }

    @Override
    public Optional<String> getUpsertStatement(
            String database, String tableName, String[] fieldNames, String[] pkNames) {
        // Use the existing query/insert/update executor instead of unverified native MERGE SQL.
        return Optional.empty();
    }

    @Override
    public boolean supportHashSplitter() {
        // The generic hash expression is unverified for Ingres; use the single-split fallback.
        return false;
    }
}
