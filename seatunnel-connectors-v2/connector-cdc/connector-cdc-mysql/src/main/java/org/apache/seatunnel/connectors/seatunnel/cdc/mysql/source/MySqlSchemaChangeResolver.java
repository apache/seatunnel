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

package org.apache.seatunnel.connectors.seatunnel.cdc.mysql.source;

import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.api.table.operation.event.TableOperationEvent;
import org.apache.seatunnel.api.table.operation.event.TruncateTableEvent;
import org.apache.seatunnel.api.table.schema.event.AlterTableColumnEvent;
import org.apache.seatunnel.api.table.schema.event.AlterTableEvent;
import org.apache.seatunnel.connectors.cdc.base.config.JdbcSourceConfig;
import org.apache.seatunnel.connectors.cdc.base.config.SourceConfig;
import org.apache.seatunnel.connectors.cdc.base.schema.AbstractSchemaChangeResolver;
import org.apache.seatunnel.connectors.cdc.base.utils.DdlStatements;
import org.apache.seatunnel.connectors.cdc.base.utils.SourceRecordUtils;
import org.apache.seatunnel.connectors.seatunnel.cdc.mysql.source.parser.CustomMySqlAntlrDdlParser;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.DatabaseIdentifier;

import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;

import io.debezium.connector.AbstractSourceInfo;
import io.debezium.data.Envelope;
import io.debezium.relational.Tables;
import io.debezium.relational.ddl.DdlParser;

import java.util.List;
import java.util.Objects;

public class MySqlSchemaChangeResolver extends AbstractSchemaChangeResolver {

    public MySqlSchemaChangeResolver(SourceConfig.Factory<JdbcSourceConfig> sourceConfigFactory) {
        this(sourceConfigFactory, false);
    }

    public MySqlSchemaChangeResolver(
            SourceConfig.Factory<JdbcSourceConfig> sourceConfigFactory,
            boolean tableOperationsEnabled) {
        this(sourceConfigFactory, tableOperationsEnabled, true);
    }

    public MySqlSchemaChangeResolver(
            SourceConfig.Factory<JdbcSourceConfig> sourceConfigFactory,
            boolean tableOperationsEnabled,
            boolean schemaChangesEnabled) {
        super(sourceConfigFactory.create(0));
        setTableOperationsEnabled(tableOperationsEnabled);
        setSchemaChangesEnabled(schemaChangesEnabled);
    }

    @Override
    public TableOperationEvent resolveTableOperation(
            SourceRecord record, List<CatalogTable> catalogTables) {
        if (!tableOperationsEnabled) {
            return null;
        }
        String ddl = SourceRecordUtils.getDdl(record);
        if (!DdlStatements.isTruncateTable(ddl)) {
            return null;
        }
        if (Objects.isNull(ddlParser)) {
            this.ddlParser = createDdlParser(TablePath.DEFAULT);
        }
        if (Objects.isNull(tables)) {
            this.tables = new Tables();
        }
        Struct value = (Struct) record.value();
        Struct source = value.getStruct(Envelope.FieldName.SOURCE);
        if (source != null && source.schema().field(AbstractSourceInfo.DATABASE_NAME_KEY) != null) {
            String databaseName = source.getString(AbstractSourceInfo.DATABASE_NAME_KEY);
            if (databaseName != null) {
                ddlParser.setCurrentDatabase(databaseName);
            }
        }
        ddlParser.parse(ddl, tables);
        CustomMySqlAntlrDdlParser mysqlParser = (CustomMySqlAntlrDdlParser) ddlParser;
        // Drop any accidental ALTER events so they cannot leak into a later schema resolve.
        mysqlParser.getAndClearParsedEvents();
        List<TruncateTableEvent> operations = mysqlParser.getAndClearParsedTableOperations();
        if (operations.isEmpty()) {
            return null;
        }
        TruncateTableEvent event = operations.get(0);
        if (!isCapturedTable(event.tablePath(), catalogTables)) {
            return null;
        }
        event.setStatement(ddl);
        event.setSourceDialectName(getSourceDialectName());
        return event;
    }

    private static boolean isCapturedTable(TablePath path, List<CatalogTable> catalogTables) {
        if (catalogTables == null || catalogTables.isEmpty() || path == null) {
            return false;
        }
        return catalogTables.stream()
                .anyMatch(
                        table -> {
                            TablePath captured = table.getTablePath();
                            boolean tableMatch =
                                    captured.getTableName().equalsIgnoreCase(path.getTableName());
                            return tableMatch && databaseMatches(captured, path);
                        });
    }

    /**
     * When the captured table has a database, the event must name the same database. A missing
     * event database is not a wildcard: it would otherwise match every same-named table across
     * captured databases.
     */
    private static boolean databaseMatches(TablePath captured, TablePath path) {
        if (!hasText(captured.getDatabaseName())) {
            return true;
        }
        if (!hasText(path.getDatabaseName())) {
            return false;
        }
        return captured.getDatabaseName().equalsIgnoreCase(path.getDatabaseName());
    }

    private static boolean hasText(String value) {
        return value != null && !value.isEmpty();
    }

    @Override
    protected DdlParser createDdlParser(TablePath tablePath) {
        return new CustomMySqlAntlrDdlParser(this.jdbcSourceConfig.getDbzConnectorConfig());
    }

    @Override
    protected List<AlterTableEvent> getAndClearParsedSchemaChangeEvents() {
        return ((CustomMySqlAntlrDdlParser) ddlParser).getAndClearParsedEvents();
    }

    @Override
    protected List<AlterTableColumnEvent> getAndClearParsedEvents() {
        return ((CustomMySqlAntlrDdlParser) ddlParser).getAndClearParsedColumnEvents();
    }

    @Override
    protected String getSourceDialectName() {
        return DatabaseIdentifier.MYSQL;
    }
}
