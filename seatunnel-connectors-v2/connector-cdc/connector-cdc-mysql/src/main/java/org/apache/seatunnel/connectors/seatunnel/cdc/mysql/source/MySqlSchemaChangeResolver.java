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

import org.apache.seatunnel.api.table.catalog.TableIdentifier;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.api.table.event.AlterTableColumnEvent;
import org.apache.seatunnel.api.table.event.AlterTableColumnsEvent;
import org.apache.seatunnel.api.table.event.SchemaChangeEvent;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.connectors.cdc.base.config.JdbcSourceConfig;
import org.apache.seatunnel.connectors.cdc.base.config.SourceConfig;
import org.apache.seatunnel.connectors.cdc.base.schema.AbstractSchemaChangeResolver;
import org.apache.seatunnel.connectors.cdc.base.utils.SourceRecordUtils;
import org.apache.seatunnel.connectors.seatunnel.cdc.mysql.source.parser.CustomMySqlAntlrDdlParser;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.DatabaseIdentifier;

import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.connect.source.SourceRecord;

import io.debezium.relational.Tables;

import java.util.List;
import java.util.Objects;

public class MySqlSchemaChangeResolver extends AbstractSchemaChangeResolver {
    private transient Tables tables;
    private transient CustomMySqlAntlrDdlParser customMySqlAntlrDdlParser;

    public MySqlSchemaChangeResolver(SourceConfig.Factory<JdbcSourceConfig> sourceConfigFactory) {
        super(sourceConfigFactory.create(0));
    }

    @Override
    public SchemaChangeEvent resolve(SourceRecord record, SeaTunnelDataType dataType) {
        TablePath tablePath = SourceRecordUtils.getTablePath(record);
        String ddl = SourceRecordUtils.getDdl(record);
        if (Objects.isNull(customMySqlAntlrDdlParser)) {
            this.customMySqlAntlrDdlParser =
                    new CustomMySqlAntlrDdlParser(
                            tablePath, this.jdbcSourceConfig.getDbzConnectorConfig());
        }
        if (Objects.isNull(tables)) {
            this.tables = new Tables();
        }
        customMySqlAntlrDdlParser.setCurrentDatabase(tablePath.getDatabaseName());
        customMySqlAntlrDdlParser.setCurrentSchema(tablePath.getSchemaName());
        // Parse DDL statement using Debezium's Antlr parser
        customMySqlAntlrDdlParser.parse(ddl, tables);
        List<AlterTableColumnEvent> parsedEvents =
                customMySqlAntlrDdlParser.getAndClearParsedEvents();
        parsedEvents.forEach(e -> e.setSourceDialectName(DatabaseIdentifier.MYSQL));
        AlterTableColumnsEvent alterTableColumnsEvent =
                new AlterTableColumnsEvent(
                        TableIdentifier.of(
                                StringUtils.EMPTY,
                                tablePath.getDatabaseName(),
                                tablePath.getSchemaName(),
                                tablePath.getTableName()),
                        parsedEvents);
        alterTableColumnsEvent.setStatement(ddl);
        alterTableColumnsEvent.setSourceDialectName(DatabaseIdentifier.MYSQL);
        return parsedEvents.isEmpty() ? null : alterTableColumnsEvent;
    }
}
