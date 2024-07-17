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

package org.apache.seatunnel.connectors.seatunnel.jdbc.sink;

import org.apache.seatunnel.api.sink.SinkWriter;
import org.apache.seatunnel.api.sink.SupportMultiTableSinkWriter;
import org.apache.seatunnel.api.sink.SupportSchemaEvolutionSinkWriter;
import org.apache.seatunnel.api.table.catalog.Column;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.api.table.schema.event.AlterTableAddColumnEvent;
import org.apache.seatunnel.api.table.schema.event.AlterTableChangeColumnEvent;
import org.apache.seatunnel.api.table.schema.event.AlterTableColumnEvent;
import org.apache.seatunnel.api.table.schema.event.AlterTableColumnsEvent;
import org.apache.seatunnel.api.table.schema.event.AlterTableDropColumnEvent;
import org.apache.seatunnel.api.table.schema.event.AlterTableModifyColumnEvent;
import org.apache.seatunnel.api.table.schema.event.SchemaChangeEvent;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.common.utils.SeaTunnelException;
import org.apache.seatunnel.connectors.seatunnel.jdbc.config.JdbcSinkConfig;
import org.apache.seatunnel.connectors.seatunnel.jdbc.exception.JdbcConnectorErrorCode;
import org.apache.seatunnel.connectors.seatunnel.jdbc.exception.JdbcConnectorException;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.JdbcOutputFormat;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.JdbcOutputFormatBuilder;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.connection.JdbcConnectionProvider;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.JdbcDialect;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.executor.JdbcBatchStatementExecutor;
import org.apache.seatunnel.connectors.seatunnel.jdbc.state.JdbcSinkState;
import org.apache.seatunnel.connectors.seatunnel.jdbc.state.XidInfo;

import org.apache.commons.lang3.StringUtils;

import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.sql.Connection;
import java.util.List;

@Slf4j
public abstract class AbstractJdbcSinkWriter<ResourceT>
        implements SinkWriter<SeaTunnelRow, XidInfo, JdbcSinkState>,
                SupportMultiTableSinkWriter<ResourceT>,
                SupportSchemaEvolutionSinkWriter {

    protected JdbcDialect dialect;
    protected TablePath sinkTablePath;
    protected TableSchema tableSchema;
    protected transient boolean isOpen;
    protected JdbcConnectionProvider connectionProvider;
    protected JdbcSinkConfig jdbcSinkConfig;
    protected JdbcOutputFormat<SeaTunnelRow, JdbcBatchStatementExecutor<SeaTunnelRow>> outputFormat;

    @Override
    public void applySchemaChange(SchemaChangeEvent event) throws IOException {
        if (event instanceof AlterTableColumnsEvent) {
            AlterTableColumnsEvent alterTableColumnsEvent = (AlterTableColumnsEvent) event;
            List<AlterTableColumnEvent> events = alterTableColumnsEvent.getEvents();
            for (AlterTableColumnEvent alterTableColumnEvent : events) {
                String sourceDialectName = alterTableColumnEvent.getSourceDialectName();
                if (StringUtils.isBlank(sourceDialectName)) {
                    throw new SeaTunnelException(
                            "The sourceDialectName in AlterTableColumnEvent can not be empty. event: "
                                    + event);
                }
                processSchemaChangeEvent(alterTableColumnEvent);
            }
        } else {
            log.warn("We only support AlterTableColumnsEvent, but actual event is " + event);
        }
    }

    protected void processSchemaChangeEvent(AlterTableColumnEvent event) throws IOException {
        TableSchema newTableSchema = this.tableSchema.copy();
        List<Column> columns = newTableSchema.getColumns();
        switch (event.getEventType()) {
            case SCHEMA_CHANGE_ADD_COLUMN:
                Column addColumn = ((AlterTableAddColumnEvent) event).getColumn();
                columns.add(addColumn);
                break;
            case SCHEMA_CHANGE_DROP_COLUMN:
                String dropColumn = ((AlterTableDropColumnEvent) event).getColumn();
                columns.removeIf(column -> column.getName().equalsIgnoreCase(dropColumn));
                break;
            case SCHEMA_CHANGE_MODIFY_COLUMN:
                Column modifyColumn = ((AlterTableModifyColumnEvent) event).getColumn();
                replaceColumnByIndex(columns, modifyColumn.getName(), modifyColumn);
                break;
            case SCHEMA_CHANGE_CHANGE_COLUMN:
                AlterTableChangeColumnEvent alterTableChangeColumnEvent =
                        (AlterTableChangeColumnEvent) event;
                Column changeColumn = alterTableChangeColumnEvent.getColumn();
                String oldColumnName = alterTableChangeColumnEvent.getOldColumn();
                replaceColumnByIndex(columns, oldColumnName, changeColumn);
                break;
            default:
                throw new SeaTunnelException(
                        "Unsupported schemaChangeEvent for event type: " + event.getEventType());
        }
        this.tableSchema = newTableSchema;
        reOpenOutputFormat(event);
    }

    protected void reOpenOutputFormat(AlterTableColumnEvent event) throws IOException {
        this.prepareCommit();
        JdbcConnectionProvider refreshTableSchemaConnectionProvider =
                dialect.getJdbcConnectionProvider(jdbcSinkConfig.getJdbcConnectionConfig());
        try (Connection connection =
                refreshTableSchemaConnectionProvider.getOrEstablishConnection()) {
            dialect.applySchemaChange(event, connection, sinkTablePath);
        } catch (Throwable e) {
            throw new JdbcConnectorException(
                    JdbcConnectorErrorCode.REFRESH_PHYSICAL_TABLESCHEMA_BY_SCHEMA_CHANGE_EVENT, e);
        }
        this.outputFormat =
                new JdbcOutputFormatBuilder(
                                dialect, connectionProvider, jdbcSinkConfig, tableSchema)
                        .build();
        this.outputFormat.open();
    }

    protected void replaceColumnByIndex(
            List<Column> columns, String oldColumnName, Column newColumn) {
        for (int i = 0; i < columns.size(); i++) {
            if (columns.get(i).getName().equalsIgnoreCase(oldColumnName)) {
                columns.set(i, newColumn);
            }
        }
    }
}
