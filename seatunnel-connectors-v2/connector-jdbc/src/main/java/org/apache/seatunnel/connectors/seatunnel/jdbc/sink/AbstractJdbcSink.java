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
import org.apache.seatunnel.api.table.catalog.Column;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.api.table.event.AlterTableAddColumnEvent;
import org.apache.seatunnel.api.table.event.AlterTableChangeColumnEvent;
import org.apache.seatunnel.api.table.event.AlterTableColumnEvent;
import org.apache.seatunnel.api.table.event.AlterTableColumnsEvent;
import org.apache.seatunnel.api.table.event.AlterTableDropColumnEvent;
import org.apache.seatunnel.api.table.event.AlterTableModifyColumnEvent;
import org.apache.seatunnel.api.table.event.SchemaChangeEvent;
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

import java.io.IOException;
import java.util.List;

public abstract class AbstractJdbcSink implements SinkWriter<SeaTunnelRow, XidInfo, JdbcSinkState> {

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
            List<AlterTableColumnEvent> events = ((AlterTableColumnsEvent) event).getEvents();
            for (AlterTableColumnEvent alterTableColumnEvent : events) {
                processSchemaChangeEvent(alterTableColumnEvent);
            }
        } else {
            this.processSchemaChangeEvent(event);
        }
    }

    protected void processSchemaChangeEvent(SchemaChangeEvent event) throws IOException {
        // apply columns change
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
        // refresh table schema in seatunnel
        this.tableSchema = newTableSchema;
        reOpenOutputFormat(event);
    }

    protected void reOpenOutputFormat(SchemaChangeEvent event) throws IOException {
        JdbcOutputFormat<SeaTunnelRow, JdbcBatchStatementExecutor<SeaTunnelRow>> oldOutputFormat =
                this.outputFormat;
        try {
            flushCommit();
            try {
                dialect.refreshPhysicalTableSchemaBySchemaChangeEvent(
                        event, connectionProvider, sinkTablePath);
            } catch (Throwable e) {
                throw new JdbcConnectorException(
                        JdbcConnectorErrorCode.REFRESH_PHYSICAL_TABLESCHEMA_BY_SCHEMA_CHANGE_EVENT,
                        e);
            }
            isOpen = false;
            this.outputFormat =
                    new JdbcOutputFormatBuilder(
                                    dialect, connectionProvider, jdbcSinkConfig, tableSchema)
                            .build();
        } finally {
            oldOutputFormat.close();
        }
    }

    protected abstract void flushCommit() throws IOException;

    protected void replaceColumnByIndex(
            List<Column> columns, String oldColumnName, Column newColumn) {
        for (int i = 0; i < columns.size(); i++) {
            if (columns.get(i).getName().equalsIgnoreCase(oldColumnName)) {
                columns.set(i, newColumn);
            }
        }
    }
}
