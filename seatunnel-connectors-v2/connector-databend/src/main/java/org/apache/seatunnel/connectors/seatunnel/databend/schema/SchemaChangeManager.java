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

package org.apache.seatunnel.connectors.seatunnel.databend.schema;

import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.api.table.converter.BasicTypeDefine;
import org.apache.seatunnel.api.table.schema.event.AlterTableAddColumnEvent;
import org.apache.seatunnel.api.table.schema.event.AlterTableChangeColumnEvent;
import org.apache.seatunnel.api.table.schema.event.AlterTableColumnEvent;
import org.apache.seatunnel.api.table.schema.event.AlterTableColumnsEvent;
import org.apache.seatunnel.api.table.schema.event.AlterTableDropColumnEvent;
import org.apache.seatunnel.api.table.schema.event.AlterTableModifyColumnEvent;
import org.apache.seatunnel.api.table.schema.event.SchemaChangeEvent;
import org.apache.seatunnel.common.utils.SeaTunnelException;
import org.apache.seatunnel.connectors.seatunnel.databend.config.DatabendSinkConfig;
import org.apache.seatunnel.connectors.seatunnel.databend.exception.DatabendConnectorErrorCode;
import org.apache.seatunnel.connectors.seatunnel.databend.exception.DatabendConnectorException;
import org.apache.seatunnel.connectors.seatunnel.databend.util.DatabendTypeConverter;

import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.io.Serializable;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

/** SchemaChangeManager for Databend that implements schema evolution */
@Slf4j
public class SchemaChangeManager implements Serializable {
    private static final long serialVersionUID = 1L;

    private static final String CHECK_COLUMN_EXISTS =
            "SELECT column_name FROM information_schema.columns WHERE table_schema = ? AND table_name = ? AND column_name = ?";

    private final DatabendSinkConfig databendSinkConfig;

    public SchemaChangeManager(DatabendSinkConfig databendSinkConfig) {
        this.databendSinkConfig = databendSinkConfig;
    }

    /** Apply schema change event to Databend table */
    public void applySchemaChange(TablePath tablePath, SchemaChangeEvent event) throws IOException {
        try (Connection connection =
                DriverManager.getConnection(
                        String.format(
                                "%s/%s", databendSinkConfig.getUrl(), tablePath.getDatabaseName()),
                        databendSinkConfig.toProperties())) {
            if (event instanceof AlterTableColumnsEvent) {
                for (AlterTableColumnEvent columnEvent :
                        ((AlterTableColumnsEvent) event).getEvents()) {
                    applySchemaChange(connection, tablePath, columnEvent);
                }
            } else if (event instanceof AlterTableColumnEvent) {
                applySchemaChange(connection, tablePath, (AlterTableColumnEvent) event);
            } else {
                throw new SeaTunnelException(
                        "Unsupported schemaChangeEvent: " + event.getClass().getName());
            }
        } catch (SQLException e) {
            throw new DatabendConnectorException(
                    DatabendConnectorErrorCode.SQL_OPERATION_FAILED,
                    "Failed to apply schema change: " + e.getMessage(),
                    e);
        }
    }

    private void applySchemaChange(
            Connection connection, TablePath tablePath, AlterTableColumnEvent event)
            throws SQLException, IOException {
        if (event instanceof AlterTableChangeColumnEvent) {
            AlterTableChangeColumnEvent changeColumnEvent = (AlterTableChangeColumnEvent) event;
            if (!changeColumnEvent.getOldColumn().equals(changeColumnEvent.getColumn().getName())) {
                if (!columnExists(connection, tablePath, changeColumnEvent.getOldColumn())
                        && columnExists(
                                connection, tablePath, changeColumnEvent.getColumn().getName())) {
                    log.warn(
                            "Column {} already exists in table {}. Skipping change column operation. event: {}",
                            changeColumnEvent.getColumn().getName(),
                            tablePath.getFullName(),
                            event);
                    return;
                }
                applyRenameColumn(connection, tablePath, changeColumnEvent);
            }
        } else if (event instanceof AlterTableModifyColumnEvent) {
            applyModifyColumn(connection, tablePath, (AlterTableModifyColumnEvent) event);
        } else if (event instanceof AlterTableAddColumnEvent) {
            // handle column
            AlterTableAddColumnEvent addColumnEvent = (AlterTableAddColumnEvent) event;
            if (columnExists(connection, tablePath, addColumnEvent.getColumn().getName())) {
                log.warn(
                        "Column {} already exists in table {}. Skipping add column operation. event: {}",
                        addColumnEvent.getColumn().getName(),
                        tablePath.getFullName(),
                        event);
                return;
            }
            applyAddColumn(connection, tablePath, addColumnEvent);
        } else if (event instanceof AlterTableDropColumnEvent) {
            AlterTableDropColumnEvent dropColumnEvent = (AlterTableDropColumnEvent) event;
            if (!columnExists(connection, tablePath, dropColumnEvent.getColumn())) {
                log.warn(
                        "Column {} does not exist in table {}. Skipping drop column operation. event: {}",
                        dropColumnEvent.getColumn(),
                        tablePath.getFullName(),
                        event);
                return;
            }
            applyDropColumn(connection, tablePath, dropColumnEvent);
        } else {
            throw new SeaTunnelException(
                    "Unsupported AlterTableColumnEvent type: " + event.getClass().getName());
        }
    }

    private void applyRenameColumn(
            Connection connection, TablePath tablePath, AlterTableChangeColumnEvent event)
            throws SQLException {
        StringBuilder sqlBuilder =
                new StringBuilder()
                        .append("ALTER TABLE ")
                        .append(quoteIdentifier(tablePath.getFullName()))
                        .append(" RENAME COLUMN ")
                        .append(quoteIdentifier(event.getOldColumn()))
                        .append(" TO ")
                        .append(quoteIdentifier(event.getColumn().getName()));

        String sql = sqlBuilder.toString();
        log.info("Executing SQL for rename column: {}", sql);

        try (Statement statement = connection.createStatement()) {
            statement.execute(sql);
            log.info(
                    "Successfully renamed column from {} to {} in table {}",
                    event.getOldColumn(),
                    event.getColumn().getName(),
                    tablePath.getFullName());
        } catch (SQLException e) {
            log.error("Failed to rename column: {}", sql, e);
            throw e;
        }
    }

    private void applyModifyColumn(
            Connection connection, TablePath tablePath, AlterTableModifyColumnEvent event)
            throws SQLException {
        BasicTypeDefine typeDefine = DatabendTypeConverter.convertToDatabendType(event.getColumn());

        StringBuilder sqlBuilder =
                new StringBuilder()
                        .append("ALTER TABLE ")
                        .append(quoteIdentifier(tablePath.getFullName()))
                        .append(" MODIFY COLUMN ")
                        .append(quoteIdentifier(event.getColumn().getName()))
                        .append(" ")
                        .append(typeDefine.getColumnType());

        if (!event.getColumn().isNullable()) {
            sqlBuilder.append(" NOT NULL");
        }

        if (event.getColumn().getComment() != null) {
            sqlBuilder.append(" COMMENT '").append(event.getColumn().getComment()).append("'");
        }

        String sql = sqlBuilder.toString();
        log.info("Executing SQL for modify column: {}", sql);

        try (Statement statement = connection.createStatement()) {
            statement.execute(sql);
            log.info(
                    "Successfully modified column {} in table {}",
                    event.getColumn().getName(),
                    tablePath.getFullName());
        } catch (SQLException e) {
            log.error("Failed to modify column: {}", sql, e);
            throw e;
        }
    }

    private void applyAddColumn(
            Connection connection, TablePath tablePath, AlterTableAddColumnEvent event)
            throws SQLException {
        // trans SeaTunnel type to Databend
        BasicTypeDefine typeDefine = DatabendTypeConverter.convertToDatabendType(event.getColumn());

        StringBuilder sqlBuilder =
                new StringBuilder()
                        .append("ALTER TABLE ")
                        .append(quoteIdentifier(tablePath.getFullName()))
                        .append(" ADD COLUMN ")
                        .append(quoteIdentifier(event.getColumn().getName()))
                        .append(" ")
                        .append(typeDefine.getColumnType());

        // add nullable
        if (!event.getColumn().isNullable()) {
            sqlBuilder.append(" NOT NULL");
        }

        // add comment
        if (event.getColumn().getComment() != null) {
            sqlBuilder.append(" COMMENT '").append(event.getColumn().getComment()).append("'");
        }

        // add default
        if (event.getColumn().getDefaultValue() != null) {
            sqlBuilder
                    .append(" DEFAULT ")
                    .append(quoteDefaultValue(event.getColumn().getDefaultValue()));
        }

        // after column
        if (event.getAfterColumn() != null) {
            sqlBuilder.append(" AFTER ").append(quoteIdentifier(event.getAfterColumn()));
        }

        String sql = sqlBuilder.toString();
        log.info("Executing SQL for add column: {}", sql);

        try (Statement statement = connection.createStatement()) {
            statement.execute(sql);
            log.info(
                    "Successfully added column {} to table {}",
                    event.getColumn().getName(),
                    tablePath.getFullName());
        } catch (SQLException e) {
            log.error("Failed to add column: {}", sql, e);
            throw e;
        }
    }

    private void applyDropColumn(
            Connection connection, TablePath tablePath, AlterTableDropColumnEvent event)
            throws SQLException {
        String sql =
                String.format(
                        "ALTER TABLE %s DROP COLUMN %s",
                        quoteIdentifier(tablePath.getFullName()),
                        quoteIdentifier(event.getColumn()));

        log.info("Executing SQL for drop column: {}", sql);

        try (Statement statement = connection.createStatement()) {
            statement.execute(sql);
            log.info(
                    "Successfully dropped column {} from table {}",
                    event.getColumn(),
                    tablePath.getFullName());
        } catch (SQLException e) {
            log.error("Failed to drop column: {}", sql, e);
            throw e;
        }
    }

    /** check if column exists in the table */
    private boolean columnExists(Connection connection, TablePath tablePath, String columnName)
            throws SQLException {
        try (PreparedStatement stmt = connection.prepareStatement(CHECK_COLUMN_EXISTS)) {
            stmt.setString(1, tablePath.getDatabaseName());
            stmt.setString(2, tablePath.getTableName());
            stmt.setString(3, columnName);

            try (ResultSet rs = stmt.executeQuery()) {
                return rs.next(); // if result set has any row, column exists
            }
        }
    }

    /** add backticks to identifier */
    private String quoteIdentifier(String identifier) {
        return "`" + identifier + "`";
    }

    /** add single quotes to default value */
    private String quoteDefaultValue(Object defaultValue) {
        String strValue = String.valueOf(defaultValue);

        if (strValue.equalsIgnoreCase("current_timestamp")) {
            return "NOW()"; // Databend use NOW instead of CURRENT_TIMESTAMP
        } else if (strValue.equalsIgnoreCase("null")) {
            return "NULL";
        } else if (strValue.matches("-?\\d+(\\.\\d+)?")) {
            // if the value is a number, return it as is
            return strValue;
        } else {
            // add single quotes for string values
            return "'" + strValue.replace("'", "''") + "'";
        }
    }
}
