/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.seatunnel.connectors.seatunnel.starrocks.util;

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
import org.apache.seatunnel.connectors.seatunnel.starrocks.datatypes.StarRocksType;
import org.apache.seatunnel.connectors.seatunnel.starrocks.datatypes.StarRocksTypeConverter;

import org.apache.maven.artifact.versioning.ComparableVersion;

import lombok.extern.slf4j.Slf4j;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

@Slf4j
public class SchemaUtils {

    private static final String MIN_VERSION_TABLE_CHANGE_COLUMN = "3.3.2";

    private SchemaUtils() {}

    /**
     * Refresh physical table schema by schema change event
     *
     * @param event schema change event
     * @param connection jdbc connection
     * @param tablePath sink table path
     */
    public static void applySchemaChange(
            SchemaChangeEvent event, Connection connection, TablePath tablePath)
            throws SQLException {
        if (event instanceof AlterTableColumnsEvent) {
            for (AlterTableColumnEvent columnEvent : ((AlterTableColumnsEvent) event).getEvents()) {
                applySchemaChange(columnEvent, connection, tablePath);
            }
        } else {
            if (event instanceof AlterTableChangeColumnEvent) {
                AlterTableChangeColumnEvent changeColumnEvent = (AlterTableChangeColumnEvent) event;
                if (!changeColumnEvent
                        .getOldColumn()
                        .equals(changeColumnEvent.getColumn().getName())) {
                    if (!columnExists(connection, tablePath, changeColumnEvent.getOldColumn())
                            && columnExists(
                                    connection,
                                    tablePath,
                                    changeColumnEvent.getColumn().getName())) {
                        log.warn(
                                "Column {} already exists in table {}. Skipping change column operation. event: {}",
                                changeColumnEvent.getColumn().getName(),
                                tablePath.getFullName(),
                                event);
                        return;
                    }
                }
                applySchemaChange(connection, tablePath, changeColumnEvent);
            } else if (event instanceof AlterTableModifyColumnEvent) {
                applySchemaChange(connection, tablePath, (AlterTableModifyColumnEvent) event);
            } else if (event instanceof AlterTableAddColumnEvent) {
                AlterTableAddColumnEvent addColumnEvent = (AlterTableAddColumnEvent) event;
                if (columnExists(connection, tablePath, addColumnEvent.getColumn().getName())) {
                    log.warn(
                            "Column {} already exists in table {}. Skipping add column operation. event: {}",
                            addColumnEvent.getColumn().getName(),
                            tablePath.getFullName(),
                            event);
                    return;
                }
                applySchemaChange(connection, tablePath, addColumnEvent);
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
                applySchemaChange(connection, tablePath, dropColumnEvent);
            } else {
                throw new SeaTunnelException(
                        "Unsupported schemaChangeEvent : " + event.getEventType());
            }
        }
    }

    public static void applySchemaChange(
            Connection connection, TablePath tablePath, AlterTableChangeColumnEvent event)
            throws SQLException {
        ComparableVersion targetVersion = new ComparableVersion(MIN_VERSION_TABLE_CHANGE_COLUMN);
        ComparableVersion currentVersion;
        try (Statement statement = connection.createStatement();
                ResultSet resultSet =
                        statement.executeQuery("SELECT CURRENT_VERSION() as version")) {
            resultSet.next();
            String version = resultSet.getString(1);
            log.debug("starrocks version: {}", version);
            String versionOne = version.split(" ")[0];
            currentVersion = new ComparableVersion(versionOne);
        }

        if (currentVersion.compareTo(targetVersion) >= 0) {
            StringBuilder sqlBuilder =
                    new StringBuilder()
                            .append("ALTER TABLE")
                            .append(" ")
                            .append(tablePath.getFullName())
                            .append(" ")
                            .append("RENAME COLUMN")
                            .append(" ")
                            .append(quoteIdentifier(event.getOldColumn()))
                            .append(" TO ")
                            .append(quoteIdentifier(event.getColumn().getName()));
            if (event.getColumn().getComment() != null) {
                sqlBuilder
                        .append(" ")
                        .append("COMMENT ")
                        .append("'")
                        .append(event.getColumn().getComment())
                        .append("'");
            }
            if (event.getAfterColumn() != null) {
                sqlBuilder
                        .append(" ")
                        .append("AFTER ")
                        .append(quoteIdentifier(event.getAfterColumn()));
            }

            String changeColumnSQL = sqlBuilder.toString();
            try (Statement statement = connection.createStatement()) {
                log.info("Executing change column SQL: " + changeColumnSQL);
                statement.execute(changeColumnSQL);
            }
        } else {
            log.warn("versions prior to starrocks 3.3.2 do not support rename column operations");
        }
    }

    public static void applySchemaChange(
            Connection connection, TablePath tablePath, AlterTableModifyColumnEvent event)
            throws SQLException {
        BasicTypeDefine<StarRocksType> typeDefine =
                StarRocksTypeConverter.INSTANCE.reconvert(event.getColumn());
        StringBuilder sqlBuilder =
                new StringBuilder()
                        .append("ALTER TABLE")
                        .append(" ")
                        .append(tablePath.getFullName())
                        .append(" ")
                        .append("MODIFY COLUMN")
                        .append(" ")
                        .append(quoteIdentifier(event.getColumn().getName()))
                        .append(" ")
                        .append(typeDefine.getColumnType());
        if (event.getColumn().getComment() != null) {
            sqlBuilder
                    .append(" ")
                    .append("COMMENT ")
                    .append("'")
                    .append(event.getColumn().getComment())
                    .append("'");
        }
        if (event.getAfterColumn() != null) {
            sqlBuilder.append(" ").append("AFTER ").append(quoteIdentifier(event.getAfterColumn()));
        }

        String modifyColumnSQL = sqlBuilder.toString();
        try (Statement statement = connection.createStatement()) {
            log.info("Executing modify column SQL: " + modifyColumnSQL);
            statement.execute(modifyColumnSQL);
        }
    }

    public static void applySchemaChange(
            Connection connection, TablePath tablePath, AlterTableAddColumnEvent event)
            throws SQLException {
        BasicTypeDefine<StarRocksType> typeDefine =
                StarRocksTypeConverter.INSTANCE.reconvert(event.getColumn());
        StringBuilder sqlBuilder =
                new StringBuilder()
                        .append("ALTER TABLE")
                        .append(" ")
                        .append(tablePath.getFullName())
                        .append(" ")
                        .append("ADD COLUMN")
                        .append(" ")
                        .append(quoteIdentifier(event.getColumn().getName()))
                        .append(" ")
                        .append(typeDefine.getColumnType());
        if (event.getColumn().getComment() != null) {
            sqlBuilder
                    .append(" ")
                    .append("COMMENT ")
                    .append("'")
                    .append(event.getColumn().getComment())
                    .append("'");
        }
        if (event.getAfterColumn() != null) {
            sqlBuilder.append(" ").append("AFTER ").append(quoteIdentifier(event.getAfterColumn()));
        }

        String addColumnSQL = sqlBuilder.toString();
        try (Statement statement = connection.createStatement()) {
            log.info("Executing add column SQL: " + addColumnSQL);
            statement.execute(addColumnSQL);
        }
    }

    public static void applySchemaChange(
            Connection connection, TablePath tablePath, AlterTableDropColumnEvent event)
            throws SQLException {
        String dropColumnSQL =
                String.format(
                        "ALTER TABLE %s DROP COLUMN %s",
                        tablePath.getFullName(), quoteIdentifier(event.getColumn()));
        try (Statement statement = connection.createStatement()) {
            log.info("Executing drop column SQL: {}", dropColumnSQL);
            statement.execute(dropColumnSQL);
        }
    }

    /**
     * Check if the column exists in the table
     *
     * @param connection
     * @param tablePath
     * @param column
     * @return
     */
    public static boolean columnExists(Connection connection, TablePath tablePath, String column) {
        String selectColumnSQL =
                String.format(
                        "SELECT %s FROM %s WHERE 1 != 1",
                        quoteIdentifier(column), tablePath.getFullName());
        try (Statement statement = connection.createStatement()) {
            return statement.execute(selectColumnSQL);
        } catch (SQLException e) {
            log.debug("Column {} does not exist in table {}", column, tablePath.getFullName(), e);
            return false;
        }
    }

    public static String quoteIdentifier(String identifier) {
        return "`" + identifier + "`";
    }
}
