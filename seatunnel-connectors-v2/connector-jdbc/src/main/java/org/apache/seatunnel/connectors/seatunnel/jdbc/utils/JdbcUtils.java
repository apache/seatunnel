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

package org.apache.seatunnel.connectors.seatunnel.jdbc.utils;

import org.apache.seatunnel.api.table.catalog.Column;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.api.table.converter.BasicTypeDefine;
import org.apache.seatunnel.api.table.converter.ConverterLoader;
import org.apache.seatunnel.api.table.converter.TypeConverter;
import org.apache.seatunnel.api.table.event.AlterTableAddColumnEvent;
import org.apache.seatunnel.api.table.event.AlterTableChangeColumnEvent;
import org.apache.seatunnel.api.table.event.AlterTableDropColumnEvent;
import org.apache.seatunnel.api.table.event.AlterTableModifyColumnEvent;
import org.apache.seatunnel.api.table.event.SchemaChangeEvent;
import org.apache.seatunnel.common.utils.SeaTunnelException;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.DatabaseIdentifier;

import org.apache.commons.lang3.StringUtils;

import com.mysql.cj.MysqlType;

public class JdbcUtils {

    /**
     * generate alter table sql
     *
     * @param event schema change event
     * @return
     */
    public static String generateAlterTableSql(SchemaChangeEvent event, TablePath sinkTablePath) {
        switch (event.getEventType()) {
            case SCHEMA_CHANGE_ADD_COLUMN:
                Column addColumn = ((AlterTableAddColumnEvent) event).getColumn();
                return generateAlterTableSql(
                        AlterType.ADD.getTypeQuoteBySpace(),
                        addColumn,
                        sinkTablePath.getFullNameWithQuoted(),
                        StringUtils.EMPTY);
            case SCHEMA_CHANGE_DROP_COLUMN:
                String dropColumn = ((AlterTableDropColumnEvent) event).getColumn();
                return String.format(
                        "ALTER TABLE %s drop column %s",
                        sinkTablePath.getFullNameWithQuoted(), dropColumn);
            case SCHEMA_CHANGE_MODIFY_COLUMN:
                Column modifyColumn = ((AlterTableModifyColumnEvent) event).getColumn();
                return generateAlterTableSql(
                        AlterType.MODIFY.getTypeQuoteBySpace(),
                        modifyColumn,
                        sinkTablePath.getFullNameWithQuoted(),
                        StringUtils.EMPTY);
            case SCHEMA_CHANGE_CHANGE_COLUMN:
                AlterTableChangeColumnEvent alterTableChangeColumnEvent =
                        (AlterTableChangeColumnEvent) event;
                Column changeColumn = alterTableChangeColumnEvent.getColumn();
                String oldColumnName = alterTableChangeColumnEvent.getOldColumn();
                return generateAlterTableSql(
                        AlterType.CHANGE.getTypeQuoteBySpace(),
                        changeColumn,
                        sinkTablePath.getFullNameWithQuoted(),
                        oldColumnName);
            default:
                throw new SeaTunnelException(
                        "Unsupported schemaChangeEvent for event type: " + event.getEventType());
        }
    }

    public static String generateAlterTableSql(
            String alterOperation, Column newColumn, String tableName, String oldColumnName) {
        StringBuilder sql = new StringBuilder("ALTER TABLE " + tableName + alterOperation);
        TypeConverter<?> typeConverter =
                ConverterLoader.loadTypeConverter(DatabaseIdentifier.MYSQL);
        BasicTypeDefine<MysqlType> mysqlTypeBasicTypeDefine =
                (BasicTypeDefine<MysqlType>) typeConverter.reconvert(newColumn);
        if (alterOperation.trim().equals(AlterType.CHANGE)) {
            sql.append(oldColumnName)
                    .append(StringUtils.SPACE)
                    .append(newColumn.getName())
                    .append(StringUtils.SPACE);
        } else {
            sql.append(newColumn.getName()).append(StringUtils.SPACE);
        }
        sql.append(mysqlTypeBasicTypeDefine.getColumnType()).append(StringUtils.SPACE);
        if (mysqlTypeBasicTypeDefine.isNullable()) {
            sql.append("NULL ");
        } else {
            sql.append("NOT NULL ");
        }
        String comment = mysqlTypeBasicTypeDefine.getComment();
        if (StringUtils.isNotBlank(comment)) {
            sql.append("COMMENT '").append(comment).append("'");
        }
        return sql + ";";
    }

    public enum AlterType {
        ADD,
        DROP,
        MODIFY,
        CHANGE;

        public String getTypeQuoteBySpace() {
            return StringUtils.SPACE + this.name() + StringUtils.SPACE;
        }
    }
}
