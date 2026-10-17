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

package org.apache.seatunnel.connectors.seatunnel.cdc.mariadb.utils;

import io.debezium.relational.TableId;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class MariaDbDdlBuilder {
    private final TableId tableId;
    private final List<Column> columns;
    private List<String> primaryKeys;

    public MariaDbDdlBuilder(TableId tableId) {
        this.tableId = tableId;
        this.columns = new ArrayList<>();
        this.primaryKeys = new ArrayList<>();
    }

    public MariaDbDdlBuilder addColumn(Column column) {
        columns.add(column);
        if (column.isPrimaryKey()) {
            primaryKeys.add(column.getColumnName());
        }
        return this;
    }

    public String generateDdl() {
        String columnDefinitions =
                columns.stream().map(Column::generateDdl).collect(Collectors.joining(", "));
        String keyDefinitions =
                primaryKeys.stream()
                        .map(MariaDbUtils::quote)
                        .collect(Collectors.joining(", ", "PRIMARY KEY (", ")"));
        return String.format(
                "CREATE TABLE %s (%s, %s)", tableId.table(), columnDefinitions, keyDefinitions);
    }

    public static class Column {
        private String columnName;
        private String columnType;
        private boolean nullable;
        private boolean primaryKey;
        private boolean uniqueKey;
        private String defaultValue;
        private String extra;

        public Column() {}

        public Column(
                String columnName,
                String columnType,
                boolean nullable,
                boolean primaryKey,
                boolean uniqueKey,
                String defaultValue,
                String extra) {
            this.columnName = columnName;
            this.columnType = columnType;
            this.nullable = nullable;
            this.primaryKey = primaryKey;
            this.uniqueKey = uniqueKey;
            this.defaultValue = defaultValue;
            this.extra = extra;
        }

        public static ColumnBuilder builder() {
            return new ColumnBuilder();
        }

        public String getColumnName() {
            return columnName;
        }

        public String getColumnType() {
            return columnType;
        }

        public boolean isNullable() {
            return nullable;
        }

        public boolean isPrimaryKey() {
            return primaryKey;
        }

        public boolean isUniqueKey() {
            return uniqueKey;
        }

        public String getDefaultValue() {
            return defaultValue;
        }

        public String getExtra() {
            return extra;
        }

        public String generateDdl() {
            return MariaDbUtils.quote(columnName)
                    + " "
                    + columnType
                    + " "
                    + (nullable ? "" : "NOT NULL");
        }

        public static class ColumnBuilder {
            private String columnName;
            private String columnType;
            private boolean nullable;
            private boolean primaryKey;
            private boolean uniqueKey;
            private String defaultValue;
            private String extra;

            public ColumnBuilder columnName(String columnName) {
                this.columnName = columnName;
                return this;
            }

            public ColumnBuilder columnType(String columnType) {
                this.columnType = columnType;
                return this;
            }

            public ColumnBuilder nullable(boolean nullable) {
                this.nullable = nullable;
                return this;
            }

            public ColumnBuilder primaryKey(boolean primaryKey) {
                this.primaryKey = primaryKey;
                return this;
            }

            public ColumnBuilder uniqueKey(boolean uniqueKey) {
                this.uniqueKey = uniqueKey;
                return this;
            }

            public ColumnBuilder defaultValue(String defaultValue) {
                this.defaultValue = defaultValue;
                return this;
            }

            public ColumnBuilder extra(String extra) {
                this.extra = extra;
                return this;
            }

            public Column build() {
                return new Column(
                        columnName,
                        columnType,
                        nullable,
                        primaryKey,
                        uniqueKey,
                        defaultValue,
                        extra);
            }
        }
    }
}
