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

package org.apache.seatunnel.connectors.seatunnel.cdc.mysql.utils;

import io.debezium.relational.TableId;
import lombok.Builder;
import lombok.Getter;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class MySqlDdlBuilder {
    private final TableId tableId;
    private final List<Column> columns;
    private List<String> primaryKeys;

    public MySqlDdlBuilder(TableId tableId) {
        this.tableId = tableId;
        this.columns = new ArrayList<>();
        this.primaryKeys = new ArrayList<>();
    }

    public MySqlDdlBuilder addColumn(Column column) {
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
                        .map(MySqlUtils::quote)
                        .collect(Collectors.joining(", ", "PRIMARY KEY (", ")"));
        return String.format(
                "CREATE TABLE %s (%s, %s)", tableId.table(), columnDefinitions, keyDefinitions);
    }

    @Getter
    @Builder
    public static class Column {
        private String columnName;
        private String columnType;
        private boolean nullable;
        private boolean primaryKey;
        private boolean uniqueKey;
        private String defaultValue;
        private String extra;

        public String generateDdl() {
            return MySqlUtils.quote(columnName)
                    + " "
                    + columnType
                    + " "
                    + (nullable ? "" : "NOT NULL");
        }
    }
}
