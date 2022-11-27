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

package org.apache.seatunnel.api.table.catalog;

import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * Represent a physical table schema.
 */
public final class TableSchema implements Serializable {
    private static final long serialVersionUID = 1L;
    private final List<Column> columns;

    private final PrimaryKey primaryKey;

    private TableSchema(List<Column> columns, PrimaryKey primaryKey) {
        this.columns = columns;
        this.primaryKey = primaryKey;
    }

    public static TableSchema.Builder builder() {
        return new Builder();
    }

    /**
     * Returns all {@link Column}s of this schema.
     */
    public List<Column> getColumns() {
        return columns;
    }

    public SeaTunnelRowType toPhysicalRowDataType() {
        SeaTunnelDataType<?>[] fieldTypes = columns.stream()
            .filter(Column::isPhysical)
            .map(Column::getDataType)
            .toArray(SeaTunnelDataType[]::new);
        String[] fields = columns.stream()
            .filter(Column::isPhysical)
            .map(Column::getName)
            .toArray(String[]::new);
        return new SeaTunnelRowType(fields, fieldTypes);
    }

    public static final class Builder {
        private final List<Column> columns = new ArrayList<>();

        private PrimaryKey primaryKey;

        public Builder columns(List<Column> columns) {
            this.columns.addAll(columns);
            return this;
        }

        public Builder column(Column column) {
            this.columns.add(column);
            return this;
        }

        public Builder physicalColumn(String name, SeaTunnelDataType<?> dataType) {
            this.columns.add(Column.physical(name, dataType));
            return this;
        }

        public Builder primaryKey(PrimaryKey primaryKey) {
            this.primaryKey = primaryKey;
            return this;
        }

        public Builder primaryKey(String constraintName, List<String> columnNames) {
            this.primaryKey = PrimaryKey.of(constraintName, columnNames);
            return this;
        }

        public TableSchema build() {
            return new TableSchema(columns, primaryKey);
        }
    }

    public static final class PrimaryKey implements Serializable {
        private static final long serialVersionUID = 1L;

        private final String constraintName;
        private final List<String> columnNames;

        private PrimaryKey(String constraintName, List<String> columnNames) {
            this.constraintName = constraintName;
            this.columnNames = columnNames;
        }

        public static PrimaryKey of(String constraintName, List<String> columnNames) {
            return new PrimaryKey(constraintName, columnNames);
        }

        public String getConstraintName() {
            return constraintName;
        }

        public List<String> getColumnNames() {
            return columnNames;
        }

        @Override
        public String toString() {
            return String.format("CONSTRAINT %s PRIMARY KEY (%s) NOT ENFORCED", constraintName, String.join(", ", columnNames));
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            PrimaryKey that = (PrimaryKey) o;
            return constraintName.equals(that.constraintName) && columnNames.equals(that.columnNames);
        }

        @Override
        public int hashCode() {
            return Objects.hash(constraintName, columnNames);
        }
    }
}
