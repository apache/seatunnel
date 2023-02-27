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

import lombok.AllArgsConstructor;
import lombok.Data;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/** Represent a physical table schema. */
@Data
@AllArgsConstructor
public final class TableSchema implements Serializable {
    private static final long serialVersionUID = 1L;
    private final List<Column> columns;

    private final PrimaryKey primaryKey;

    private final List<ConstraintKey> constraintKeys;

    public static Builder builder() {
        return new Builder();
    }

    public SeaTunnelRowType toPhysicalRowDataType() {
        SeaTunnelDataType<?>[] fieldTypes =
                columns.stream()
                        .filter(Column::isPhysical)
                        .map(Column::getDataType)
                        .toArray(SeaTunnelDataType[]::new);
        String[] fields =
                columns.stream()
                        .filter(Column::isPhysical)
                        .map(Column::getName)
                        .toArray(String[]::new);
        return new SeaTunnelRowType(fields, fieldTypes);
    }

    public static final class Builder {
        private final List<Column> columns = new ArrayList<>();

        private PrimaryKey primaryKey;

        private final List<ConstraintKey> constraintKeys = new ArrayList<>();

        public Builder columns(List<Column> columns) {
            this.columns.addAll(columns);
            return this;
        }

        public Builder column(Column column) {
            this.columns.add(column);
            return this;
        }

        public Builder primaryKey(PrimaryKey primaryKey) {
            this.primaryKey = primaryKey;
            return this;
        }

        public Builder constraintKey(ConstraintKey constraintKey) {
            this.constraintKeys.add(constraintKey);
            return this;
        }

        public TableSchema build() {
            return new TableSchema(columns, primaryKey, constraintKeys);
        }
    }
}
