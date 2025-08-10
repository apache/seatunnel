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

import lombok.AccessLevel;
import lombok.Data;
import lombok.Getter;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

/** Represent a physical table schema. */
@Data
public class AbstractSchema implements Serializable {
    private static final long serialVersionUID = 1L;
    protected final List<Column> columns;

    @Getter(AccessLevel.PRIVATE)
    protected final List<String> columnNames;

    public AbstractSchema(List<Column> columns) {
        this.columns = columns;
        this.columnNames = columns.stream().map(Column::getName).collect(Collectors.toList());
    }

    // Lombok requires a no-arg constructor for @Data annotation to work properly
    private AbstractSchema() {
        this.columns = new ArrayList<>();
        this.columnNames = new ArrayList<>();
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

    public String[] getFieldNames() {
        return columnNames.toArray(new String[0]);
    }

    public int indexOf(String columnName) {
        return columnNames.indexOf(columnName);
    }

    public Column getColumn(String columnName) {
        return columns.get(indexOf(columnName));
    }

    public boolean contains(String columnName) {
        return columnNames.contains(columnName);
    }

    public List<Column> getColumns() {
        return Collections.unmodifiableList(columns);
    }
}
