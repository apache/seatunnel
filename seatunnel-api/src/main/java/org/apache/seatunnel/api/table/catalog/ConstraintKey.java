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

import lombok.AllArgsConstructor;
import lombok.Data;

import java.io.Serializable;
import java.util.List;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.checkNotNull;

@Data
public class ConstraintKey implements Serializable {
    private static final long serialVersionUID = 1L;

    private final ConstraintType constraintType;

    private final String constraintName;

    private final List<ConstraintKeyColumn> columnNames;

    private ConstraintKey(
            ConstraintType constraintType,
            String constraintName,
            List<ConstraintKeyColumn> columnNames) {
        checkNotNull(constraintType, "constraintType must not be null");

        this.constraintType = constraintType;
        this.constraintName = constraintName;
        this.columnNames = columnNames;
    }

    public static ConstraintKey of(
            ConstraintType constraintType,
            String constraintName,
            List<ConstraintKeyColumn> columnNames) {
        return new ConstraintKey(constraintType, constraintName, columnNames);
    }

    @Data
    @AllArgsConstructor
    public static class ConstraintKeyColumn implements Serializable {
        private final String columnName;
        private final ColumnSortType sortType;

        public static ConstraintKeyColumn of(String columnName, ColumnSortType sortType) {
            return new ConstraintKeyColumn(columnName, sortType);
        }

        public ConstraintKeyColumn copy() {
            return ConstraintKeyColumn.of(columnName, sortType);
        }
    }

    public enum ConstraintType {
        KEY,
        UNIQUE_KEY,
        FOREIGN_KEY
    }

    public enum ColumnSortType {
        ASC,
        DESC
    }

    public ConstraintKey copy() {
        List<ConstraintKeyColumn> collect =
                columnNames.stream().map(ConstraintKeyColumn::copy).collect(Collectors.toList());
        return ConstraintKey.of(constraintType, constraintName, collect);
    }
}
