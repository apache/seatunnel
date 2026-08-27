/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.seatunnel.connectors.seatunnel.jdbc.catalog;

import org.apache.seatunnel.api.table.catalog.ConstraintKey;
import org.apache.seatunnel.api.table.catalog.PrimaryKey;

import java.util.HashSet;
import java.util.List;
import java.util.stream.Collectors;

public abstract class AbstractJdbcCreateTableSqlBuilder {

    protected boolean primaryContainsAllConstrainKey(
            PrimaryKey primaryKey, ConstraintKey constraintKey) {
        List<String> columnNames = primaryKey.getColumnNames();
        List<ConstraintKey.ConstraintKeyColumn> constraintKeyColumnNames =
                constraintKey.getColumnNames();
        return new HashSet<>(
                        columnNames.stream().map(Object::toString).collect(Collectors.toList()))
                .containsAll(
                        constraintKeyColumnNames.stream()
                                .map(ConstraintKey.ConstraintKeyColumn::getColumnName)
                                .collect(Collectors.toList()));
    }
}
