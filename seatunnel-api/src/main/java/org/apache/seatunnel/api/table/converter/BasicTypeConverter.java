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

package org.apache.seatunnel.api.table.converter;

import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.Column;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public interface BasicTypeConverter<T extends BasicTypeDefine> extends TypeConverter<T> {

    /**
     * Convert {@link CatalogTable} columns definition to external system's type definition.
     *
     * @param table
     * @param identifiers
     * @return
     */
    default List<T> reconvert(CatalogTable table, String... identifiers) {
        List<T> typeDefines = new ArrayList<>();
        for (Column column : table.getTableSchema().getColumns()) {
            T t = reconvert(column);
            if (table.getCatalogName().equals(identifier())) {
                t.setColumnType(column.getSourceType());
            }
            if (identifiers != null) {
                Arrays.asList(identifiers)
                        .forEach(
                                id -> {
                                    if (id.equals(t.getName())) {
                                        t.setColumnType(column.getSourceType());
                                    }
                                });
            }
            typeDefines.add(t);
        }
        return typeDefines;
    }
}
