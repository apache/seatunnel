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

package org.apache.seatunnel.api.table.factory;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.common.utils.SeaTunnelException;

import org.apache.commons.lang3.StringUtils;

import lombok.Getter;

import java.util.ArrayList;
import java.util.List;

@Getter
public abstract class TableFactoryContext {

    private final ReadonlyConfig options;
    private final ClassLoader classLoader;

    public TableFactoryContext(ReadonlyConfig options, ClassLoader classLoader) {
        this.options = options;
        this.classLoader = classLoader;
    }

    protected static void checkCatalogTableIllegal(List<CatalogTable> catalogTables) {
        for (CatalogTable catalogTable : catalogTables) {
            List<String> alreadyChecked = new ArrayList<>();
            for (String fieldName : catalogTable.getTableSchema().getFieldNames()) {
                if (StringUtils.isBlank(fieldName)) {
                    throw new SeaTunnelException(
                            String.format(
                                    "Table %s field name cannot be empty",
                                    catalogTable.getTablePath().getFullName()));
                }
                if (alreadyChecked.contains(fieldName)) {
                    throw new SeaTunnelException(
                            String.format(
                                    "Table %s field %s duplicate",
                                    catalogTable.getTablePath().getFullName(), fieldName));
                }
                alreadyChecked.add(fieldName);
            }
        }
    }
}
