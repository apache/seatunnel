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

package org.apache.seatunnel.connectors.seatunnel.fake.utils;

import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.PrimaryKey;
import org.apache.seatunnel.connectors.seatunnel.fake.config.FakeConfig;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class IdGeneratorUtils {

    private static final Map<String, AutoIncrementIdGenerator> idGenerators =
            new ConcurrentHashMap<>();

    public static synchronized AutoIncrementIdGenerator getIdGenerator(
            FakeConfig fakeConfig, String columnName) {
        CatalogTable catalogTable = fakeConfig.getCatalogTable();
        String tableName = catalogTable.getTableId().getTableName();
        String key = String.format("%s_%s", tableName, columnName);
        return idGenerators.computeIfAbsent(
                key,
                k -> {
                    if (isPrimaryColumn(fakeConfig, columnName)) {
                        return new AutoIncrementIdGenerator(fakeConfig.getAutoIncrementStart());
                    }
                    return null;
                });
    }

    public static boolean isPrimaryColumn(FakeConfig fakeConfig, String columnName) {
        PrimaryKey primaryKey = fakeConfig.getCatalogTable().getTableSchema().getPrimaryKey();
        if (primaryKey == null) {
            return false;
        }
        List<String> primaryColumns = primaryKey.getColumnNames();
        return primaryColumns != null && primaryColumns.contains(columnName);
    }
}
