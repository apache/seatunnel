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

import org.apache.seatunnel.shade.com.google.common.cache.Cache;
import org.apache.seatunnel.shade.com.google.common.cache.CacheBuilder;

import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.PrimaryKey;
import org.apache.seatunnel.connectors.seatunnel.fake.config.FakeConfig;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

public class IdGeneratorUtils {

    private static final Cache<String, AutoIncrementIdGenerator> idGenerators =
            CacheBuilder.newBuilder()
                    .maximumSize(1000)
                    .expireAfterWrite(30, TimeUnit.MINUTES)
                    .build();

    public static synchronized Optional<AutoIncrementIdGenerator> getIdGenerator(
            String jobId, FakeConfig fakeConfig, String columnName) {
        CatalogTable catalogTable = fakeConfig.getCatalogTable();
        String tableName = catalogTable.getTableId().getTableName();
        String key = String.format("%s:%s_%s", jobId, tableName, columnName);
        AutoIncrementIdGenerator idGenerator = null;
        try {
            idGenerator =
                    idGenerators.get(
                            key,
                            () -> {
                                if (isPrimaryColumn(fakeConfig, columnName)) {
                                    return new AutoIncrementIdGenerator(
                                            fakeConfig.getAutoIncrementStart());
                                } else {
                                    return null;
                                }
                            });
        } catch (ExecutionException e) {
            throw new RuntimeException(e);
        }
        return Optional.ofNullable(idGenerator);
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
