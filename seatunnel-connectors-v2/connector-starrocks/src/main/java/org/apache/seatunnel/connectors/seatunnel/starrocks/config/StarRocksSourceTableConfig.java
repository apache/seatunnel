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

package org.apache.seatunnel.connectors.seatunnel.starrocks.config;

import org.apache.seatunnel.shade.com.google.common.collect.Lists;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.TableIdentifier;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.api.table.catalog.schema.ReadonlyConfigParser;

import lombok.Getter;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@Getter
public class StarRocksSourceTableConfig implements Serializable {

    private final String table;

    private final CatalogTable catalogTable;

    private final String scanFilter;

    private StarRocksSourceTableConfig(
            String tableName, CatalogTable catalogTable, String scanFilter) {
        this.table = tableName;
        this.catalogTable = catalogTable;
        this.scanFilter = scanFilter;
    }

    public static StarRocksSourceTableConfig parseStarRocksSourceConfig(ReadonlyConfig config) {

        String table = config.get(StarRocksSourceOptions.TABLE);
        TablePath tablePath = TablePath.of(table);
        TableSchema tableSchema = new ReadonlyConfigParser().parse(config);
        CatalogTable catalogTable =
                CatalogTable.of(
                        TableIdentifier.of("", tablePath),
                        tableSchema,
                        new HashMap<>(),
                        new ArrayList<>(),
                        "");

        return new StarRocksSourceTableConfig(
                table, catalogTable, config.get(StarRocksSourceOptions.SCAN_FILTER));
    }

    public static List<StarRocksSourceTableConfig> of(ReadonlyConfig config) {

        if (config.getOptional(StarRocksSourceOptions.TABLE_LIST).isPresent()) {
            List<Map<String, Object>> maps = config.get(StarRocksSourceOptions.TABLE_LIST);
            return maps.stream()
                    .map(ReadonlyConfig::fromMap)
                    .map(StarRocksSourceTableConfig::parseStarRocksSourceConfig)
                    .collect(Collectors.toList());
        }
        return Lists.newArrayList(parseStarRocksSourceConfig(config));
    }
}
