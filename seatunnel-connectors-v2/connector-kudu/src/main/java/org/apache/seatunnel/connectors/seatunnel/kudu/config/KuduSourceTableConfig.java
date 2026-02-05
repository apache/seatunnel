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

package org.apache.seatunnel.connectors.seatunnel.kudu.config;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.options.ConnectorCommonOptions;
import org.apache.seatunnel.api.table.catalog.Catalog;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.CatalogTableUtil;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.api.table.factory.FactoryUtil;
import org.apache.seatunnel.connectors.seatunnel.kudu.catalog.KuduCatalog;
import org.apache.seatunnel.connectors.seatunnel.kudu.catalog.KuduCatalogFactory;

import lombok.Getter;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

@Getter
public class KuduSourceTableConfig implements Serializable {

    private final TablePath tablePath;

    private final CatalogTable catalogTable;

    private String filter;

    private KuduSourceTableConfig(String tablePath, CatalogTable catalogTable, String filter) {
        this.tablePath = TablePath.of(tablePath);
        this.catalogTable = catalogTable;
        this.filter = filter;
    }

    public static List<KuduSourceTableConfig> of(ReadonlyConfig config) {
        Optional<Catalog> optionalCatalog =
                FactoryUtil.createOptionalCatalog(
                        KuduCatalogFactory.IDENTIFIER,
                        config,
                        KuduSourceTableConfig.class.getClassLoader(),
                        KuduCatalogFactory.IDENTIFIER);

        try (KuduCatalog kuduCatalog = (KuduCatalog) optionalCatalog.get()) {
            kuduCatalog.open();

            List<ReadonlyConfig> tableConfigs = new ArrayList<>();
            if (config.getOptional(ConnectorCommonOptions.TABLE_LIST).isPresent()) {
                tableConfigs =
                        config.get(ConnectorCommonOptions.TABLE_LIST).stream()
                                .map(ReadonlyConfig::fromMap)
                                .collect(Collectors.toList());
            } else {
                tableConfigs.add(config);
            }

            List<KuduSourceTableConfig> result = new ArrayList<>();
            for (ReadonlyConfig tableConfig : tableConfigs) {
                Boolean useRegex = tableConfig.get(KuduSourceOptions.USE_REGEX);
                if (useRegex != null && useRegex) {
                    result.addAll(parseKuduSourceConfigWithRegex(tableConfig, kuduCatalog));
                } else {
                    result.add(parseKuduSourceConfig(tableConfig, kuduCatalog));
                }
            }

            return result;
        }
    }

    public static KuduSourceTableConfig parseKuduSourceConfig(
            ReadonlyConfig config, KuduCatalog kuduCatalog) {
        CatalogTable catalogTable;
        String tableName = config.get(KuduBaseOptions.TABLE_NAME);
        if (config.getOptional(ConnectorCommonOptions.SCHEMA).isPresent()) {
            catalogTable = CatalogTableUtil.buildWithConfig(config);
        } else {
            catalogTable =
                    kuduCatalog.getTable(TablePath.of(config.get(KuduBaseOptions.TABLE_NAME)));
        }
        return new KuduSourceTableConfig(
                tableName, catalogTable, config.get(KuduSourceOptions.FILTER));
    }

    static List<KuduSourceTableConfig> parseKuduSourceConfigWithRegex(
            ReadonlyConfig config, KuduCatalog kuduCatalog) {
        String patternString = config.get(KuduBaseOptions.TABLE_NAME);
        if (patternString == null) {
            throw new IllegalArgumentException(
                    "When `use_regex` is enabled, `table_name` must be configured");
        }

        Pattern pattern = Pattern.compile(patternString);

        List<String> allTables =
                kuduCatalog.listTables(kuduCatalog.getDefaultDatabase()).stream()
                        .filter(tableName -> pattern.matcher(tableName).matches())
                        .collect(Collectors.toList());

        List<KuduSourceTableConfig> result = new ArrayList<>();
        for (String tableName : allTables) {
            CatalogTable catalogTable = kuduCatalog.getTable(TablePath.of(tableName));
            result.add(
                    new KuduSourceTableConfig(
                            tableName, catalogTable, config.get(KuduSourceOptions.FILTER)));
        }

        return result;
    }
}
