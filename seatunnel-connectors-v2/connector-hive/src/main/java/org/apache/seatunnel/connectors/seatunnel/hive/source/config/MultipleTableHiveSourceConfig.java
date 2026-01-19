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

package org.apache.seatunnel.connectors.seatunnel.hive.source.config;

import org.apache.seatunnel.shade.com.google.common.collect.Lists;
import org.apache.seatunnel.shade.org.apache.commons.lang3.StringUtils;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.options.ConnectorCommonOptions;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.connectors.seatunnel.hive.config.HiveOptions;
import org.apache.seatunnel.connectors.seatunnel.hive.utils.HiveMetaStoreCatalog;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.stream.Collectors;

@Slf4j
public class MultipleTableHiveSourceConfig implements Serializable {

    private static final long serialVersionUID = 1L;

    @Getter private List<HiveSourceConfig> hiveSourceConfigs;

    public MultipleTableHiveSourceConfig(ReadonlyConfig readonlyConfig) {
        if (readonlyConfig.getOptional(ConnectorCommonOptions.TABLE_LIST).isPresent()) {
            parseFromLocalFileSourceByTableList(readonlyConfig);
        } else if (readonlyConfig.getOptional(ConnectorCommonOptions.TABLE_CONFIGS).isPresent()) {
            parseFromLocalFileSourceByTableConfigs(readonlyConfig);
        } else if (HiveSourceTableDiscovery.isEnabled(readonlyConfig)) {
            parseFromLocalFileSourceByDiscovery(readonlyConfig);
        } else {
            parseFromLocalFileSourceConfig(readonlyConfig);
        }
    }

    private void parseFromLocalFileSourceByTableList(ReadonlyConfig readonlyConfig) {
        List<ReadonlyConfig> expanded =
                readonlyConfig.get(ConnectorCommonOptions.TABLE_LIST).stream()
                        .map(ReadonlyConfig::fromMap)
                        .flatMap(tableConfig -> expandTableConfigIfNeeded(tableConfig).stream())
                        .collect(Collectors.toList());
        this.hiveSourceConfigs = buildHiveSourceConfigs(expanded);
    }

    // hive is structured, should use table_list
    @Deprecated
    private void parseFromLocalFileSourceByTableConfigs(ReadonlyConfig readonlyConfig) {
        List<ReadonlyConfig> expanded =
                readonlyConfig.get(ConnectorCommonOptions.TABLE_CONFIGS).stream()
                        .map(ReadonlyConfig::fromMap)
                        .flatMap(tableConfig -> expandTableConfigIfNeeded(tableConfig).stream())
                        .collect(Collectors.toList());
        this.hiveSourceConfigs = buildHiveSourceConfigs(expanded);
    }

    private void parseFromLocalFileSourceByDiscovery(ReadonlyConfig readonlyConfig) {
        List<ReadonlyConfig> expanded = expandTableConfigIfNeeded(readonlyConfig);
        this.hiveSourceConfigs = buildHiveSourceConfigs(expanded);
    }

    private List<ReadonlyConfig> expandTableConfigIfNeeded(ReadonlyConfig tableConfig) {
        if (!HiveSourceTableDiscovery.isEnabled(tableConfig)) {
            return Lists.newArrayList(tableConfig);
        }

        String tableNamePattern =
                tableConfig.getOptional(HiveOptions.TABLE_NAME).orElse("<missing table_name>");
        if (!tableConfig.getOptional(HiveOptions.METASTORE_URI).isPresent()
                || StringUtils.isBlank(tableConfig.get(HiveOptions.METASTORE_URI))) {
            throw new IllegalArgumentException(
                    "Hive metastore_uri is required for regex table discovery (use_regex). table_name="
                            + tableNamePattern);
        }

        try (HiveMetaStoreCatalog catalog = HiveMetaStoreCatalog.create(tableConfig)) {
            catalog.open();
            List<TablePath> tablePaths =
                    HiveSourceTableDiscovery.discoverTablePaths(tableConfig, catalog);
            if (tablePaths.isEmpty()) {
                throw new IllegalArgumentException(
                        "No hive tables matched the regex pattern. Please check `table_name` and `use_regex`. table_name="
                                + tableNamePattern);
            }
            logMatchedTables(tableNamePattern, tablePaths);
            return tablePaths.stream()
                    .map(path -> overrideTableName(tableConfig, path.getFullName()))
                    .collect(Collectors.toList());
        }
    }

    private void logMatchedTables(String tableNamePattern, List<TablePath> tablePaths) {
        String matchedTables =
                tablePaths.stream().map(TablePath::getFullName).collect(Collectors.joining(", "));
        log.info(
                "Hive regex discovery matched {} table(s) for table_name='{}': {}",
                tablePaths.size(),
                tableNamePattern,
                matchedTables);
    }

    private ReadonlyConfig overrideTableName(ReadonlyConfig baseConfig, String tableName) {
        LinkedHashMap<String, Object> map = new LinkedHashMap<>(baseConfig.getSourceMap());
        map.put(HiveOptions.TABLE_NAME.key(), tableName);
        return ReadonlyConfig.fromMap(map);
    }

    private List<HiveSourceConfig> buildHiveSourceConfigs(List<ReadonlyConfig> tableConfigs) {
        List<HiveSourceConfig> configs = new ArrayList<>(tableConfigs.size());
        for (ReadonlyConfig tableConfig : tableConfigs) {
            String tableName =
                    tableConfig.getOptional(HiveOptions.TABLE_NAME).orElse("<missing table_name>");
            try {
                configs.add(new HiveSourceConfig(tableConfig));
            } catch (Exception exception) {
                log.error(
                        "Failed to initialize Hive source config for table_name='{}'. "
                                + "Please check table existence/permissions and metastore connectivity.",
                        tableName,
                        exception);
                throw exception;
            }
        }
        return configs;
    }

    private void parseFromLocalFileSourceConfig(ReadonlyConfig localFileSourceRootConfig) {
        HiveSourceConfig hiveSourceConfig = new HiveSourceConfig(localFileSourceRootConfig);
        this.hiveSourceConfigs = Lists.newArrayList(hiveSourceConfig);
    }
}
