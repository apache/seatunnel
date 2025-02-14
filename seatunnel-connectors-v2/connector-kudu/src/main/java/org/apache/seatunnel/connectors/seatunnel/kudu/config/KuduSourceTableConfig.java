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

import org.apache.seatunnel.shade.com.google.common.collect.Lists;

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
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

@Getter
public class KuduSourceTableConfig implements Serializable {

    private final TablePath tablePath;

    private final CatalogTable catalogTable;

    private String filter;

    private KuduSourceTableConfig(String tablePath, CatalogTable catalogTable) {
        this.tablePath = TablePath.of(tablePath);
        this.catalogTable = catalogTable;
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
            if (config.getOptional(ConnectorCommonOptions.TABLE_LIST).isPresent()) {
                return config.get(ConnectorCommonOptions.TABLE_LIST).stream()
                        .map(ReadonlyConfig::fromMap)
                        .map(readonlyConfig -> parseKuduSourceConfig(readonlyConfig, kuduCatalog))
                        .collect(Collectors.toList());
            }
            KuduSourceTableConfig kuduSourceTableConfig =
                    parseKuduSourceConfig(config, kuduCatalog);
            return Lists.newArrayList(kuduSourceTableConfig);
        }
    }

    public static KuduSourceTableConfig parseKuduSourceConfig(
            ReadonlyConfig config, KuduCatalog kuduCatalog) {
        CatalogTable catalogTable;
        String tableName = config.get(CommonConfig.TABLE_NAME);
        if (config.getOptional(ConnectorCommonOptions.SCHEMA).isPresent()) {
            catalogTable = CatalogTableUtil.buildWithConfig(config);
        } else {
            catalogTable = kuduCatalog.getTable(TablePath.of(config.get(CommonConfig.TABLE_NAME)));
        }
        return new KuduSourceTableConfig(tableName, catalogTable);
    }
}
