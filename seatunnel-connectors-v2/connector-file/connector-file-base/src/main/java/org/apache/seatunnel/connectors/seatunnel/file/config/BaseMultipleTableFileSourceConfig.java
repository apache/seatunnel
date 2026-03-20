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

package org.apache.seatunnel.connectors.seatunnel.file.config;

import org.apache.seatunnel.shade.com.google.common.collect.Lists;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.options.ConnectorCommonOptions;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.common.exception.SeaTunnelRuntimeException;

import lombok.Getter;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.apache.seatunnel.api.table.schema.exception.SchemaEvolutionErrorCode.CATALOG_TABLE_SIZE_IS_ERROR;

public abstract class BaseMultipleTableFileSourceConfig implements Serializable {

    private static final long serialVersionUID = 1L;

    @Getter private List<BaseFileSourceConfig> fileSourceConfigs;

    public BaseMultipleTableFileSourceConfig(
            ReadonlyConfig fileSourceRootConfig, List<CatalogTable> catalogTablesFromConfig) {
        if (fileSourceRootConfig.getOptional(ConnectorCommonOptions.TABLE_CONFIGS).isPresent()) {
            parseFromFileSourceConfigs(fileSourceRootConfig, catalogTablesFromConfig);
        } else {
            parseFromFileSourceConfig(fileSourceRootConfig, catalogTablesFromConfig.get(0));
        }
    }

    private void parseFromFileSourceConfigs(
            ReadonlyConfig fileSourceRootConfig, List<CatalogTable> catalogTableFromConfigs) {
        final List<Map<String, Object>> maps =
                fileSourceRootConfig.get(ConnectorCommonOptions.TABLE_CONFIGS);
        if (catalogTableFromConfigs.size() != maps.size()) {
            throw new SeaTunnelRuntimeException(
                    CATALOG_TABLE_SIZE_IS_ERROR, "The catalogTableFromConfigs size is not correct");
        }
        this.fileSourceConfigs = new ArrayList<>();
        for (int i = 0; i < catalogTableFromConfigs.size(); i++) {
            fileSourceConfigs.add(
                    this.getBaseSourceConfig(
                            ReadonlyConfig.fromMap(maps.get(i)), catalogTableFromConfigs.get(i)));
        }
    }

    public abstract BaseFileSourceConfig getBaseSourceConfig(
            ReadonlyConfig readonlyConfig, CatalogTable catalogTableFromConfig);

    private void parseFromFileSourceConfig(
            ReadonlyConfig fileSourceRootConfig, CatalogTable catalogTableFromConfig) {
        this.fileSourceConfigs =
                Lists.newArrayList(
                        getBaseSourceConfig(fileSourceRootConfig, catalogTableFromConfig));
    }
}
