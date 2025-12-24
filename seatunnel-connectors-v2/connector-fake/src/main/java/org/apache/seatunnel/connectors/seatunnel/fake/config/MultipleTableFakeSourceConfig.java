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

package org.apache.seatunnel.connectors.seatunnel.fake.config;

import org.apache.seatunnel.shade.com.google.common.collect.Lists;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.options.ConnectorCommonOptions;

import org.apache.commons.collections4.CollectionUtils;

import lombok.Getter;

import java.io.Serializable;
import java.util.HashSet;
import java.util.List;
import java.util.stream.Collectors;

public class MultipleTableFakeSourceConfig implements Serializable {

    private static final long serialVersionUID = 1L;

    @Getter private List<FakeConfig> fakeConfigs;

    public MultipleTableFakeSourceConfig(ReadonlyConfig fakeSourceRootConfig) {
        if (fakeSourceRootConfig.getOptional(ConnectorCommonOptions.TABLE_CONFIGS).isPresent()) {
            parseFromConfigs(fakeSourceRootConfig);
        } else {
            parseFromConfig(fakeSourceRootConfig);
        }
        // validate
        if (fakeConfigs.size() > 1) {
            List<String> tableNames =
                    fakeConfigs.stream()
                            .map(FakeConfig::getCatalogTable)
                            .map(catalogTable -> catalogTable.getTableId().toTablePath().toString())
                            .collect(Collectors.toList());
            if (CollectionUtils.size(tableNames) != new HashSet<>(tableNames).size()) {
                throw new IllegalArgumentException("table name: " + tableNames + " must be unique");
            }
        }
    }

    private void parseFromConfigs(ReadonlyConfig readonlyConfig) {
        List<ReadonlyConfig> readonlyConfigs =
                readonlyConfig.getOptional(ConnectorCommonOptions.TABLE_CONFIGS).get().stream()
                        .map(ReadonlyConfig::fromMap)
                        .collect(Collectors.toList());
        // Use the config outside if it's not set in sub config
        fakeConfigs =
                readonlyConfigs.stream()
                        .map(FakeConfig::buildWithConfig)
                        .collect(Collectors.toList());
    }

    private void parseFromConfig(ReadonlyConfig readonlyConfig) {
        FakeConfig fakeConfig = FakeConfig.buildWithConfig(readonlyConfig);
        fakeConfigs = Lists.newArrayList(fakeConfig);
    }
}
