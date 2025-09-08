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

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.options.ConnectorCommonOptions;

import lombok.Getter;

import java.io.Serializable;
import java.util.List;
import java.util.stream.Collectors;

public class MultipleTableHiveSourceConfig implements Serializable {

    private static final long serialVersionUID = 1L;

    @Getter private List<HiveSourceConfig> hiveSourceConfigs;

    public MultipleTableHiveSourceConfig(ReadonlyConfig readonlyConfig) {
        if (readonlyConfig.getOptional(ConnectorCommonOptions.TABLE_LIST).isPresent()) {
            parseFromLocalFileSourceByTableList(readonlyConfig);
        } else if (readonlyConfig.getOptional(ConnectorCommonOptions.TABLE_CONFIGS).isPresent()) {
            parseFromLocalFileSourceByTableConfigs(readonlyConfig);
        } else {
            parseFromLocalFileSourceConfig(readonlyConfig);
        }
    }

    private void parseFromLocalFileSourceByTableList(ReadonlyConfig readonlyConfig) {
        this.hiveSourceConfigs =
                readonlyConfig.get(ConnectorCommonOptions.TABLE_LIST).stream()
                        .map(ReadonlyConfig::fromMap)
                        .map(HiveSourceConfig::new)
                        .collect(Collectors.toList());
    }
    // hive is structured, should use table_list
    @Deprecated
    private void parseFromLocalFileSourceByTableConfigs(ReadonlyConfig readonlyConfig) {
        this.hiveSourceConfigs =
                readonlyConfig.get(ConnectorCommonOptions.TABLE_CONFIGS).stream()
                        .map(ReadonlyConfig::fromMap)
                        .map(HiveSourceConfig::new)
                        .collect(Collectors.toList());
    }

    private void parseFromLocalFileSourceConfig(ReadonlyConfig localFileSourceRootConfig) {
        HiveSourceConfig hiveSourceConfig = new HiveSourceConfig(localFileSourceRootConfig);
        this.hiveSourceConfigs = Lists.newArrayList(hiveSourceConfig);
    }
}
