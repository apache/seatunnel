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

import lombok.Getter;

import java.io.Serializable;
import java.util.List;
import java.util.stream.Collectors;

public abstract class BaseMultipleTableFileSourceConfig implements Serializable {

    private static final long serialVersionUID = 1L;

    @Getter private List<BaseFileSourceConfig> fileSourceConfigs;

    public BaseMultipleTableFileSourceConfig(ReadonlyConfig fileSourceRootConfig) {
        if (fileSourceRootConfig.getOptional(ConnectorCommonOptions.TABLE_CONFIGS).isPresent()) {
            parseFromFileSourceConfigs(fileSourceRootConfig);
        } else {
            parseFromFileSourceConfig(fileSourceRootConfig);
        }
    }

    private void parseFromFileSourceConfigs(ReadonlyConfig fileSourceRootConfig) {
        this.fileSourceConfigs =
                fileSourceRootConfig.get(ConnectorCommonOptions.TABLE_CONFIGS).stream()
                        .map(ReadonlyConfig::fromMap)
                        .map(this::getBaseSourceConfig)
                        .collect(Collectors.toList());
    }

    public abstract BaseFileSourceConfig getBaseSourceConfig(ReadonlyConfig readonlyConfig);

    private void parseFromFileSourceConfig(ReadonlyConfig fileSourceRootConfig) {
        this.fileSourceConfigs = Lists.newArrayList(getBaseSourceConfig(fileSourceRootConfig));
    }
}
