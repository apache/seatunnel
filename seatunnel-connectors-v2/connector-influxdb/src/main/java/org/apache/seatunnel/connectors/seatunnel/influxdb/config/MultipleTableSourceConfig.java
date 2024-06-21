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

package org.apache.seatunnel.connectors.seatunnel.influxdb.config;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;

import com.google.common.collect.Lists;
import lombok.Getter;

import java.io.Serializable;
import java.util.List;
import java.util.stream.Collectors;

public class MultipleTableSourceConfig implements Serializable {
    private static final long serialVersionUID = 1L;

    @Getter private List<SourceConfig> sourceConfigs;

    public MultipleTableSourceConfig(ReadonlyConfig readonlyConfig) {
        if (readonlyConfig.getOptional(SourceConfig.TABLE_CONFIGS).isPresent()) {
            parseSourceConfigs(readonlyConfig);
        } else {
            parseSourceConfig(readonlyConfig);
        }
    }

    private void parseSourceConfigs(ReadonlyConfig readonlyConfig) {
        this.sourceConfigs =
                readonlyConfig.get(SourceConfig.TABLE_CONFIGS).stream()
                        .map(ReadonlyConfig::fromMap)
                        .map(SourceConfig::loadConfig)
                        .collect(Collectors.toList());
    }

    private void parseSourceConfig(ReadonlyConfig localFileSourceRootConfig) {
        SourceConfig influxdbSourceConfig = SourceConfig.loadConfig(localFileSourceRootConfig);
        this.sourceConfigs = Lists.newArrayList(influxdbSourceConfig);
    }
}
