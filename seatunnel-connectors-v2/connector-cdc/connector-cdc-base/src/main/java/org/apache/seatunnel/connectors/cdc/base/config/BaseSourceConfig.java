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

package org.apache.seatunnel.connectors.cdc.base.config;

import org.apache.seatunnel.connectors.cdc.base.source.IncrementalSource;

import io.debezium.config.Configuration;
import lombok.Getter;

import java.util.Properties;

/**
 * A basic Source configuration which is used by {@link IncrementalSource}.
 */
public abstract class BaseSourceConfig implements SourceConfig {

    private static final long serialVersionUID = 1L;

    @Getter
    protected final StartupConfig startupConfig;

    @Getter
    protected final StopConfig stopConfig;

    @Getter
    protected final int splitSize;

    @Getter
    protected final double distributionFactorUpper;
    @Getter
    protected final double distributionFactorLower;

    // --------------------------------------------------------------------------------------------
    // Debezium Configurations
    // --------------------------------------------------------------------------------------------
    protected final Properties dbzProperties;

    public BaseSourceConfig(
        StartupConfig startupConfig,
        StopConfig stopConfig,
        int splitSize,
        double distributionFactorUpper,
        double distributionFactorLower,
        Properties dbzProperties) {
        this.startupConfig = startupConfig;
        this.stopConfig = stopConfig;
        this.splitSize = splitSize;
        this.distributionFactorUpper = distributionFactorUpper;
        this.distributionFactorLower = distributionFactorLower;
        this.dbzProperties = dbzProperties;
    }

    public Configuration getDbzConfiguration() {
        return Configuration.from(dbzProperties);
    }
}
