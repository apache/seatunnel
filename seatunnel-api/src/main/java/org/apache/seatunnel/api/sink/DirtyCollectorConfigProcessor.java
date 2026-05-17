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

package org.apache.seatunnel.api.sink;

import org.apache.seatunnel.shade.com.typesafe.config.Config;
import org.apache.seatunnel.shade.com.typesafe.config.ConfigFactory;
import org.apache.seatunnel.shade.com.typesafe.config.ConfigValue;

import org.apache.seatunnel.api.table.catalog.CatalogTable;

import lombok.extern.slf4j.Slf4j;

/** Configuration processor for dirty data collection functionality */
@Slf4j
public class DirtyCollectorConfigProcessor {

    private static final String DIRTY_COLLECTOR_CONFIG_KEY = "dirty.collector";
    private static final String DIRTY_VALIDATOR_CONFIG_KEY = "dirty.validator";

    public static Config parseEnvDirtyConfig(Config envConfig) {
        if (envConfig != null && envConfig.hasPath(DIRTY_COLLECTOR_CONFIG_KEY)) {
            return envConfig.getConfig(DIRTY_COLLECTOR_CONFIG_KEY);
        }
        return null;
    }

    public static Config mergeConfig(Config sinkConfig, Config envDirtyConfig) {
        if (sinkConfig == null) {
            return ConfigFactory.empty();
        }

        if (sinkConfig.hasPath(DIRTY_COLLECTOR_CONFIG_KEY)) {
            log.debug("Sink has its own dirty collector configuration, using sink config");
            return sinkConfig;
        }

        if (envDirtyConfig != null) {
            log.debug("Merging env dirty collector config into sink config");
            ConfigValue envValue = envDirtyConfig.root();
            return sinkConfig.withValue(DIRTY_COLLECTOR_CONFIG_KEY, envValue);
        }

        log.debug("No dirty collector configuration found");
        return sinkConfig;
    }

    public static DirtyRecordCollector initializeCollector(Config config) {
        if (config == null || !config.hasPath(DIRTY_COLLECTOR_CONFIG_KEY)) {
            log.debug("No dirty collector configuration found, using NoOp collector");
            return NoOpDirtyRecordCollector.INSTANCE;
        }

        Config collectorConfig = config.getConfig(DIRTY_COLLECTOR_CONFIG_KEY);
        DirtyRecordCollector collector =
                DirtyRecordCollectorFactory.createCollector(collectorConfig);
        log.info(
                "Successfully initialized dirty record collector: {}",
                collector.getClass().getSimpleName());
        return collector;
    }

    public static Config getMergedSinkConfigForDirty(Config envConfig, Config sinkConfig) {
        Config envDirtyConfig = parseEnvDirtyConfig(envConfig);
        Config mergedConfig = mergeConfig(sinkConfig, envDirtyConfig);
        if (envConfig != null
                && envConfig.hasPath(DIRTY_VALIDATOR_CONFIG_KEY)
                && !mergedConfig.hasPath(DIRTY_VALIDATOR_CONFIG_KEY)) {
            mergedConfig =
                    mergedConfig.withValue(
                            DIRTY_VALIDATOR_CONFIG_KEY,
                            envConfig.getValue(DIRTY_VALIDATOR_CONFIG_KEY));
        }
        return mergedConfig;
    }

    public static boolean hasDirtyHandlingConfig(Config envConfig, Config sinkConfig) {
        Config mergedConfig = getMergedSinkConfigForDirty(envConfig, sinkConfig);
        return mergedConfig.hasPath(DIRTY_COLLECTOR_CONFIG_KEY)
                || mergedConfig.hasPath(DIRTY_VALIDATOR_CONFIG_KEY);
    }

    public static DirtyRecordCollector processConfig(Config envConfig, Config sinkConfig) {
        return processConfig(envConfig, sinkConfig, null);
    }

    public static DirtyRecordCollector processConfig(
            Config envConfig, Config sinkConfig, CatalogTable catalogTable) {
        Config envDirtyConfig = parseEnvDirtyConfig(envConfig);
        Config mergedConfig = mergeConfig(sinkConfig, envDirtyConfig);
        if (envConfig != null
                && envConfig.hasPath(DIRTY_VALIDATOR_CONFIG_KEY)
                && !mergedConfig.hasPath(DIRTY_VALIDATOR_CONFIG_KEY)) {
            log.debug("Merging env dirty validator config into sink config");
            mergedConfig =
                    mergedConfig.withValue(
                            DIRTY_VALIDATOR_CONFIG_KEY,
                            envConfig.getValue(DIRTY_VALIDATOR_CONFIG_KEY));
        }

        DirtyRecordCollector collector = initializeCollector(mergedConfig);

        DirtyDataValidator validator = createValidator(mergedConfig, catalogTable);
        if (validator != null) {
            log.info(
                    "Wrapping collector with ValidatingDirtyRecordCollector (validator={})",
                    validator.getClass().getSimpleName());
            collector = new ValidatingDirtyRecordCollector(collector, validator);
        }

        return collector;
    }

    public static DirtyDataValidator createValidator(Config config, CatalogTable catalogTable) {
        if (config == null || !config.hasPath(DIRTY_VALIDATOR_CONFIG_KEY)) {
            return null;
        }

        try {
            Config validatorConfig = config.getConfig(DIRTY_VALIDATOR_CONFIG_KEY);
            if (!validatorConfig.hasPath("type")) {
                throw new IllegalArgumentException(
                        "dirty.validator is configured but missing required 'type' field.");
            }

            String validatorType = validatorConfig.getString("type");
            DirtyDataValidator validator =
                    DirtyDataValidatorFactory.createValidator(
                            validatorType, validatorConfig, catalogTable);

            if (validator == null) {
                throw new IllegalArgumentException(
                        "Could not resolve dirty.validator type '"
                                + validatorType
                                + "'. Ensure the implementation is on the classpath or registered via SPI.");
            }

            log.info("Successfully created dirty data validator: {}", validatorType);
            return validator;
        } catch (IllegalArgumentException e) {
            throw e;
        } catch (Exception e) {
            String validatorType = config.getConfig(DIRTY_VALIDATOR_CONFIG_KEY).getString("type");
            throw new RuntimeException(
                    "Failed to initialize dirty.validator of type '" + validatorType + "'", e);
        }
    }
}
