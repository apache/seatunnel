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

import org.apache.seatunnel.api.configuration.Option;
import org.apache.seatunnel.api.configuration.Options;
import org.apache.seatunnel.api.sink.multitablesink.MultiTableSink;
import org.apache.seatunnel.api.table.catalog.CatalogTable;

import lombok.extern.slf4j.Slf4j;

/**
 * Resolves optional dirty collector and validator configuration for sink writers.
 *
 * <p>Sink blocks replace their corresponding env blocks independently; explicitly unknown
 * implementations fail instead of silently falling back to a different data-handling policy.
 */
@Slf4j
public class DirtyCollectorConfigProcessor {

    public static final Option<Config> DIRTY_COLLECTOR =
            Options.key("dirty.collector")
                    .objectType(Config.class)
                    .noDefaultValue()
                    .withDescription(
                            "Optional dirty-record collector configuration, inherited from env unless replaced by a sink block.");
    public static final Option<Config> DIRTY_VALIDATOR =
            Options.key("dirty.validator")
                    .objectType(Config.class)
                    .noDefaultValue()
                    .withDescription(
                            "Optional pre-write dirty-record validator configuration; type is required when configured.");

    public static final Option<String> DIRTY_TYPE =
            Options.key("type")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "Required implementation selector within an explicitly configured dirty collector or validator block.");

    private static final String DIRTY_COLLECTOR_CONFIG_KEY = DIRTY_COLLECTOR.key();
    private static final String DIRTY_VALIDATOR_CONFIG_KEY = DIRTY_VALIDATOR.key();

    /**
     * Reads the optional environment collector block without treating an absent or null block as an
     * active policy. Sink-specific override resolution is performed separately.
     */
    public static Config parseEnvDirtyConfig(Config envConfig) {
        if (envConfig != null && envConfig.hasPath(DIRTY_COLLECTOR_CONFIG_KEY)) {
            return envConfig.getConfig(DIRTY_COLLECTOR_CONFIG_KEY);
        }
        return null;
    }

    /**
     * Inherits the environment collector only when the sink does not provide its own value. An
     * explicit null sink value disables inheritance rather than resurrecting the env policy.
     */
    public static Config mergeConfig(Config sinkConfig, Config envDirtyConfig) {
        if (sinkConfig == null) {
            sinkConfig = ConfigFactory.empty();
        }

        // Presence includes an explicit null override, which deliberately disables inheritance.
        if (sinkConfig.hasPathOrNull(DIRTY_COLLECTOR_CONFIG_KEY)) {
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

    /**
     * Initializes the effective collector while keeping the absent-feature path a no-op. A
     * configured but malformed block is rejected by the collector factory.
     */
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

    /**
     * Resolves collector and validator overrides independently, including explicit null values.
     * This shared merge contract keeps client validation and worker initialization consistent.
     */
    public static Config getMergedSinkConfigForDirty(Config envConfig, Config sinkConfig) {
        Config envDirtyConfig = parseEnvDirtyConfig(envConfig);
        Config mergedConfig = mergeConfig(sinkConfig, envDirtyConfig);
        if (envConfig != null
                && envConfig.hasPath(DIRTY_VALIDATOR_CONFIG_KEY)
                && !mergedConfig.hasPathOrNull(DIRTY_VALIDATOR_CONFIG_KEY)) {
            mergedConfig =
                    mergedConfig.withValue(
                            DIRTY_VALIDATOR_CONFIG_KEY,
                            envConfig.getValue(DIRTY_VALIDATOR_CONFIG_KEY));
        }
        return mergedConfig;
    }

    /**
     * Reports whether an effective non-null block is configured, without instantiating plugins.
     * Writer capability checks must still distinguish an explicitly selected no-op collector.
     */
    public static boolean hasDirtyHandlingConfig(Config envConfig, Config sinkConfig) {
        Config mergedConfig = getMergedSinkConfigForDirty(envConfig, sinkConfig);
        return mergedConfig.hasPath(DIRTY_COLLECTOR_CONFIG_KEY)
                || mergedConfig.hasPath(DIRTY_VALIDATOR_CONFIG_KEY);
    }

    /**
     * Resolves a dirty-handling policy without a table-specific schema. The same failure and
     * lifecycle rules apply as for the overload receiving a catalog table.
     */
    public static DirtyRecordCollector processConfig(Config envConfig, Config sinkConfig) {
        return processConfig(envConfig, sinkConfig, null);
    }

    /**
     * Creates an active collector before its validator and closes partial resources on failure.
     * Validation cannot be enabled with a collector that would discard rejected records.
     */
    public static DirtyRecordCollector processConfig(
            Config envConfig, Config sinkConfig, CatalogTable catalogTable) {
        return processConfig(envConfig, sinkConfig, catalogTable, 1);
    }

    /**
     * Rejects active dirty handling for multiple input tables before allocating plugin resources. A
     * shared validator has only one initialization schema, including when templates fan out sinks.
     *
     * @param envConfig environment defaults
     * @param sinkConfig sink overrides, including explicit null values
     * @param catalogTable the single input table used to initialize the validator
     * @param inputTableCount effective input catalog count before any template expansion
     * @return the initialized dirty-handling policy, or the shared no-op collector when disabled
     */
    public static DirtyRecordCollector processConfig(
            Config envConfig, Config sinkConfig, CatalogTable catalogTable, int inputTableCount) {
        Config mergedConfig = getMergedSinkConfigForDirty(envConfig, sinkConfig);
        if (inputTableCount > 1 && hasActiveDirtyHandling(mergedConfig)) {
            throw new IllegalArgumentException(
                    "Dirty data handling requires a single input catalog table; received "
                            + inputTableCount
                            + ".");
        }
        DirtyRecordCollector collector = initializeCollector(mergedConfig);
        DirtyDataValidator validator = null;
        try {
            if (mergedConfig.hasPath(DIRTY_VALIDATOR_CONFIG_KEY)
                    && collector instanceof NoOpDirtyRecordCollector) {
                throw new IllegalArgumentException(
                        "dirty.validator requires an active dirty.collector.");
            }
            validator = createValidator(mergedConfig, catalogTable);
            return validator == null
                    ? collector
                    : new ValidatingDirtyRecordCollector(collector, validator);
        } catch (Throwable failure) {
            if (validator != null) {
                try {
                    validator.close();
                } catch (Throwable closeFailure) {
                    if (closeFailure != failure) {
                        failure.addSuppressed(closeFailure);
                    }
                }
            }
            try {
                collector.close();
            } catch (Throwable closeFailure) {
                if (closeFailure != failure) {
                    failure.addSuppressed(closeFailure);
                }
            }
            if (failure instanceof Error) {
                throw (Error) failure;
            }
            if (failure instanceof RuntimeException) {
                throw (RuntimeException) failure;
            }
            throw new IllegalStateException("Could not initialize dirty data handling", failure);
        }
    }

    /**
     * Rejects sinks that planners aggregate into a multi-table writer before save-mode side
     * effects. Even a single input table uses that writer for multi-table-capable sinks; absent,
     * explicitly null and no-op policies retain their existing planning behavior.
     *
     * @param effectiveConfig sink configuration after environment inheritance
     * @param sink the prepared sink, before invoking its save-mode handler
     */
    public static void validateSinkSupport(Config effectiveConfig, SeaTunnelSink<?, ?, ?, ?> sink) {
        if ((sink instanceof SupportMultiTableSink || sink instanceof MultiTableSink)
                && hasActiveDirtyHandling(effectiveConfig)) {
            throw new IllegalArgumentException(
                    "dirty.collector/dirty.validator requires a synchronous single-table writer "
                            + "without row-error routing or deferred terminal outcomes; "
                            + "multi-table-capable sinks are unsupported even for one input table. "
                            + "Disable dirty handling for this sink before using its save mode.");
        }
    }

    /**
     * Releases a prepared policy when sink planning fails or skips every output. Cleanup preserves
     * the primary planning failure so a secondary close error cannot hide its actionable cause.
     *
     * @param collector the prepared policy, or null when preparation has not completed
     * @param failure the primary planning failure, or null when no output was planned
     */
    public static void closeAfterPlanningFailure(
            DirtyRecordCollector collector, Throwable failure) {
        if (collector == null || collector instanceof NoOpDirtyRecordCollector) {
            return;
        }
        try {
            collector.close();
        } catch (Throwable closeFailure) {
            if (failure == null) {
                throw new IllegalStateException(
                        "Could not close dirty data handling after sink planning", closeFailure);
            }
            if (closeFailure != failure) {
                failure.addSuppressed(closeFailure);
            }
        }
    }

    /**
     * Initializes an explicitly configured validator, preserving missing and blank type errors.
     * Unknown or broken implementations must not silently disable pre-write validation.
     */
    public static DirtyDataValidator createValidator(Config config, CatalogTable catalogTable) {
        if (config == null || !config.hasPath(DIRTY_VALIDATOR_CONFIG_KEY)) {
            return null;
        }

        Config validatorConfig = config.getConfig(DIRTY_VALIDATOR_CONFIG_KEY);
        if (!validatorConfig.hasPath(DIRTY_TYPE.key())) {
            throw new IllegalArgumentException(
                    "dirty.validator is configured but missing required 'type' field.");
        }
        return DirtyDataValidatorFactory.createValidator(
                validatorConfig.getString(DIRTY_TYPE.key()), validatorConfig, catalogTable);
    }
    /**
     * Uses the same declared-policy rules for input cardinality and prepared sink capabilities. No
     * plugin is instantiated here, so planners can reject unsafe save mode without new side
     * effects.
     */
    private static boolean hasActiveDirtyHandling(Config effectiveConfig) {
        Config collectorConfig =
                effectiveConfig.hasPath(DIRTY_COLLECTOR_CONFIG_KEY)
                        ? effectiveConfig.getConfig(DIRTY_COLLECTOR_CONFIG_KEY)
                        : null;
        String type = DirtyRecordCollectorFactory.collectorType(collectorConfig);
        return effectiveConfig.hasPath(DIRTY_VALIDATOR_CONFIG_KEY)
                || !("noop".equals(type) || "none".equals(type));
    }
}
