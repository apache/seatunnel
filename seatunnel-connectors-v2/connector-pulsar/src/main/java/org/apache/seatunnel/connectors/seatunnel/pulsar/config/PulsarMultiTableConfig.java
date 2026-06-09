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

package org.apache.seatunnel.connectors.seatunnel.pulsar.config;

import org.apache.seatunnel.api.common.SeaTunnelAPIErrorCode;
import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.options.table.TableSchemaOptions;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.connectors.seatunnel.pulsar.config.PulsarSourceOptions.StartMode;
import org.apache.seatunnel.connectors.seatunnel.pulsar.config.PulsarSourceOptions.StopMode;
import org.apache.seatunnel.connectors.seatunnel.pulsar.exception.PulsarConnectorException;

import lombok.Getter;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static org.apache.pulsar.shade.org.apache.commons.lang3.StringUtils.isBlank;

/**
 * Normalized Pulsar source configuration that unifies single-table and multi-table options into a
 * consistent runtime model.
 */
@Getter
public class PulsarMultiTableConfig implements Serializable {
    private static final long serialVersionUID = 1L;

    private final List<PulsarTableConfig> tableConfigs;
    private final ReadonlyConfig globalConfig;
    private final boolean isMultiTable;
    private final boolean isTablesConfigs;

    /** Create the normalized config used by Pulsar source runtime components. */
    private PulsarMultiTableConfig(
            List<PulsarTableConfig> tableConfigs,
            ReadonlyConfig globalConfig,
            boolean isMultiTable,
            boolean isTablesConfigs) {
        this.tableConfigs = tableConfigs;
        this.globalConfig = globalConfig;
        this.isMultiTable = isMultiTable;
        this.isTablesConfigs = isTablesConfigs;
    }

    /**
     * Parse source options into a normalized config model.
     *
     * <p>When {@code tables_configs} is absent, single-table options are converted into a one-item
     * table config list so downstream runtime logic can use one unified code path.
     */
    public static PulsarMultiTableConfig of(ReadonlyConfig config) {
        if (config.getOptional(TableSchemaOptions.TABLE_CONFIGS).isPresent()) {
            return parseMultiTable(config);
        } else {
            return parseSingleTable(config);
        }
    }

    private static PulsarMultiTableConfig parseSingleTable(ReadonlyConfig config) {
        String topic = config.getOptional(PulsarSourceOptions.TOPIC).orElse(null);
        String topicPattern = config.getOptional(PulsarSourceOptions.TOPIC_PATTERN).orElse(null);
        String subscriptionName =
                config.getOptional(PulsarSourceOptions.SUBSCRIPTION_NAME).orElse(null);
        String format = config.get(PulsarSourceOptions.FORMAT);
        StartMode startMode = config.get(PulsarSourceOptions.CURSOR_STARTUP_MODE);
        StopMode stopMode = config.get(PulsarSourceOptions.CURSOR_STOP_MODE);
        Long startTimestamp =
                config.getOptional(PulsarSourceOptions.CURSOR_STARTUP_TIMESTAMP).orElse(null);
        Long stopTimestamp =
                config.getOptional(PulsarSourceOptions.CURSOR_STOP_TIMESTAMP).orElse(null);
        PulsarSourceOptions.CursorResetStrategy resetMode =
                config.getOptional(PulsarSourceOptions.CURSOR_RESET_MODE).orElse(null);

        TablePath tablePath = TablePath.of("default", topic != null ? topic : "pulsar_table");
        validateTableConfig(
                -1,
                tablePath,
                topic,
                topicPattern,
                format,
                startMode,
                startTimestamp,
                stopMode,
                stopTimestamp,
                resetMode,
                subscriptionName);
        PulsarTableConfig tableConfig =
                new PulsarTableConfig(
                        tablePath,
                        topic,
                        topicPattern != null ? Pattern.compile(topicPattern) : null,
                        format,
                        startMode,
                        startTimestamp,
                        stopMode,
                        stopTimestamp,
                        resetMode,
                        subscriptionName,
                        config);

        return new PulsarMultiTableConfig(
                java.util.Collections.singletonList(tableConfig), config, false, false);
    }

    private static PulsarMultiTableConfig parseMultiTable(ReadonlyConfig config) {
        List<Map<String, Object>> tablesConfigs = config.get(TableSchemaOptions.TABLE_CONFIGS);
        if (tablesConfigs.isEmpty()) {
            throw new PulsarConnectorException(
                    SeaTunnelAPIErrorCode.CONFIG_VALIDATION_FAILED,
                    "tables_configs cannot be empty");
        }
        List<PulsarTableConfig> tableConfigs = new ArrayList<>();
        Map<String, Integer> tablePathIndexes = new LinkedHashMap<>();
        Map<String, Integer> explicitTopicIndexes = new LinkedHashMap<>();
        Map<String, Integer> topicPatternIndexes = new LinkedHashMap<>();
        List<PatternEntry> knownPatterns = new ArrayList<>();

        for (int i = 0; i < tablesConfigs.size(); i++) {
            Map<String, Object> tableConfigMap = tablesConfigs.get(i);
            ReadonlyConfig tableConfig = mergeWithGlobal(config, tableConfigMap);

            String topic = tableConfig.getOptional(PulsarSourceOptions.TOPIC).orElse(null);
            String topicPatternStr =
                    tableConfig.getOptional(PulsarSourceOptions.TOPIC_PATTERN).orElse(null);

            if ((topic == null && topicPatternStr == null)
                    || (topic != null && topicPatternStr != null)) {
                throw new PulsarConnectorException(
                        SeaTunnelAPIErrorCode.CONFIG_VALIDATION_FAILED,
                        String.format(
                                "tables_configs[%d] must specify exactly one of 'topic' or 'topic-pattern'",
                                i));
            }

            String tablePathStr =
                    tableConfig.getOptional(PulsarSourceOptions.TABLE_PATH).orElse(topic);
            if (topicPatternStr != null && tablePathStr == null) {
                throw new PulsarConnectorException(
                        SeaTunnelAPIErrorCode.CONFIG_VALIDATION_FAILED,
                        String.format(
                                "tables_configs[%d] uses 'topic-pattern' but missing required 'table_path'",
                                i));
            }

            TablePath tablePath = TablePath.of(tablePathStr);
            Integer duplicateTablePathIndex = tablePathIndexes.putIfAbsent(tablePath.toString(), i);
            if (duplicateTablePathIndex != null) {
                throw new PulsarConnectorException(
                        SeaTunnelAPIErrorCode.CONFIG_VALIDATION_FAILED,
                        String.format(
                                "Duplicate table_path '%s' found in tables_configs[%d] and tables_configs[%d]",
                                tablePath, duplicateTablePathIndex, i));
            }

            String format =
                    tableConfig
                            .getOptional(PulsarSourceOptions.FORMAT)
                            .orElse(config.get(PulsarSourceOptions.FORMAT));
            StartMode startMode =
                    tableConfig
                            .getOptional(PulsarSourceOptions.CURSOR_STARTUP_MODE)
                            .orElse(config.get(PulsarSourceOptions.CURSOR_STARTUP_MODE));
            StopMode stopMode =
                    tableConfig
                            .getOptional(PulsarSourceOptions.CURSOR_STOP_MODE)
                            .orElse(config.get(PulsarSourceOptions.CURSOR_STOP_MODE));
            PulsarSourceOptions.CursorResetStrategy resetMode =
                    tableConfig.getOptional(PulsarSourceOptions.CURSOR_RESET_MODE).orElse(null);
            String subscriptionName =
                    tableConfig.getOptional(PulsarSourceOptions.SUBSCRIPTION_NAME).orElse(null);

            Long startTimestamp =
                    tableConfig
                            .getOptional(PulsarSourceOptions.CURSOR_STARTUP_TIMESTAMP)
                            .orElse(
                                    config.getOptional(PulsarSourceOptions.CURSOR_STARTUP_TIMESTAMP)
                                            .orElse(null));
            Long stopTimestamp =
                    tableConfig
                            .getOptional(PulsarSourceOptions.CURSOR_STOP_TIMESTAMP)
                            .orElse(
                                    config.getOptional(PulsarSourceOptions.CURSOR_STOP_TIMESTAMP)
                                            .orElse(null));

            validateTableConfig(
                    i,
                    tablePath,
                    topic,
                    topicPatternStr,
                    format,
                    startMode,
                    startTimestamp,
                    stopMode,
                    stopTimestamp,
                    resetMode,
                    subscriptionName);
            validateTopicOverlap(
                    i,
                    topic,
                    topicPatternStr,
                    explicitTopicIndexes,
                    topicPatternIndexes,
                    knownPatterns);

            PulsarTableConfig pulsarTableConfig =
                    new PulsarTableConfig(
                            tablePath,
                            topic,
                            topicPatternStr != null ? Pattern.compile(topicPatternStr) : null,
                            format,
                            startMode,
                            startTimestamp,
                            stopMode,
                            stopTimestamp,
                            resetMode,
                            subscriptionName,
                            tableConfig);

            tableConfigs.add(pulsarTableConfig);
        }

        return new PulsarMultiTableConfig(tableConfigs, config, tableConfigs.size() > 1, true);
    }

    private static ReadonlyConfig mergeWithGlobal(
            ReadonlyConfig globalConfig, Map<String, Object> tableConfigMap) {
        Map<String, Object> merged = new HashMap<>(globalConfig.getSourceMap());
        merged.remove(TableSchemaOptions.TABLE_CONFIGS.key());
        merged.putAll(tableConfigMap);
        return ReadonlyConfig.fromMap(merged);
    }

    private static void validateTableConfig(
            int index,
            TablePath tablePath,
            String topic,
            String topicPattern,
            String format,
            StartMode startMode,
            Long startTimestamp,
            StopMode stopMode,
            Long stopTimestamp,
            PulsarSourceOptions.CursorResetStrategy resetMode,
            String subscriptionName) {
        String configPrefix =
                index >= 0
                        ? String.format("tables_configs[%d] ('%s')", index, tablePath)
                        : "Pulsar source config";

        if ((topic == null && topicPattern == null) || (topic != null && topicPattern != null)) {
            throw new PulsarConnectorException(
                    SeaTunnelAPIErrorCode.CONFIG_VALIDATION_FAILED,
                    String.format(
                            "%s must specify exactly one of 'topic' or 'topic-pattern'",
                            configPrefix));
        }

        if (isBlank(subscriptionName)) {
            throw new PulsarConnectorException(
                    SeaTunnelAPIErrorCode.CONFIG_VALIDATION_FAILED,
                    String.format("%s must configure 'subscription.name'", configPrefix));
        }

        validateFormat(format, index, tablePath);

        if (startMode == StartMode.TIMESTAMP && startTimestamp == null) {
            throw new PulsarConnectorException(
                    SeaTunnelAPIErrorCode.CONFIG_VALIDATION_FAILED,
                    String.format(
                            "%s has cursor.startup.mode=TIMESTAMP but cursor.startup.timestamp is not set",
                            configPrefix));
        }
        if (startMode == StartMode.SUBSCRIPTION && resetMode == null) {
            throw new PulsarConnectorException(
                    SeaTunnelAPIErrorCode.CONFIG_VALIDATION_FAILED,
                    String.format(
                            "%s has cursor.startup.mode=SUBSCRIPTION but cursor.reset.mode is not set",
                            configPrefix));
        }

        if (stopMode == StopMode.TIMESTAMP && stopTimestamp == null) {
            throw new PulsarConnectorException(
                    SeaTunnelAPIErrorCode.CONFIG_VALIDATION_FAILED,
                    String.format(
                            "%s has cursor.stop.mode=TIMESTAMP but cursor.stop.timestamp is not set",
                            configPrefix));
        }
    }

    private static void validateFormat(String format, int index, TablePath tablePath) {
        String configPrefix =
                index >= 0
                        ? String.format("tables_configs[%d] ('%s')", index, tablePath)
                        : "Pulsar source config";
        String normalized = format.toUpperCase();
        if (!Objects.equals("JSON", normalized) && !Objects.equals("CANAL_JSON", normalized)) {
            throw new PulsarConnectorException(
                    SeaTunnelAPIErrorCode.CONFIG_VALIDATION_FAILED,
                    String.format(
                            "%s uses unsupported format '%s', only JSON and CANAL_JSON are supported",
                            configPrefix, format));
        }
    }

    private static void validateTopicOverlap(
            int index,
            String topic,
            String topicPatternStr,
            Map<String, Integer> explicitTopicIndexes,
            Map<String, Integer> topicPatternIndexes,
            List<PatternEntry> knownPatterns) {
        if (topic != null) {
            for (String topicName : splitTopics(topic)) {
                Integer duplicateIndex = explicitTopicIndexes.putIfAbsent(topicName, index);
                if (duplicateIndex != null) {
                    throw new PulsarConnectorException(
                            SeaTunnelAPIErrorCode.CONFIG_VALIDATION_FAILED,
                            String.format(
                                    "Duplicate topic '%s' found in tables_configs[%d] and tables_configs[%d]",
                                    topicName, duplicateIndex, index));
                }
                for (PatternEntry knownPattern : knownPatterns) {
                    if (knownPattern.pattern.matcher(topicName).matches()) {
                        throw new PulsarConnectorException(
                                SeaTunnelAPIErrorCode.CONFIG_VALIDATION_FAILED,
                                String.format(
                                        "tables_configs[%d] topic '%s' overlaps with topic-pattern in tables_configs[%d]",
                                        index, topicName, knownPattern.index));
                    }
                }
            }
            return;
        }

        Integer duplicatePatternIndex = topicPatternIndexes.putIfAbsent(topicPatternStr, index);
        if (duplicatePatternIndex != null) {
            throw new PulsarConnectorException(
                    SeaTunnelAPIErrorCode.CONFIG_VALIDATION_FAILED,
                    String.format(
                            "Duplicate topic-pattern '%s' found in tables_configs[%d] and tables_configs[%d]",
                            topicPatternStr, duplicatePatternIndex, index));
        }

        Pattern pattern = Pattern.compile(topicPatternStr);
        for (Map.Entry<String, Integer> explicitTopic : explicitTopicIndexes.entrySet()) {
            if (pattern.matcher(explicitTopic.getKey()).matches()) {
                throw new PulsarConnectorException(
                        SeaTunnelAPIErrorCode.CONFIG_VALIDATION_FAILED,
                        String.format(
                                "tables_configs[%d] topic-pattern '%s' overlaps with topic '%s' in tables_configs[%d]",
                                index,
                                topicPatternStr,
                                explicitTopic.getKey(),
                                explicitTopic.getValue()));
            }
        }
        // Different Java regex patterns may still overlap. We intentionally allow that and resolve
        // ownership by tables_configs declaration order in MultiTablePartitionDiscoverer so future
        // topics do not fail the job when they match more than one pattern.
        knownPatterns.add(new PatternEntry(index, pattern));
    }

    private static List<String> splitTopics(String topic) {
        return Arrays.stream(topic.split(","))
                .map(String::trim)
                .filter(value -> !value.isEmpty())
                .collect(Collectors.toList());
    }

    private static final class PatternEntry {
        private final int index;
        private final Pattern pattern;

        private PatternEntry(int index, Pattern pattern) {
            this.index = index;
            this.pattern = pattern;
        }
    }
}
