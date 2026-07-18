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

package org.apache.seatunnel.connectors.seatunnel.hugegraph.config;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.connectors.seatunnel.hugegraph.exception.HugeGraphConnectorErrorCode;
import org.apache.seatunnel.connectors.seatunnel.hugegraph.exception.HugeGraphConnectorException;

import lombok.Data;

import java.io.Serializable;
import java.time.DateTimeException;
import java.time.ZoneId;
import java.util.Collections;
import java.util.List;
import java.util.Map;

@Data
public class HugeGraphSourceConfig implements Serializable {

    private static final long serialVersionUID = 1L;

    private HugeGraphConnectionConfig connectionConfig;
    private String label;
    private MappingConfig.LabelType labelType;
    // Read-all-labels mode: when true, {@code label}/{@code schema} are null and {@code labels}
    // holds every label of {@code labelType} to read (one produced table each).
    private boolean readAllLabels;
    private List<String> labels;
    private SeaTunnelRowType schema;
    private int pageSize;
    private long splitSize;
    private String timeZone;
    // Optional server-side property equality conditions; null/empty = read all elements.
    private Map<String, Object> filter;

    public static HugeGraphSourceConfig of(ReadonlyConfig config, SeaTunnelRowType schema) {
        HugeGraphSourceConfig sourceConfig = new HugeGraphSourceConfig();
        sourceConfig.setConnectionConfig(HugeGraphConnectionConfig.of(config));
        sourceConfig.setReadAllLabels(false);
        sourceConfig.setLabel(config.get(HugeGraphSourceOptions.LABEL));
        sourceConfig.setLabels(Collections.singletonList(config.get(HugeGraphSourceOptions.LABEL)));
        sourceConfig.setLabelType(
                config.getOptional(HugeGraphSourceOptions.LABEL_TYPE)
                        .orElse(HugeGraphSourceOptions.LABEL_TYPE.defaultValue()));
        sourceConfig.setSchema(schema);
        sourceConfig.setPageSize(
                config.getOptional(HugeGraphSourceOptions.PAGE_SIZE)
                        .orElse(HugeGraphSourceOptions.PAGE_SIZE.defaultValue()));
        sourceConfig.setSplitSize(
                config.getOptional(HugeGraphSourceOptions.SPLIT_SIZE)
                        .orElse(HugeGraphSourceOptions.SPLIT_SIZE.defaultValue()));
        config.getOptional(HugeGraphSourceOptions.TIME_ZONE).ifPresent(sourceConfig::setTimeZone);
        config.getOptional(HugeGraphSourceOptions.FILTER).ifPresent(sourceConfig::setFilter);
        validate(sourceConfig);
        return sourceConfig;
    }

    /**
     * Read-all-labels construction: no single {@code label} and no user {@code schema}/{@code
     * filter}; the labels are discovered from the server and each gets its own auto-discovered row
     * type. See {@link
     * org.apache.seatunnel.connectors.seatunnel.hugegraph.source.HugeGraphSourceFactory}.
     */
    public static HugeGraphSourceConfig ofReadAll(ReadonlyConfig config, List<String> labels) {
        HugeGraphSourceConfig sourceConfig = new HugeGraphSourceConfig();
        sourceConfig.setConnectionConfig(HugeGraphConnectionConfig.of(config));
        sourceConfig.setReadAllLabels(true);
        sourceConfig.setLabel(null);
        sourceConfig.setLabels(labels);
        sourceConfig.setLabelType(
                config.getOptional(HugeGraphSourceOptions.LABEL_TYPE)
                        .orElse(HugeGraphSourceOptions.LABEL_TYPE.defaultValue()));
        sourceConfig.setSchema(null);
        sourceConfig.setPageSize(
                config.getOptional(HugeGraphSourceOptions.PAGE_SIZE)
                        .orElse(HugeGraphSourceOptions.PAGE_SIZE.defaultValue()));
        sourceConfig.setSplitSize(
                config.getOptional(HugeGraphSourceOptions.SPLIT_SIZE)
                        .orElse(HugeGraphSourceOptions.SPLIT_SIZE.defaultValue()));
        config.getOptional(HugeGraphSourceOptions.TIME_ZONE).ifPresent(sourceConfig::setTimeZone);
        validate(sourceConfig);
        return sourceConfig;
    }

    private static void validate(HugeGraphSourceConfig sourceConfig) {
        int pageSize = sourceConfig.getPageSize();
        if (pageSize < HugeGraphSourceOptions.MIN_PAGE_SIZE
                || pageSize > HugeGraphSourceOptions.MAX_PAGE_SIZE) {
            throw new HugeGraphConnectorException(
                    HugeGraphConnectorErrorCode.ILLEGAL_CONFIG_ARGUMENT,
                    String.format(
                            "Option 'page_size' must be in range [%s, %s], but got %s",
                            HugeGraphSourceOptions.MIN_PAGE_SIZE,
                            HugeGraphSourceOptions.MAX_PAGE_SIZE,
                            pageSize));
        }

        if (sourceConfig.getSplitSize() < HugeGraphSourceOptions.MIN_SPLIT_SIZE) {
            throw new HugeGraphConnectorException(
                    HugeGraphConnectorErrorCode.ILLEGAL_CONFIG_ARGUMENT,
                    String.format(
                            "Option 'split_size' must be at least %s bytes (the HugeGraph minimum "
                                    + "shard size); a smaller value would split the keyspace into a "
                                    + "huge number of shards and risk OOM / oversized checkpoints. "
                                    + "Got %s.",
                            HugeGraphSourceOptions.MIN_SPLIT_SIZE, sourceConfig.getSplitSize()));
        }

        if (sourceConfig.isReadAllLabels()) {
            // Read-all mode discovers labels from the server; there must be at least one, and no
            // single user schema applies (each label gets its own auto-discovered row type).
            if (sourceConfig.getLabels() == null || sourceConfig.getLabels().isEmpty()) {
                throw new HugeGraphConnectorException(
                        HugeGraphConnectorErrorCode.ILLEGAL_CONFIG_ARGUMENT,
                        "Read-all-labels mode requires at least one label, but none were discovered.");
            }
        } else if (sourceConfig.getSchema() == null) {
            // Single-label mode: schema must be present, but an empty fields block is valid — a
            // property-less label (e.g. a pure relationship edge, or a vertex with no properties)
            // is exported as just the reserved columns (~id/~label/…). Requiring a fake property
            // would make such labels unreadable.
            throw new HugeGraphConnectorException(
                    HugeGraphConnectorErrorCode.ILLEGAL_CONFIG_ARGUMENT,
                    "Option 'schema' is required (use 'schema = { fields {} }' for a label with no properties)");
        }
        if (sourceConfig.getTimeZone() != null) {
            try {
                ZoneId.of(sourceConfig.getTimeZone());
            } catch (DateTimeException e) {
                throw new HugeGraphConnectorException(
                        HugeGraphConnectorErrorCode.ILLEGAL_CONFIG_ARGUMENT,
                        String.format(
                                "Option 'time_zone' must be a valid ZoneId, but got '%s'",
                                sourceConfig.getTimeZone()),
                        e);
            }
        }
    }
}
