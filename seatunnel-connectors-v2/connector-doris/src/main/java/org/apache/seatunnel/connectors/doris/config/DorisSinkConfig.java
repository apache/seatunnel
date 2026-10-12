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

package org.apache.seatunnel.connectors.doris.config;

import org.apache.seatunnel.shade.com.typesafe.config.Config;
import org.apache.seatunnel.shade.org.apache.commons.lang3.StringUtils;

import org.apache.seatunnel.api.common.SeaTunnelAPIErrorCode;
import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.sink.DataSaveMode;
import org.apache.seatunnel.connectors.doris.exception.DorisConnectorException;

import lombok.Getter;
import lombok.Setter;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;

import java.io.Serializable;
import java.time.DateTimeException;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import static org.apache.seatunnel.connectors.doris.config.DorisBaseOptions.DATABASE;
import static org.apache.seatunnel.connectors.doris.config.DorisBaseOptions.DORIS_BATCH_SIZE;
import static org.apache.seatunnel.connectors.doris.config.DorisBaseOptions.FENODES;
import static org.apache.seatunnel.connectors.doris.config.DorisBaseOptions.PASSWORD;
import static org.apache.seatunnel.connectors.doris.config.DorisBaseOptions.QUERY_PORT;
import static org.apache.seatunnel.connectors.doris.config.DorisBaseOptions.TABLE;
import static org.apache.seatunnel.connectors.doris.config.DorisBaseOptions.USERNAME;
import static org.apache.seatunnel.connectors.doris.config.DorisSinkOptions.BENODES;
import static org.apache.seatunnel.connectors.doris.config.DorisSinkOptions.CASE_SENSITIVE;
import static org.apache.seatunnel.connectors.doris.config.DorisSinkOptions.DATA_SAVE_MODE;
import static org.apache.seatunnel.connectors.doris.config.DorisSinkOptions.DIRECT_TO_BE;
import static org.apache.seatunnel.connectors.doris.config.DorisSinkOptions.DORIS_SINK_CONFIG_PREFIX;
import static org.apache.seatunnel.connectors.doris.config.DorisSinkOptions.NEEDS_UNSUPPORTED_TYPE_CASTING;
import static org.apache.seatunnel.connectors.doris.config.DorisSinkOptions.SAVE_MODE_CREATE_TEMPLATE;
import static org.apache.seatunnel.connectors.doris.config.DorisSinkOptions.SINK_BUFFER_COUNT;
import static org.apache.seatunnel.connectors.doris.config.DorisSinkOptions.SINK_BUFFER_SIZE;
import static org.apache.seatunnel.connectors.doris.config.DorisSinkOptions.SINK_CHECK_INTERVAL;
import static org.apache.seatunnel.connectors.doris.config.DorisSinkOptions.SINK_DATETIME_TIMEZONE;
import static org.apache.seatunnel.connectors.doris.config.DorisSinkOptions.SINK_ENABLE_2PC;
import static org.apache.seatunnel.connectors.doris.config.DorisSinkOptions.SINK_ENABLE_DELETE;
import static org.apache.seatunnel.connectors.doris.config.DorisSinkOptions.SINK_LABEL_PREFIX;
import static org.apache.seatunnel.connectors.doris.config.DorisSinkOptions.SINK_MAX_RETRIES;

@Slf4j
@Setter
@Getter
@ToString
public class DorisSinkConfig implements Serializable {

    // common option
    private String frontends;
    private String backends;
    private String database;
    private String table;
    private String username;
    private String password;
    private Integer queryPort;
    private int batchSize;

    // sink option
    private Boolean enable2PC;
    private Boolean enableDelete;
    private String labelPrefix;
    private Integer checkInterval;
    private Integer maxRetries;
    private Integer bufferSize;
    private Integer bufferCount;
    private Properties streamLoadProps;
    private List<String> partitions = Collections.emptyList();
    private boolean directToBe;
    private boolean needsUnsupportedTypeCasting;
    private boolean caseSensitive;
    private ZoneId datetimeTimezone;

    // create table option
    private String createTableTemplate;

    public static DorisSinkConfig of(Config pluginConfig) {
        return of(ReadonlyConfig.fromConfig(pluginConfig));
    }

    public static DorisSinkConfig of(ReadonlyConfig config) {

        DorisSinkConfig dorisSinkConfig = new DorisSinkConfig();

        // common option
        dorisSinkConfig.setFrontends(config.get(FENODES));
        dorisSinkConfig.setBackends(config.getOptional(BENODES).orElse(null));
        dorisSinkConfig.setUsername(config.get(USERNAME));
        dorisSinkConfig.setPassword(config.get(PASSWORD));
        dorisSinkConfig.setQueryPort(config.get(QUERY_PORT));
        dorisSinkConfig.setStreamLoadProps(parseStreamLoadProperties(config));
        if (config.get(DATA_SAVE_MODE) == DataSaveMode.DROP_DATA) {
            List<String> partitions = parseDropDataPartitions(dorisSinkConfig.getStreamLoadProps());
            dorisSinkConfig.setPartitions(partitions);
            if (!partitions.isEmpty()) {
                dorisSinkConfig
                        .getStreamLoadProps()
                        .setProperty("partitions", String.join(",", partitions));
            }
        }
        dorisSinkConfig.setDatabase(config.get(DATABASE));
        dorisSinkConfig.setTable(config.get(TABLE));
        dorisSinkConfig.setBatchSize(config.get(DORIS_BATCH_SIZE));

        // sink option
        dorisSinkConfig.setEnable2PC(config.get(SINK_ENABLE_2PC));
        dorisSinkConfig.setLabelPrefix(config.get(SINK_LABEL_PREFIX));
        dorisSinkConfig.setCheckInterval(config.get(SINK_CHECK_INTERVAL));
        dorisSinkConfig.setMaxRetries(config.get(SINK_MAX_RETRIES));
        dorisSinkConfig.setBufferSize(config.get(SINK_BUFFER_SIZE));
        dorisSinkConfig.setBufferCount(config.get(SINK_BUFFER_COUNT));
        dorisSinkConfig.setEnableDelete(config.get(SINK_ENABLE_DELETE));
        dorisSinkConfig.setDirectToBe(config.get(DIRECT_TO_BE));
        dorisSinkConfig.setNeedsUnsupportedTypeCasting(config.get(NEEDS_UNSUPPORTED_TYPE_CASTING));
        dorisSinkConfig.setCaseSensitive(config.get(CASE_SENSITIVE));
        dorisSinkConfig.setDatetimeTimezone(parseDatetimeTimezone(config));
        // create table option
        dorisSinkConfig.setCreateTableTemplate(config.get(SAVE_MODE_CREATE_TEMPLATE));

        if (!dorisSinkConfig.isDirectToBe()
                && StringUtils.isNotBlank(dorisSinkConfig.getBackends())) {
            log.info("Option 'benodes' is configured but inactive because 'direct_to_be=false'.");
        }

        return dorisSinkConfig;
    }

    private static ZoneId parseDatetimeTimezone(ReadonlyConfig config) {
        if (!config.getOptional(SINK_DATETIME_TIMEZONE).isPresent()) {
            return null;
        }
        String zoneId = config.get(SINK_DATETIME_TIMEZONE).trim();
        if (zoneId.isEmpty()) {
            return null;
        }
        try {
            return ZoneId.of(zoneId);
        } catch (DateTimeException e) {
            throw new DorisConnectorException(
                    SeaTunnelAPIErrorCode.CONFIG_VALIDATION_FAILED,
                    "PluginName: Doris, Message: Option 'sink.datetime-timezone' is not a valid ZoneId ID: "
                            + zoneId,
                    e);
        }
    }

    private static Properties parseStreamLoadProperties(ReadonlyConfig config) {
        Properties streamLoadProps = new Properties();
        if (config.getOptional(DORIS_SINK_CONFIG_PREFIX).isPresent()) {
            Map<String, String> map = config.getOptional(DORIS_SINK_CONFIG_PREFIX).get();
            map.forEach(
                    (key, value) -> {
                        streamLoadProps.put(key.toLowerCase(), value);
                    });
        }
        return streamLoadProps;
    }
    /**
     * Parses a comma-separated partition list for partition-scoped DROP_DATA cleanup.
     *
     * @throws DorisConnectorException if a partition name is blank or duplicated
     */
    private static List<String> parseDropDataPartitions(Properties streamLoadProperties) {
        String configuredPartitions = streamLoadProperties.getProperty("partitions");
        if (configuredPartitions == null) {
            return Collections.emptyList();
        }

        String[] values = configuredPartitions.split(",", -1);
        List<String> partitions = new ArrayList<>(values.length);
        Set<String> uniquePartitions = new HashSet<>();
        for (String value : values) {
            String partition = value.trim();
            if (partition.isEmpty()) {
                throw new DorisConnectorException(
                        SeaTunnelAPIErrorCode.CONFIG_VALIDATION_FAILED,
                        "PluginName: Doris, Message: 'doris.config.partitions' cannot contain blank partition names.");
            }
            if (!uniquePartitions.add(partition)) {
                throw new DorisConnectorException(
                        SeaTunnelAPIErrorCode.CONFIG_VALIDATION_FAILED,
                        "PluginName: Doris, Message: 'doris.config.partitions' cannot contain duplicate partition names.");
            }
            partitions.add(partition);
        }
        return Collections.unmodifiableList(partitions);
    }
}
