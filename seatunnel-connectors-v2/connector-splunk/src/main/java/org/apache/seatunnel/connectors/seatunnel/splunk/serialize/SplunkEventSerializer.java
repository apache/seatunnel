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

package org.apache.seatunnel.connectors.seatunnel.splunk.serialize;

import org.apache.seatunnel.shade.com.fasterxml.jackson.core.JsonGenerator;
import org.apache.seatunnel.shade.com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.seatunnel.shade.org.apache.commons.lang3.StringUtils;

import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.api.table.type.SqlType;
import org.apache.seatunnel.connectors.seatunnel.splunk.config.SplunkSinkConfig;
import org.apache.seatunnel.connectors.seatunnel.splunk.exception.SplunkConnectorErrorCode;
import org.apache.seatunnel.connectors.seatunnel.splunk.exception.SplunkConnectorException;
import org.apache.seatunnel.format.json.JsonSerializationSchema;

import java.io.Serializable;
import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.Set;

/**
 * Serializes a {@link SeaTunnelRow} into a single Splunk HTTP Event Collector event envelope.
 *
 * <p>The envelope carries the Splunk metadata fields ({@code time}, {@code host}, {@code source},
 * {@code sourcetype}, {@code index}) alongside the row itself under {@code event}. Metadata fields
 * that are not configured are omitted so that the collector falls back to the defaults of the HEC
 * token rather than being overridden with nulls.
 */
public class SplunkEventSerializer implements Serializable {

    private static final long serialVersionUID = 1L;

    private static final String FIELD_TIME = "time";
    private static final String FIELD_HOST = "host";
    private static final String FIELD_SOURCE = "source";
    private static final String FIELD_SOURCE_TYPE = "sourcetype";
    private static final String FIELD_INDEX = "index";
    private static final String FIELD_EVENT = "event";

    /** Scale that turns epoch milliseconds into epoch seconds without losing precision. */
    private static final int EPOCH_SECONDS_SCALE = 3;

    /** Upstream types accepted for {@code time_field}. */
    private static final Set<SqlType> SUPPORTED_TIME_TYPES =
            EnumSet.of(SqlType.TIMESTAMP, SqlType.TIMESTAMP_TZ, SqlType.BIGINT);

    private final SplunkSinkConfig config;
    private final JsonSerializationSchema jsonSerializationSchema;
    private final ObjectMapper objectMapper;

    /** Index of the row field feeding the Splunk {@code host} metadata, or -1 when unconfigured. */
    private final int hostFieldIndex;

    /** Index of the row field feeding the Splunk {@code time} metadata, or -1 when unconfigured. */
    private final int timeFieldIndex;

    private final SqlType timeFieldType;

    public SplunkEventSerializer(SeaTunnelRowType rowType, SplunkSinkConfig config) {
        this.config = config;
        this.jsonSerializationSchema = new JsonSerializationSchema(rowType);
        this.objectMapper = new ObjectMapper();
        // The collector reads `time` as epoch seconds. Written as a double, any realistic epoch
        // renders in scientific notation (1.786969845123E9), which Splunk does not accept, so the
        // value is carried as a BigDecimal and written in plain notation.
        this.objectMapper.configure(JsonGenerator.Feature.WRITE_BIGDECIMAL_AS_PLAIN, true);
        this.hostFieldIndex = resolveFieldIndex(rowType, config.getHostField(), "host_field");
        this.timeFieldIndex = resolveFieldIndex(rowType, config.getTimeField(), "time_field");
        this.timeFieldType =
                timeFieldIndex < 0 ? null : rowType.getFieldType(timeFieldIndex).getSqlType();

        if (timeFieldType != null && !SUPPORTED_TIME_TYPES.contains(timeFieldType)) {
            throw new SplunkConnectorException(
                    SplunkConnectorErrorCode.INVALID_CONFIG,
                    String.format(
                            "Option 'time_field' refers to field '%s' of type %s, which cannot be used as "
                                    + "a Splunk event timestamp. Supported types are TIMESTAMP, TIMESTAMP_TZ "
                                    + "and BIGINT (epoch milliseconds).",
                            config.getTimeField(), timeFieldType));
        }
    }

    /** Serializes one row into the HEC event envelope, ready to be concatenated into a batch. */
    public String serialize(SeaTunnelRow row) {
        ObjectNode envelope = objectMapper.createObjectNode();

        BigDecimal time = extractTime(row);
        if (time != null) {
            envelope.put(FIELD_TIME, time);
        }

        String host = extractHost(row);
        if (StringUtils.isNotEmpty(host)) {
            envelope.put(FIELD_HOST, host);
        }
        if (StringUtils.isNotEmpty(config.getSource())) {
            envelope.put(FIELD_SOURCE, config.getSource());
        }
        if (StringUtils.isNotEmpty(config.getSourceType())) {
            envelope.put(FIELD_SOURCE_TYPE, config.getSourceType());
        }
        if (StringUtils.isNotEmpty(config.getIndex())) {
            envelope.put(FIELD_INDEX, config.getIndex());
        }

        // JsonSerializationSchema reuses its internal node, so the envelope must be written out
        // before the next row is converted. serialize() does exactly that, one row at a time.
        envelope.set(FIELD_EVENT, jsonSerializationSchema.convert(row));

        try {
            return objectMapper.writeValueAsString(envelope);
        } catch (JsonProcessingException e) {
            throw new SplunkConnectorException(
                    SplunkConnectorErrorCode.SERIALIZE_EVENT_FAILED,
                    "Failed to write the Splunk HEC event envelope as JSON",
                    e);
        }
    }

    /**
     * Converts the configured time field into the epoch-seconds representation the collector
     * expects, keeping millisecond precision. Returns {@code null} when no time field is configured
     * or the row carries no value, in which case Splunk stamps the event on ingest.
     */
    private BigDecimal extractTime(SeaTunnelRow row) {
        if (timeFieldIndex < 0) {
            return null;
        }
        Object value = row.getField(timeFieldIndex);
        if (value == null) {
            return null;
        }
        switch (timeFieldType) {
            case TIMESTAMP:
                return epochSeconds(
                        ((LocalDateTime) value).toInstant(ZoneOffset.UTC).toEpochMilli());
            case TIMESTAMP_TZ:
                return epochSeconds(((OffsetDateTime) value).toInstant().toEpochMilli());
            case BIGINT:
                return epochSeconds(((Number) value).longValue());
            default:
                // Unreachable: the constructor rejects every other type.
                throw new SplunkConnectorException(
                        SplunkConnectorErrorCode.SERIALIZE_EVENT_FAILED,
                        "Unsupported time field type: " + timeFieldType);
        }
    }

    /** Rescales epoch milliseconds to epoch seconds, keeping the milliseconds as decimals. */
    private static BigDecimal epochSeconds(long epochMillis) {
        return BigDecimal.valueOf(epochMillis, EPOCH_SECONDS_SCALE);
    }

    private String extractHost(SeaTunnelRow row) {
        if (hostFieldIndex < 0) {
            return config.getHost();
        }
        Object value = row.getField(hostFieldIndex);
        return value == null ? config.getHost() : value.toString();
    }

    /** Resolves a configured field name to its row index, failing with the available names. */
    private static int resolveFieldIndex(
            SeaTunnelRowType rowType, String fieldName, String optionKey) {
        if (StringUtils.isBlank(fieldName)) {
            return -1;
        }
        int index = rowType.indexOf(fieldName, false);
        if (index < 0) {
            throw new SplunkConnectorException(
                    SplunkConnectorErrorCode.INVALID_CONFIG,
                    String.format(
                            "Option '%s' refers to field '%s', which does not exist upstream. "
                                    + "Available fields are %s.",
                            optionKey, fieldName, Arrays.toString(rowType.getFieldNames())));
        }
        return index;
    }
}
