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

package org.apache.seatunnel.connectors.seatunnel.redis.sink;

import org.apache.seatunnel.shade.org.apache.commons.lang3.StringUtils;

import org.apache.seatunnel.api.common.SeaTunnelAPIErrorCode;
import org.apache.seatunnel.api.serialization.SerializationSchema;
import org.apache.seatunnel.api.sink.SupportMultiTableSinkWriter;
import org.apache.seatunnel.api.table.type.RowKind;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.common.constants.PluginType;
import org.apache.seatunnel.common.exception.CommonErrorCode;
import org.apache.seatunnel.common.utils.JsonUtils;
import org.apache.seatunnel.common.utils.PlaceholderUtils;
import org.apache.seatunnel.connectors.seatunnel.common.sink.AbstractSinkWriter;
import org.apache.seatunnel.connectors.seatunnel.redis.client.RedisClient;
import org.apache.seatunnel.connectors.seatunnel.redis.config.RedisBaseOptions;
import org.apache.seatunnel.connectors.seatunnel.redis.config.RedisDataType;
import org.apache.seatunnel.connectors.seatunnel.redis.config.RedisParameters;
import org.apache.seatunnel.connectors.seatunnel.redis.exception.RedisConnectorException;
import org.apache.seatunnel.format.json.JsonSerializationSchema;
import org.apache.seatunnel.format.text.TextSerializationSchema;

import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

@Slf4j
public class RedisSinkWriter extends AbstractSinkWriter<SeaTunnelRow, Void>
        implements SupportMultiTableSinkWriter<Void> {
    private static final Pattern LEGACY_PLACEHOLDER_PATTERN =
            Pattern.compile("(?<!\\$)\\{([^}]+)\\}");
    private static final Pattern PLACEHOLDER_PATTERN = Pattern.compile("\\$\\{([^}]+)\\}");
    private final SeaTunnelRowType seaTunnelRowType;
    private final RedisParameters redisParameters;
    private final SerializationSchema serializationSchema;
    private final RedisClient redisClient;

    private final int batchSize;

    private final List<RowKind> rowKinds;
    private final List<String> keyBuffer;
    private final List<String> valueBuffer;

    public RedisSinkWriter(SeaTunnelRowType seaTunnelRowType, RedisParameters redisParameters) {
        this.seaTunnelRowType = seaTunnelRowType;
        this.redisParameters = redisParameters;
        this.serializationSchema = createSerializationSchema(redisParameters, seaTunnelRowType);
        this.redisClient = redisParameters.buildRedisClient();
        this.batchSize = redisParameters.getBatchSize();
        this.rowKinds = new ArrayList<>(batchSize);
        this.keyBuffer = new ArrayList<>(batchSize);
        this.valueBuffer = new ArrayList<>(batchSize);
    }

    @Override
    public void write(SeaTunnelRow element) throws IOException {
        rowKinds.add(element.getRowKind());
        List<String> fields = Arrays.asList(seaTunnelRowType.getFieldNames());
        String key = getKey(element, fields);
        keyBuffer.add(key);
        String value = getValue(element, fields);
        valueBuffer.add(value);
        if (keyBuffer.size() >= batchSize) {
            flush();
        }

        log.debug("write redis key: {}, value: {}， rowKind: {}", key, value, element.getRowKind());
    }

    private String getKey(SeaTunnelRow element, List<String> fields) {
        String key = redisParameters.getKeyField();
        Boolean supportCustomKey = redisParameters.getSupportCustomKey();
        if (Boolean.TRUE.equals(supportCustomKey)) {
            return getCustomKey(element, fields, key);
        }
        return getNormalKey(element, fields, key);
    }

    private static String getNormalKey(SeaTunnelRow element, List<String> fields, String keyField) {
        if (fields.contains(keyField)) {
            Object fieldValue = element.getField(fields.indexOf(keyField));
            return fieldValue == null ? "" : fieldValue.toString();
        } else {
            return keyField;
        }
    }

    protected String getCustomKey(SeaTunnelRow element, List<String> fields, String keyField) {
        // First, detect and convert the old format placeholders to the new format
        String normalizedKeyField = normalizePlaceholders(keyField);

        Matcher matcher = PLACEHOLDER_PATTERN.matcher(normalizedKeyField);

        Map<String, String> placeholderValues = new HashMap<>();

        while (matcher.find()) {
            String fieldName = matcher.group(1);
            String fieldValue = getFieldValue(element, fields, fieldName);
            placeholderValues.put(fieldName, fieldValue);
        }

        return placeholderValues.keySet().stream()
                .reduce(
                        normalizedKeyField,
                        (result, placeholderName) -> {
                            return PlaceholderUtils.replacePlaceholders(
                                    result,
                                    placeholderName,
                                    placeholderValues.get(placeholderName),
                                    null);
                        });
    }

    private String getFieldValue(SeaTunnelRow element, List<String> fields, String fieldName) {
        if (fields.contains(fieldName)) {
            Object fieldValue = element.getField(fields.indexOf(fieldName));
            return fieldValue == null ? "" : fieldValue.toString();
        } else {
            // If the field does not exist, return the original field name
            return fieldName;
        }
    }

    private String getValue(SeaTunnelRow element, List<String> fields) {
        String value;
        RedisDataType redisDataType = redisParameters.getRedisDataType();
        if (RedisDataType.HASH.equals(redisDataType)) {
            value = handleHashType(element, fields);
        } else {
            value = handleOtherTypes(element, fields);
        }
        if (value == null) {
            byte[] serialize = serializationSchema.serialize(element);
            value = new String(serialize);
        }
        return value;
    }

    private String handleHashType(SeaTunnelRow element, List<String> fields) {
        String hashKeyField = redisParameters.getHashKeyField();
        String hashValueField = redisParameters.getHashValueField();
        if (StringUtils.isEmpty(hashKeyField)) {
            return null;
        }
        String hashKey;
        if (fields.contains(hashKeyField)) {
            Object hashKeyFieldValue = element.getField(fields.indexOf(hashKeyField));
            hashKey = hashKeyFieldValue == null ? "" : hashKeyFieldValue.toString();
        } else {
            hashKey = hashKeyField;
        }
        String hashValue;
        if (StringUtils.isEmpty(hashValueField)) {
            hashValue = new String(serializationSchema.serialize(element));
        } else {
            if (fields.contains(hashValueField)) {
                Object hashValueFieldValue = element.getField(fields.indexOf(hashValueField));
                hashValue = hashValueFieldValue == null ? "" : hashValueFieldValue.toString();
            } else {
                hashValue = hashValueField;
            }
        }
        Map<String, String> kvMap = new HashMap<>();
        kvMap.put(hashKey, hashValue);
        return JsonUtils.toJsonString(kvMap);
    }

    private String handleOtherTypes(SeaTunnelRow element, List<String> fields) {
        String valueField = redisParameters.getValueField();
        if (StringUtils.isEmpty(valueField)) {
            return null;
        }
        if (fields.contains(valueField)) {
            Object fieldValue = element.getField(fields.indexOf(valueField));
            return fieldValue == null ? "" : fieldValue.toString();
        }
        return valueField;
    }

    private void clearBuffer() {
        rowKinds.clear();
        keyBuffer.clear();
        valueBuffer.clear();
    }

    private void doBatchWrite() {
        RedisDataType redisDataType = redisParameters.getRedisDataType();
        if (RedisDataType.KEY.equals(redisDataType) || RedisDataType.STRING.equals(redisDataType)) {
            redisClient.batchWriteString(
                    rowKinds, keyBuffer, valueBuffer, redisParameters.getExpire());
            return;
        }
        if (RedisDataType.LIST.equals(redisDataType)) {
            redisClient.batchWriteList(
                    rowKinds, keyBuffer, valueBuffer, redisParameters.getExpire());
            return;
        }
        if (RedisDataType.SET.equals(redisDataType)) {
            redisClient.batchWriteSet(
                    rowKinds, keyBuffer, valueBuffer, redisParameters.getExpire());
            return;
        }
        if (RedisDataType.HASH.equals(redisDataType)) {
            redisClient.batchWriteHash(
                    rowKinds, keyBuffer, valueBuffer, redisParameters.getExpire());
            return;
        }
        if (RedisDataType.ZSET.equals(redisDataType)) {
            redisClient.batchWriteZset(
                    rowKinds, keyBuffer, valueBuffer, redisParameters.getExpire());
            return;
        }
        throw new RedisConnectorException(
                CommonErrorCode.UNSUPPORTED_DATA_TYPE,
                "UnSupport redisDataType,only support string,list,hash,set,zset");
    }

    private SerializationSchema createSerializationSchema(
            RedisParameters redisParameters, SeaTunnelRowType rowType) {

        RedisBaseOptions.Format format = redisParameters.getFormat();

        switch (format) {
            case JSON:
                return new JsonSerializationSchema(rowType);
            case TEXT:
                String fieldDelimiter = redisParameters.getFieldDelimiter();
                return TextSerializationSchema.builder()
                        .seaTunnelRowType(rowType)
                        .delimiter(fieldDelimiter)
                        .build();
            default:
                throw new RedisConnectorException(
                        SeaTunnelAPIErrorCode.CONFIG_VALIDATION_FAILED,
                        String.format(
                                "PluginName: %s, PluginType: %s, Message: %s",
                                RedisBaseOptions.CONNECTOR_IDENTITY,
                                PluginType.SINK,
                                "Unsupported format: " + format));
        }
    }

    private String normalizePlaceholders(String input) {
        if (input == null) {
            return input;
        }

        Matcher legacyMatcher = LEGACY_PLACEHOLDER_PATTERN.matcher(input);
        if (legacyMatcher.find()) {
            // Convert legacy format {fieldName} to ${fieldName}
            return legacyMatcher.replaceAll("\\$\\{$1\\}");
        }

        return input;
    }

    @Override
    public void close() throws IOException {
        flush();
    }

    @Override
    public Optional<Void> prepareCommit() {
        flush();
        return Optional.empty();
    }

    private synchronized void flush() {
        if (!keyBuffer.isEmpty()) {
            doBatchWrite();
            clearBuffer();
        }
    }
}
