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

package org.apache.seatunnel.connectors.seatunnel.natsjetstream.sink;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.type.MapType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.api.table.type.SqlType;
import org.apache.seatunnel.common.exception.CommonErrorCodeDeprecated;
import org.apache.seatunnel.connectors.seatunnel.natsjetstream.config.NatsJetStreamMessageFormat;
import org.apache.seatunnel.connectors.seatunnel.natsjetstream.config.NatsJetStreamSinkOptions;
import org.apache.seatunnel.connectors.seatunnel.natsjetstream.exception.NatsJetStreamConnectorException;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

final class NatsJetStreamSinkValidator {

    private static final Set<String> SUPPORTED_NATIVE_MAPPING_KEYS =
            new HashSet<>(
                    Arrays.asList(
                            NatsJetStreamSinkOptions.NATIVE_MAPPING_ID,
                            NatsJetStreamSinkOptions.NATIVE_MAPPING_SUBJECT,
                            NatsJetStreamSinkOptions.NATIVE_MAPPING_HEADERS,
                            NatsJetStreamSinkOptions.NATIVE_MAPPING_DATA));

    private NatsJetStreamSinkValidator() {}

    static void validate(ReadonlyConfig pluginConfig, CatalogTable catalogTable) {
        requireNonBlank(pluginConfig, NatsJetStreamSinkOptions.URL);
        validateAuthentication(pluginConfig);
        NatsJetStreamMessageFormat format = pluginConfig.get(NatsJetStreamSinkOptions.FORMAT);
        if (format == null) {
            throw invalidOption("format", "must be one of: json, native");
        }
        if (format == NatsJetStreamMessageFormat.JSON) {
            requireNonBlank(pluginConfig, NatsJetStreamSinkOptions.SUBJECT);
            return;
        }
        validateNativeMapping(pluginConfig, catalogTable);
    }

    private static void validateAuthentication(ReadonlyConfig pluginConfig) {
        Optional<String> username = pluginConfig.getOptional(NatsJetStreamSinkOptions.USERNAME);
        Optional<String> password = pluginConfig.getOptional(NatsJetStreamSinkOptions.PASSWORD);
        Optional<String> token = pluginConfig.getOptional(NatsJetStreamSinkOptions.TOKEN);

        boolean hasUsername = username.map(NatsJetStreamSinkValidator::isNotBlank).orElse(false);
        boolean hasPassword = password.map(NatsJetStreamSinkValidator::isNotBlank).orElse(false);
        boolean hasToken = token.map(NatsJetStreamSinkValidator::isNotBlank).orElse(false);

        if (hasUsername != hasPassword) {
            throw invalidOption(
                    hasUsername
                            ? NatsJetStreamSinkOptions.PASSWORD.key()
                            : NatsJetStreamSinkOptions.USERNAME.key(),
                    "must be configured together with `"
                            + (hasUsername
                                    ? NatsJetStreamSinkOptions.USERNAME.key()
                                    : NatsJetStreamSinkOptions.PASSWORD.key())
                            + "`");
        }
        if (hasToken && hasUsername) {
            throw invalidOption(
                    NatsJetStreamSinkOptions.TOKEN.key(),
                    "cannot be configured together with `username` and `password`");
        }
    }

    private static void validateNativeMapping(
            ReadonlyConfig pluginConfig, CatalogTable catalogTable) {
        Map<String, String> nativeFields = pluginConfig.get(NatsJetStreamSinkOptions.NATIVE_FIELDS);
        if (nativeFields == null || nativeFields.isEmpty()) {
            throw invalidOption(
                    NatsJetStreamSinkOptions.NATIVE_FIELDS.key(),
                    "must define at least the `data` field mapping in native format");
        }

        for (String mappingKey : nativeFields.keySet()) {
            if (!SUPPORTED_NATIVE_MAPPING_KEYS.contains(mappingKey)) {
                throw invalidOption(
                        NatsJetStreamSinkOptions.NATIVE_FIELDS.key(),
                        "contains unsupported mapping key `" + mappingKey + "`");
            }
        }

        String dataField = nativeFields.get(NatsJetStreamSinkOptions.NATIVE_MAPPING_DATA);
        if (!isNotBlank(dataField)) {
            throw invalidOption(
                    NatsJetStreamSinkOptions.NATIVE_FIELDS.key(),
                    "must define a non-empty mapping for `data`");
        }

        SeaTunnelRowType rowType = catalogTable.getSeaTunnelRowType();
        validateMappedFieldType(
                rowType, NatsJetStreamSinkOptions.NATIVE_MAPPING_DATA, dataField, SqlType.BYTES);

        validateOptionalStringField(
                rowType, nativeFields, NatsJetStreamSinkOptions.NATIVE_MAPPING_ID);
        validateOptionalHeadersField(rowType, nativeFields);

        String subject = pluginConfig.getOptional(NatsJetStreamSinkOptions.SUBJECT).orElse(null);
        boolean hasSubjectFallback = isNotBlank(subject);
        boolean hasSubjectField =
                isNotBlank(nativeFields.get(NatsJetStreamSinkOptions.NATIVE_MAPPING_SUBJECT));
        if (!hasSubjectFallback && !hasSubjectField) {
            throw invalidOption(
                    NatsJetStreamSinkOptions.SUBJECT.key(),
                    "or `native_format_fields.subject` must be configured in native format");
        }
        validateSubjectField(rowType, nativeFields, hasSubjectFallback);
    }

    /**
     * Validates the native {@code subject} field mapping. Unlike other optional mappings, the
     * subject mapping is required to resolve to an existing {@code STRING} field whenever no
     * sink-level {@code subject} fallback is configured. Without this check, a subject mapping that
     * points to a non-existent field silently resolves to {@code null} at runtime and every row is
     * rejected by the serializer.
     */
    private static void validateSubjectField(
            SeaTunnelRowType rowType,
            Map<String, String> nativeFields,
            boolean hasSubjectFallback) {
        String fieldName = nativeFields.get(NatsJetStreamSinkOptions.NATIVE_MAPPING_SUBJECT);
        if (!isNotBlank(fieldName)) {
            return;
        }
        int fieldIndex = rowType.indexOf(fieldName, false);
        if (fieldIndex < 0) {
            if (!hasSubjectFallback) {
                throw invalidField(
                        fieldName,
                        "mapped from `"
                                + NatsJetStreamSinkOptions.NATIVE_FIELDS.key()
                                + ".subject` does not exist in table schema");
            }
            return;
        }
        SeaTunnelDataType<?> fieldType = rowType.getFieldType(fieldIndex);
        if (fieldType.getSqlType() != SqlType.STRING) {
            throw invalidField(fieldName, "must use `STRING` type for native mapping `subject`");
        }
    }

    private static void validateOptionalStringField(
            SeaTunnelRowType rowType, Map<String, String> nativeFields, String mappingKey) {
        String fieldName = nativeFields.get(mappingKey);
        if (!isNotBlank(fieldName)) {
            return;
        }
        int fieldIndex = rowType.indexOf(fieldName, false);
        if (fieldIndex < 0) {
            return;
        }
        SeaTunnelDataType<?> fieldType = rowType.getFieldType(fieldIndex);
        if (fieldType.getSqlType() != SqlType.STRING) {
            throw invalidField(
                    fieldName, "must use `STRING` type for native mapping `" + mappingKey + "`");
        }
    }

    private static void validateOptionalHeadersField(
            SeaTunnelRowType rowType, Map<String, String> nativeFields) {
        String fieldName = nativeFields.get(NatsJetStreamSinkOptions.NATIVE_MAPPING_HEADERS);
        if (!isNotBlank(fieldName)) {
            return;
        }
        int fieldIndex = rowType.indexOf(fieldName, false);
        if (fieldIndex < 0) {
            return;
        }
        SeaTunnelDataType<?> fieldType = rowType.getFieldType(fieldIndex);
        if (!(fieldType instanceof MapType)) {
            throw invalidField(fieldName, "must use MAP<STRING, STRING> type");
        }
        MapType<?, ?> mapType = (MapType<?, ?>) fieldType;
        if (mapType.getKeyType().getSqlType() != SqlType.STRING
                || mapType.getValueType().getSqlType() != SqlType.STRING) {
            throw invalidField(fieldName, "must use MAP<STRING, STRING> type");
        }
    }

    private static void validateMappedFieldType(
            SeaTunnelRowType rowType, String mappingKey, String fieldName, SqlType expectedType) {
        int fieldIndex = rowType.indexOf(fieldName, false);
        if (fieldIndex < 0) {
            throw invalidField(
                    fieldName,
                    "mapped from `"
                            + NatsJetStreamSinkOptions.NATIVE_FIELDS.key()
                            + "."
                            + mappingKey
                            + "` does not exist in table schema");
        }
        SeaTunnelDataType<?> fieldType = rowType.getFieldType(fieldIndex);
        if (fieldType.getSqlType() != expectedType) {
            throw invalidField(
                    fieldName,
                    "must use `" + expectedType + "` type for native mapping `" + mappingKey + "`");
        }
    }

    private static void requireNonBlank(
            ReadonlyConfig pluginConfig,
            org.apache.seatunnel.api.configuration.Option<String> option) {
        String value = pluginConfig.getOptional(option).orElse(null);
        if (!isNotBlank(value)) {
            throw invalidOption(option.key(), "must not be blank");
        }
    }

    private static boolean isNotBlank(String value) {
        return value != null && !value.trim().isEmpty();
    }

    private static NatsJetStreamConnectorException invalidOption(
            String optionName, String message) {
        return new NatsJetStreamConnectorException(
                CommonErrorCodeDeprecated.ILLEGAL_ARGUMENT,
                "Invalid NATS JetStream sink option `" + optionName + "`: " + message);
    }

    private static NatsJetStreamConnectorException invalidField(String fieldName, String message) {
        return new NatsJetStreamConnectorException(
                CommonErrorCodeDeprecated.ILLEGAL_ARGUMENT,
                "Invalid NATS JetStream sink field `" + fieldName + "`: " + message);
    }
}
