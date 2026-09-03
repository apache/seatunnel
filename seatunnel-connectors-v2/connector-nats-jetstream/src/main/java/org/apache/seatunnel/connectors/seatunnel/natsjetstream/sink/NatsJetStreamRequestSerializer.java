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

import org.apache.seatunnel.api.serialization.SerializationSchema;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.connectors.seatunnel.natsjetstream.sink.NatsJetStreamSinkWriter.NativeFieldMapping;
import org.apache.seatunnel.connectors.seatunnel.natsjetstream.sink.NatsJetStreamSinkWriter.PublishRequest;

import io.nats.client.impl.Headers;

import java.util.Map;

final class NatsJetStreamRequestSerializer {

    static final String ROW_KIND_HEADER = "x-seatunnel-row-kind";

    private final String defaultSubject;
    private final SerializationSchema serializationSchema;
    private final boolean includeRowKindHeader;
    private final NativeFieldMapping nativeFieldMapping;

    private NatsJetStreamRequestSerializer(
            String defaultSubject,
            SerializationSchema serializationSchema,
            boolean includeRowKindHeader,
            NativeFieldMapping nativeFieldMapping) {
        this.defaultSubject = defaultSubject;
        this.serializationSchema = serializationSchema;
        this.includeRowKindHeader = includeRowKindHeader;
        this.nativeFieldMapping = nativeFieldMapping;
    }

    static NatsJetStreamRequestSerializer forJson(
            String defaultSubject, SerializationSchema serializationSchema) {
        return new NatsJetStreamRequestSerializer(defaultSubject, serializationSchema, false, null);
    }

    static NatsJetStreamRequestSerializer forNative(
            String defaultSubject,
            boolean includeRowKindHeader,
            NativeFieldMapping nativeFieldMapping) {
        return new NatsJetStreamRequestSerializer(
                defaultSubject, null, includeRowKindHeader, nativeFieldMapping);
    }

    PublishRequest serialize(SeaTunnelRow element) {
        if (nativeFieldMapping == null) {
            return new PublishRequest(
                    defaultSubject, null, null, serializationSchema.serialize(element));
        }
        String subject = defaultSubject;
        if (nativeFieldMapping.subjectFieldIndex >= 0) {
            subject = resolveSubjectField(element, nativeFieldMapping.subjectFieldIndex, subject);
        }
        if (subject == null || subject.isEmpty()) {
            throw NatsJetStreamSinkWriter.invalidRecord("subject", "must not be null or blank");
        }

        String messageId = null;
        if (nativeFieldMapping.messageIdFieldIndex >= 0) {
            messageId =
                    resolveOptionalStringField(
                            element, nativeFieldMapping.messageIdFieldIndex, "id");
        }

        Headers headers = null;
        if (includeRowKindHeader) {
            headers = withRowKindHeader(element);
        }
        if (nativeFieldMapping.headersFieldIndex >= 0) {
            headers = requireHeaders(element, nativeFieldMapping.headersFieldIndex, headers);
        }

        byte[] data = requireBinaryPayload(element, nativeFieldMapping.dataFieldIndex);
        return new PublishRequest(subject, messageId, headers, data);
    }

    private static Headers requireHeaders(SeaTunnelRow element, int fieldIndex, Headers headers) {
        Object headersValue = element.getField(fieldIndex);
        if (headersValue == null) {
            return headers;
        }
        if (headers == null) {
            headers = new Headers();
        }
        if (!(headersValue instanceof Map)) {
            throw NatsJetStreamSinkWriter.invalidRecord(
                    "headers", "must be a map of string keys and values");
        }
        Map<?, ?> rawHeaders = (Map<?, ?>) headersValue;
        for (Map.Entry<?, ?> entry : rawHeaders.entrySet()) {
            if (!(entry.getKey() instanceof String)) {
                throw NatsJetStreamSinkWriter.invalidRecord("headers", "contains a non-string key");
            }
            if (!(entry.getValue() instanceof String)) {
                throw NatsJetStreamSinkWriter.invalidRecord(
                        "headers", "contains a non-string value");
            }
            headers.add((String) entry.getKey(), (String) entry.getValue());
        }
        return headers;
    }

    private static byte[] requireBinaryPayload(SeaTunnelRow element, int fieldIndex) {
        Object value = element.getField(fieldIndex);
        if (!(value instanceof byte[])) {
            throw NatsJetStreamSinkWriter.invalidRecord("data", "must be a non-null BYTES value");
        }
        return (byte[]) value;
    }

    private static Headers withRowKindHeader(SeaTunnelRow element) {
        Headers headers = new Headers();
        headers.add(ROW_KIND_HEADER, element.getRowKind().name());
        return headers;
    }

    private static String resolveSubjectField(
            SeaTunnelRow element, int fieldIndex, String fallbackSubject) {
        Object value = element.getField(fieldIndex);
        if (value == null) {
            return fallbackSubject;
        }
        if (!(value instanceof String)) {
            throw NatsJetStreamSinkWriter.invalidRecord("subject", "must be a STRING value");
        }
        String subject = ((String) value).trim();
        if (subject.isEmpty()) {
            return fallbackSubject;
        }
        return subject;
    }

    private static String resolveOptionalStringField(
            SeaTunnelRow element, int fieldIndex, String fieldName) {
        Object value = element.getField(fieldIndex);
        if (value == null) {
            return null;
        }
        if (!(value instanceof String)) {
            throw NatsJetStreamSinkWriter.invalidRecord(fieldName, "must be a STRING value");
        }
        String stringValue = ((String) value).trim();
        if (stringValue.isEmpty()) {
            return null;
        }
        return stringValue;
    }
}
