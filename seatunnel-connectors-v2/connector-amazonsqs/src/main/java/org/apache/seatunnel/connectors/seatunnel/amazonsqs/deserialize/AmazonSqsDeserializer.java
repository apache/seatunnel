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

package org.apache.seatunnel.connectors.seatunnel.amazonsqs.deserialize;

import org.apache.seatunnel.api.serialization.DeserializationSchema;
import org.apache.seatunnel.api.source.Collector;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.common.exception.CommonErrorCode;
import org.apache.seatunnel.common.exception.SeaTunnelRuntimeException;
import org.apache.seatunnel.connectors.seatunnel.amazonsqs.config.MessageFormat;
import org.apache.seatunnel.connectors.seatunnel.amazonsqs.exception.AmazonSqsConnectorErrorCode;
import org.apache.seatunnel.connectors.seatunnel.amazonsqs.exception.AmazonSqsConnectorException;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

public class AmazonSqsDeserializer implements SeaTunnelRowDeserializer {

    private final DeserializationSchema<SeaTunnelRow> deserializationSchema;
    private final boolean ignoreParseErrors;
    private final MessageFormat format;

    public AmazonSqsDeserializer(
            DeserializationSchema<SeaTunnelRow> deserializationSchema, boolean ignoreParseErrors) {
        this(deserializationSchema, ignoreParseErrors, MessageFormat.JSON);
    }

    public AmazonSqsDeserializer(
            DeserializationSchema<SeaTunnelRow> deserializationSchema,
            boolean ignoreParseErrors,
            MessageFormat format) {
        this.deserializationSchema = deserializationSchema;
        this.ignoreParseErrors = ignoreParseErrors;
        this.format = format;
    }

    @Override
    public SeaTunnelRow deserializeRow(String row) {
        SeaTunnelRow seaTunnelRow;
        try {
            seaTunnelRow = deserializationSchema.deserialize(row.getBytes(StandardCharsets.UTF_8));
        } catch (SeaTunnelRuntimeException e) {
            // JSON parsing wraps failures in COMMON-02; unrelated runtime errors must propagate.
            if (!CommonErrorCode.JSON_OPERATION_FAILED.equals(e.getSeaTunnelErrorCode())) {
                throw e;
            }
            return handleDeserializationFailure(e);
        } catch (IOException e) {
            return handleDeserializationFailure(e);
        }
        if (seaTunnelRow == null && !ignoreParseErrors) {
            throw new AmazonSqsConnectorException(
                    AmazonSqsConnectorErrorCode.DESERIALIZE_FAILED,
                    "Failed to deserialize Amazon SQS message");
        }
        return seaTunnelRow;
    }

    private SeaTunnelRow handleDeserializationFailure(Throwable cause) {
        if (ignoreParseErrors) {
            return null;
        }
        throw new AmazonSqsConnectorException(
                AmazonSqsConnectorErrorCode.DESERIALIZE_FAILED,
                "Failed to deserialize Amazon SQS message",
                cause);
    }

    @Override
    public List<SeaTunnelRow> deserializeRows(String row) {
        if (format == MessageFormat.CANAL_JSON || format == MessageFormat.DEBEZIUM_JSON) {
            return deserializeMultipleRows(row.getBytes(StandardCharsets.UTF_8));
        }
        return SeaTunnelRowDeserializer.super.deserializeRows(row);
    }

    private List<SeaTunnelRow> deserializeMultipleRows(byte[] message) {
        List<SeaTunnelRow> rows = new ArrayList<>();
        try {
            deserializationSchema.deserialize(message, new BufferingCollector(rows));
            return rows;
        } catch (IOException e) {
            if (ignoreParseErrors) {
                return Collections.emptyList();
            }
            throw new AmazonSqsConnectorException(
                    AmazonSqsConnectorErrorCode.DESERIALIZE_FAILED,
                    "Failed to deserialize Amazon SQS message",
                    e);
        } catch (SeaTunnelRuntimeException e) {
            if (ignoreParseErrors
                    && CommonErrorCode.JSON_OPERATION_FAILED.equals(e.getSeaTunnelErrorCode())) {
                return Collections.emptyList();
            }
            throw e;
        }
    }

    private static final class BufferingCollector implements Collector<SeaTunnelRow> {
        private final List<SeaTunnelRow> rows;

        private BufferingCollector(List<SeaTunnelRow> rows) {
            this.rows = rows;
        }

        @Override
        public void collect(SeaTunnelRow row) {
            rows.add(Objects.requireNonNull(row, "Deserialization schema emitted a null row"));
        }

        @Override
        public Object getCheckpointLock() {
            return this;
        }
    }
}
