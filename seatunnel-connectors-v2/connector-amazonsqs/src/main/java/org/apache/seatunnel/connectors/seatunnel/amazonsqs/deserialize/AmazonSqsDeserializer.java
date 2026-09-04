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
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.common.exception.CommonErrorCode;
import org.apache.seatunnel.common.exception.SeaTunnelRuntimeException;
import org.apache.seatunnel.connectors.seatunnel.amazonsqs.exception.AmazonSqsConnectorErrorCode;
import org.apache.seatunnel.connectors.seatunnel.amazonsqs.exception.AmazonSqsConnectorException;

import java.io.IOException;

public class AmazonSqsDeserializer implements SeaTunnelRowDeserializer {

    private final DeserializationSchema<SeaTunnelRow> deserializationSchema;
    private final boolean ignoreParseErrors;

    public AmazonSqsDeserializer(
            DeserializationSchema<SeaTunnelRow> deserializationSchema, boolean ignoreParseErrors) {
        this.deserializationSchema = deserializationSchema;
        this.ignoreParseErrors = ignoreParseErrors;
    }

    @Override
    public SeaTunnelRow deserializeRow(String row) {
        SeaTunnelRow seaTunnelRow;
        try {
            seaTunnelRow = deserializationSchema.deserialize(row.getBytes());
        } catch (SeaTunnelRuntimeException e) {
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
}
