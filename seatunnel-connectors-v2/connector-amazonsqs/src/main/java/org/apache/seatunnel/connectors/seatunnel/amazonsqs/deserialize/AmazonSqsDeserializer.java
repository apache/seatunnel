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
import org.apache.seatunnel.connectors.seatunnel.amazonsqs.exception.AmazonSqsConnectorException;

import org.apache.commons.collections4.CollectionUtils;

import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.util.List;

import static org.apache.seatunnel.connectors.seatunnel.amazonsqs.exception.AmazonSqsConnectorErrorCode.DESEARILIZATION_ERROR;

@Slf4j
public class AmazonSqsDeserializer implements SeaTunnelRowDeserializer {

    private final DeserializationSchema<SeaTunnelRow> deserializationSchema;

    public AmazonSqsDeserializer(DeserializationSchema<SeaTunnelRow> deserializationSchema) {
        this.deserializationSchema = deserializationSchema;
    }

    @Override
    public SeaTunnelRow deserializeRow(String row) {
        List<SeaTunnelRow> seaTunnelRows;
        try {
            seaTunnelRows = deserializationSchema.deserialize(row.getBytes());
            if (CollectionUtils.isEmpty(seaTunnelRows)) {
                log.warn("The AmazonSqsDeserializer deserialize result is empty");
                return null;
            }
        } catch (IOException e) {
            throw new AmazonSqsConnectorException(DESEARILIZATION_ERROR, row, e);
        }
        if (seaTunnelRows.size() != 1) {
            throw new AmazonSqsConnectorException(DESEARILIZATION_ERROR, row);
        }
        return seaTunnelRows.get(0);
    }
}
