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
package org.apache.seatunnel.connectors.seatunnel.azure.eventhubs.source;

import org.apache.seatunnel.api.serialization.DeserializationSchema;
import org.apache.seatunnel.api.source.Collector;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.connectors.seatunnel.azure.eventhubs.exception.AzureEventHubsConnectorErrorCode;
import org.apache.seatunnel.connectors.seatunnel.azure.eventhubs.exception.AzureEventHubsConnectorException;
import org.apache.seatunnel.connectors.seatunnel.common.source.reader.RecordEmitter;

import java.io.IOException;

/** Deserializes an event before advancing the checkpointed partition position. */
public class AzureEventHubsRecordEmitter
        implements RecordEmitter<EventHubsRecord, SeaTunnelRow, AzureEventHubsSourceSplitState> {

    private final DeserializationSchema<SeaTunnelRow> deserializationSchema;

    public AzureEventHubsRecordEmitter(DeserializationSchema<SeaTunnelRow> deserializationSchema) {
        this.deserializationSchema = deserializationSchema;
    }

    @Override
    public void emitRecord(
            EventHubsRecord element,
            Collector<SeaTunnelRow> collector,
            AzureEventHubsSourceSplitState splitState) {
        try {
            long nextSequenceNumber = Math.addExact(element.getSequenceNumber(), 1L);
            deserializationSchema.deserialize(element.getBody(), collector);
            splitState.setCurrentSequenceNumber(nextSequenceNumber);
        } catch (IOException | ArithmeticException e) {
            throw new AzureEventHubsConnectorException(
                    AzureEventHubsConnectorErrorCode.DESERIALIZATION_FAILED,
                    "Could not deserialize Event Hubs event at sequence number "
                            + element.getSequenceNumber(),
                    e);
        }
    }
}
