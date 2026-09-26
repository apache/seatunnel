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
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.Set;

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
        // The split reader already rejects sequence overflow with partition context before
        // emission.
        long nextSequenceNumber = Math.addExact(element.getSequenceNumber(), 1L);
        try {
            deserializationSchema.deserialize(element.getBody(), collector);
        } catch (IOException | RuntimeException e) {
            // Parser and collector exceptions can contain private event data, including in causes.
            throw new AzureEventHubsConnectorException(
                    AzureEventHubsConnectorErrorCode.DESERIALIZATION_FAILED,
                    "Could not deserialize or emit Event Hubs event in partition '"
                            + splitState.getPartitionId()
                            + "' at sequence number "
                            + element.getSequenceNumber()
                            + " ("
                            + (e instanceof IOException ? "I/O failure" : "runtime failure")
                            + ": "
                            + failureTypes(e)
                            + ")");
        }
        splitState.setCurrentSequenceNumber(nextSequenceNumber);
    }

    private static String failureTypes(Throwable failure) {
        Set<Throwable> visited = Collections.newSetFromMap(new IdentityHashMap<>());
        StringBuilder types = new StringBuilder();
        // Only class names are safe; never render exception text or suppressed exceptions.
        while (failure != null && visited.size() < 8 && visited.add(failure)) {
            if (types.length() > 0) {
                types.append(" <- ");
            }
            String type = failure.getClass().getSimpleName();
            types.append(
                    type.isEmpty() ? "Throwable" : type.substring(0, Math.min(type.length(), 80)));
            failure = failure.getCause();
        }
        if (failure != null) {
            types.append(" <- ...");
        }
        return types.toString();
    }
}
