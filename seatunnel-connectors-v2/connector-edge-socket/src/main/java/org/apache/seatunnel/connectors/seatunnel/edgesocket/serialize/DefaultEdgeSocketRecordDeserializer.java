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

package org.apache.seatunnel.connectors.seatunnel.edgesocket.serialize;

import org.apache.seatunnel.connectors.seatunnel.edgesocket.config.EdgeSocketConfig;
import org.apache.seatunnel.connectors.seatunnel.edgesocket.queue.EdgeSocketQueuedRecord;
import org.apache.seatunnel.connectors.seatunnel.edgesocket.serialize.record.EdgeSocketPacketRecordDeserializer;
import org.apache.seatunnel.connectors.seatunnel.edgesocket.serialize.record.EdgeSocketRawRecordDeserializer;

import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
public class DefaultEdgeSocketRecordDeserializer implements EdgeSocketRecordDeserializer {

    private final EdgeSocketRecordDeserializer delegate;

    /**
     * Delegate record deserialization to mode-specific implementation.
     *
     * @param rawMessage incoming socket message
     * @return queue-ready record
     */
    @Override
    public EdgeSocketQueuedRecord deserializeRecord(String rawMessage) {
        return delegate.deserializeRecord(rawMessage);
    }

    /**
     * Create deserializer based on configured packet mode.
     *
     * @param config connector runtime config
     * @return raw-mode or packet-mode deserializer wrapper
     */
    public static DefaultEdgeSocketRecordDeserializer create(EdgeSocketConfig config) {
        EdgeSocketRecordDeserializer delegate =
                config.getPacketMode() == EdgeSocketPacketMode.PACKET
                        ? new EdgeSocketPacketRecordDeserializer(config)
                        : new EdgeSocketRawRecordDeserializer();
        return new DefaultEdgeSocketRecordDeserializer(delegate);
    }
}
