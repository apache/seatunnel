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

package org.apache.seatunnel.connectors.seatunnel.edgesocket.serialize.record;

import org.apache.seatunnel.connectors.seatunnel.edgesocket.queue.EdgeSocketQueuedRecord;
import org.apache.seatunnel.connectors.seatunnel.edgesocket.serialize.EdgeSocketCompressionType;
import org.apache.seatunnel.connectors.seatunnel.edgesocket.serialize.EdgeSocketRecordDeserializer;

import java.nio.charset.StandardCharsets;

public class EdgeSocketRawRecordDeserializer implements EdgeSocketRecordDeserializer {

    /**
     * Convert raw line into queue record without envelope parsing.
     *
     * @param rawMessage incoming plain message
     * @return queued record tagged as {@link EdgeSocketCompressionType#NONE}
     */
    @Override
    public EdgeSocketQueuedRecord deserializeRecord(String rawMessage) {
        return new EdgeSocketQueuedRecord(
                rawMessage.getBytes(StandardCharsets.UTF_8), EdgeSocketCompressionType.NONE);
    }
}
