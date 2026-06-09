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

import org.apache.seatunnel.connectors.seatunnel.edgesocket.queue.EdgeSocketQueuedRecord;

public interface EdgeSocketPayloadDeserializer {

    /**
     * Convert queued payload bytes to final plain-text record body.
     *
     * <p>This stage handles decompression according to queue metadata produced in record
     * deserialization.
     *
     * @param queuedRecord queued binary payload with compression type
     * @return plain UTF-8 string used by row deserialization
     */
    String deserializeRecord(EdgeSocketQueuedRecord queuedRecord);
}
