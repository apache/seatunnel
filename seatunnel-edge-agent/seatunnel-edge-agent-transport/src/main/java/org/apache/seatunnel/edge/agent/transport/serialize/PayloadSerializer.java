/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.seatunnel.edge.agent.transport.serialize;

public interface PayloadSerializer {

    /**
     * Converts the raw WAL payload bytes into a line-protocol payload string for the wire.
     *
     * <p>RAW mode decodes the bytes as UTF-8 and returns the string unchanged. PACKET mode
     * compresses/encrypts the raw bytes and wraps them in an {@link
     * org.apache.seatunnel.edge.agent.transport.packet.EdgeIngressPacket} JSON envelope aligned
     * with the engine-side EdgeSocket source.
     *
     * @param rawPayload raw bytes from the WAL row
     * @return payload placed after {@code __BATCH__:<id>:} on the wire
     * @throws RuntimeException if encoding fails (fails the scheduler send for that batch)
     */
    String serialize(byte[] rawPayload);
}
