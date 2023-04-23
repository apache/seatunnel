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

package org.apache.seatunnel.api.serialization;

import org.apache.seatunnel.api.source.Collector;

import java.io.IOException;

public interface DeserializationSchemaWithTopic<T> extends DeserializationSchema<T> {

    /**
     * Deserializes the byte message.
     *
     * @param topic The topic of message
     * @param message The message, as a byte array.
     * @return The deserialized message as an SeaTunnel Row (null if the message cannot be
     *     deserialized).
     */
    T deserialize(String topic, byte[] message) throws IOException;

    default void deserialize(String topic, byte[] message, Collector<T> out) throws IOException {
        try {
            T deserialize = deserialize(topic, message);
            if (deserialize != null) {
                out.collect(deserialize);
            }
        } catch (IOException e) {
            throw new IOException(e);
        }
    }
}
