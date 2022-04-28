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

package org.apache.seatunnel.translation.serialization;

import org.apache.seatunnel.api.table.type.Row;

import java.io.IOException;

public interface RowSerialization<T> {

    /**
     * Serializes the given object.
     *
     * @param seaTunnelRow The object to serialize.
     * @return The serialized data (bytes).
     * @throws IOException Thrown, if the serialization fails.
     */
    T serialize(Row seaTunnelRow) throws IOException;

    /**
     * De-serializes the given data (bytes).
     *
     * @param engineRow The internal engine row
     * @return The SeaTunnel Row
     * @throws IOException Thrown, if the deserialization fails.
     */
    Row deserialize(T engineRow) throws IOException;
}
