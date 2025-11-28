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

package org.apache.seatunnel.connectors.seatunnel.hugegraph.mapper;

import org.apache.seatunnel.api.table.type.SeaTunnelRow;

import org.apache.hugegraph.structure.GraphElement;

import java.io.Serializable;

public interface GraphDataMapper extends Serializable {

    /**
     * Maps a SeaTunnelRow to a HugeGraph GraphElement (Vertex or Edge).
     *
     * @param row The input SeaTunnelRow.
     * @return The resulting GraphElement.
     */
    GraphElement map(SeaTunnelRow row);

    /**
     * Extracts the ID from a SeaTunnelRow.
     *
     * @param row The input SeaTunnelRow.
     * @return The extracted ID object.
     */
    Object extractId(SeaTunnelRow row);
}
