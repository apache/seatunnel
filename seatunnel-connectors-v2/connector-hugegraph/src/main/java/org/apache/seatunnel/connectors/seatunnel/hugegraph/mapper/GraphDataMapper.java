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
import java.util.Collections;
import java.util.List;

public interface GraphDataMapper extends Serializable {

    /**
     * Maps a SeaTunnelRow to a HugeGraph GraphElement (Vertex or Edge). Returns null if the element
     * should be skipped (e.g. null ID fields matched by nullValues).
     */
    GraphElement map(SeaTunnelRow row);

    /**
     * Maps a row to one or more graph elements. Without unfold this is just {@link #map} wrapped in
     * a list; with unfold enabled a list-valued id cell expands into multiple elements. Used on the
     * INSERT/append path only.
     */
    default List<GraphElement> mapAll(SeaTunnelRow row) {
        GraphElement element = map(row);
        return element == null ? Collections.emptyList() : Collections.singletonList(element);
    }

    /** Whether this mapper expands one row into multiple elements (unfold). */
    default boolean isUnfoldEnabled() {
        return false;
    }

    /**
     * Extracts the graph element ID from a SeaTunnelRow. The ID format must match the server-side
     * format to ensure DELETE operations target the correct element. Returns null if the ID cannot
     * be built (e.g. null ID fields).
     */
    Object extractId(SeaTunnelRow row);
}
