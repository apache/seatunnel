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

package org.apache.seatunnel.connectors.seatunnel.hugegraph.buffer;

import org.apache.seatunnel.connectors.seatunnel.hugegraph.config.MappingConfig.LabelType;

import org.apache.hugegraph.structure.GraphElement;
import org.apache.hugegraph.structure.graph.UpdateStrategy;

import java.util.Collections;
import java.util.Map;

/**
 * Wraps a graph element with non-sensitive mapping context for failure diagnostics.
 *
 * <p>Deliberately does NOT retain the source {@link
 * org.apache.seatunnel.api.table.type.SeaTunnelRow}: only the mapped {@link GraphElement} is ever
 * sent, and envelopes stay alive until the batch is flushed (by size/timer/checkpoint/close).
 * Keeping the raw row would pin fields that were excluded by {@code mapping.properties} (e.g. large
 * BYTES payloads) in memory for the whole batch and leak their content into failure logs.
 */
public class GraphElementEnvelope {

    private final String mappingLabel;
    private final LabelType elementType;
    private final GraphElement element;
    // Per-mapping update strategies (property name -> strategy). Empty means plain insert. Carried
    // on the envelope so the buffer can route each element by its own mapping's strategy instead of
    // one merged global map — a strategy on one mapping no longer forces upsert on every mapping,
    // and two mappings may assign different strategies to the same property name.
    private final Map<String, UpdateStrategy> updateStrategies;

    public GraphElementEnvelope(String mappingLabel, LabelType elementType, GraphElement element) {
        this(mappingLabel, elementType, element, Collections.emptyMap());
    }

    public GraphElementEnvelope(
            String mappingLabel,
            LabelType elementType,
            GraphElement element,
            Map<String, UpdateStrategy> updateStrategies) {
        this.mappingLabel = mappingLabel;
        this.elementType = elementType;
        this.element = element;
        this.updateStrategies =
                updateStrategies == null ? Collections.emptyMap() : updateStrategies;
    }

    public String getMappingLabel() {
        return mappingLabel;
    }

    public LabelType getElementType() {
        return elementType;
    }

    public GraphElement getElement() {
        return element;
    }

    public Map<String, UpdateStrategy> getUpdateStrategies() {
        return updateStrategies;
    }
}
