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

import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.connectors.seatunnel.hugegraph.config.MappingConfig.LabelType;

import org.apache.hugegraph.structure.GraphElement;

/** Wraps a graph element with non-sensitive mapping context for failure diagnostics. */
public class GraphElementEnvelope {

    private final String mappingLabel;
    private final LabelType elementType;
    private final SeaTunnelRow sourceRow;
    private final GraphElement element;

    public GraphElementEnvelope(
            String mappingLabel,
            LabelType elementType,
            SeaTunnelRow sourceRow,
            GraphElement element) {
        this.mappingLabel = mappingLabel;
        this.elementType = elementType;
        this.sourceRow = sourceRow;
        this.element = element;
    }

    public String getMappingLabel() {
        return mappingLabel;
    }

    public LabelType getElementType() {
        return elementType;
    }

    public SeaTunnelRow getSourceRow() {
        return sourceRow;
    }

    public GraphElement getElement() {
        return element;
    }
}
