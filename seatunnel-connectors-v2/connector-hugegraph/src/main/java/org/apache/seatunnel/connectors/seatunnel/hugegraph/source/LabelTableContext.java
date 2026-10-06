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

package org.apache.seatunnel.connectors.seatunnel.hugegraph.source;

import org.apache.seatunnel.api.table.type.SeaTunnelRowType;

import java.io.Serializable;
import java.util.Objects;

/**
 * Per-label read context for the HugeGraph source. Carries everything the reader needs to turn one
 * label's elements into routable rows: the property row type (to fill the property columns), the
 * produced row type (reserved columns + properties), and the {@code tableId} string ({@code
 * CatalogTable.getTablePath().toString()}) that a downstream MultiTableSink routes on.
 *
 * <p>In single-label mode the source builds exactly one context; in read-all mode one per
 * discovered label. The reader looks the context up by the split's active label.
 */
public class LabelTableContext implements Serializable {

    private static final long serialVersionUID = 1L;

    private final String label;
    private final SeaTunnelRowType propertyRowType;
    private final SeaTunnelRowType outputRowType;
    private final String tableId;

    public LabelTableContext(
            String label,
            SeaTunnelRowType propertyRowType,
            SeaTunnelRowType outputRowType,
            String tableId) {
        this.label = Objects.requireNonNull(label);
        this.propertyRowType = Objects.requireNonNull(propertyRowType);
        this.outputRowType = Objects.requireNonNull(outputRowType);
        this.tableId = Objects.requireNonNull(tableId);
    }

    public String getLabel() {
        return label;
    }

    public SeaTunnelRowType getPropertyRowType() {
        return propertyRowType;
    }

    public SeaTunnelRowType getOutputRowType() {
        return outputRowType;
    }

    public String getTableId() {
        return tableId;
    }
}
