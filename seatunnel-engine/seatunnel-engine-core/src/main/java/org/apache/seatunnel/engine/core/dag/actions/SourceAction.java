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

package org.apache.seatunnel.engine.core.dag.actions;

import org.apache.seatunnel.shade.com.google.common.collect.Lists;

import org.apache.seatunnel.api.source.SeaTunnelSource;
import org.apache.seatunnel.api.source.SourceSplit;
import org.apache.seatunnel.engine.core.job.ConnectorJarIdentifier;
import org.apache.seatunnel.lineage.LineageDataset;

import lombok.NonNull;

import java.io.Serializable;
import java.net.URL;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;

public class SourceAction<T, SplitT extends SourceSplit, StateT extends Serializable>
        extends AbstractAction {

    private static final long serialVersionUID = -4104531889750766731L;
    private final SeaTunnelSource<T, SplitT, StateT> source;
    private List<LineageDataset> lineageDatasets = Collections.emptyList();

    public SourceAction(
            long id,
            @NonNull String name,
            @NonNull SeaTunnelSource<T, SplitT, StateT> source,
            @NonNull Set<URL> jarUrls,
            @NonNull Set<ConnectorJarIdentifier> connectorJarIdentifiers) {
        super(id, name, Lists.newArrayList(), jarUrls, connectorJarIdentifiers);
        this.source = source;
    }

    public SeaTunnelSource<T, SplitT, StateT> getSource() {
        return source;
    }

    /**
     * Returns the datasets read by this source.
     *
     * <p>Legacy serialized actions do not contain this field, so the getter normalizes the missing
     * value to an empty list.
     *
     * @return configured source datasets, or an empty list for legacy actions
     */
    public List<LineageDataset> getLineageDatasets() {
        return lineageDatasets == null ? Collections.emptyList() : lineageDatasets;
    }

    /**
     * Sets the datasets read by this source.
     *
     * @param lineageDatasets source datasets; {@code null} means no datasets
     */
    public void setLineageDatasets(List<LineageDataset> lineageDatasets) {
        this.lineageDatasets =
                lineageDatasets == null
                        ? Collections.emptyList()
                        : new ArrayList<>(lineageDatasets);
    }
}
