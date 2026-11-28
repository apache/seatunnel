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

package org.apache.seatunnel.connectors.seatunnel.onehundredms;

import org.apache.seatunnel.api.source.Boundedness;
import org.apache.seatunnel.api.source.SeaTunnelSource;
import org.apache.seatunnel.api.source.SourceReader;
import org.apache.seatunnel.api.source.SourceSplitEnumerator;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.CatalogTableUtil;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;

import java.util.Collections;
import java.util.List;

/**
 * Bounded source with a single checkpointed reader. Each row carries its provider resource and
 * complete JSON object in a uniform schema.
 */
public final class OneHundredMsSource
        implements SeaTunnelSource<
                SeaTunnelRow, OneHundredMsProgress, OneHundredMsEnumerator.State> {
    private static final long serialVersionUID = 1L;
    static final String IDENTIFIER = "OneHundredMs";

    private final OneHundredMsConfig config;
    private final CatalogTable catalog;

    OneHundredMsSource(OneHundredMsConfig config) {
        this.config = config;
        SeaTunnelRowType type =
                new SeaTunnelRowType(
                        new String[] {"resource", "record_id", "parent_id", "updated_at", "data"},
                        new SeaTunnelDataType<?>[] {
                            BasicType.STRING_TYPE,
                            BasicType.STRING_TYPE,
                            BasicType.STRING_TYPE,
                            BasicType.STRING_TYPE,
                            BasicType.STRING_TYPE
                        });
        catalog = CatalogTableUtil.getCatalogTable(IDENTIFIER, type);
    }

    @Override
    public String getPluginName() {
        return IDENTIFIER;
    }

    @Override
    public Boundedness getBoundedness() {
        return Boundedness.BOUNDED;
    }

    @Override
    public List<CatalogTable> getProducedCatalogTables() {
        return Collections.singletonList(catalog);
    }

    /**
     * Creates the single reader with its own transport, worker and detached checkpoint progress.
     */
    @Override
    public SourceReader<SeaTunnelRow, OneHundredMsProgress> createReader(
            SourceReader.Context context) {
        return new OneHundredMsReader(
                config, context, catalog.getTableId().toTablePath().toString());
    }

    @Override
    public SourceSplitEnumerator<OneHundredMsProgress, OneHundredMsEnumerator.State>
            createEnumerator(SourceSplitEnumerator.Context<OneHundredMsProgress> context) {
        return new OneHundredMsEnumerator(context, config, null);
    }

    /**
     * Restores coordinator assignment state without replacing a reader-owned accepted-row prefix.
     */
    @Override
    public SourceSplitEnumerator<OneHundredMsProgress, OneHundredMsEnumerator.State>
            restoreEnumerator(
                    SourceSplitEnumerator.Context<OneHundredMsProgress> context,
                    OneHundredMsEnumerator.State state) {
        return new OneHundredMsEnumerator(context, config, state);
    }
}
