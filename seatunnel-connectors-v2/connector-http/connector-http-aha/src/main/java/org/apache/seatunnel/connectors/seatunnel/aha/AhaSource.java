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

package org.apache.seatunnel.connectors.seatunnel.aha;

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

/** Bounded Aha v1 list source. Uses a uniform JSON envelope across the eight resource types. */
public final class AhaSource
        implements SeaTunnelSource<SeaTunnelRow, AhaProgress, AhaEnumerator.State> {
    private static final long serialVersionUID = 1L;
    static final String IDENTIFIER = "Aha";

    private final AhaConfig config;
    private final CatalogTable table;

    AhaSource(AhaConfig config) {
        this.config = config;
        SeaTunnelRowType type =
                new SeaTunnelRowType(
                        new String[] {"resource", "record_id", "parent_id", "data"},
                        new SeaTunnelDataType<?>[] {
                            BasicType.STRING_TYPE,
                            BasicType.STRING_TYPE,
                            BasicType.STRING_TYPE,
                            BasicType.STRING_TYPE
                        });
        table = CatalogTableUtil.getCatalogTable(IDENTIFIER, type);
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
        return Collections.singletonList(table);
    }

    @Override
    public SourceReader<SeaTunnelRow, AhaProgress> createReader(SourceReader.Context context) {
        return new AhaReader(config, context, table.getTableId().toTablePath().toString());
    }

    @Override
    public SourceSplitEnumerator<AhaProgress, AhaEnumerator.State> createEnumerator(
            SourceSplitEnumerator.Context<AhaProgress> context) {
        return new AhaEnumerator(context, config, null);
    }

    @Override
    public SourceSplitEnumerator<AhaProgress, AhaEnumerator.State> restoreEnumerator(
            SourceSplitEnumerator.Context<AhaProgress> context, AhaEnumerator.State saved) {
        return new AhaEnumerator(context, config, saved);
    }
}
