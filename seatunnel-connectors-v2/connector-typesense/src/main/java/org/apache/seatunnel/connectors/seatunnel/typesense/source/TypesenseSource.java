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

package org.apache.seatunnel.connectors.seatunnel.typesense.source;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.options.ConnectorCommonOptions;
import org.apache.seatunnel.api.source.Boundedness;
import org.apache.seatunnel.api.source.SeaTunnelSource;
import org.apache.seatunnel.api.source.SourceReader;
import org.apache.seatunnel.api.source.SourceSplitEnumerator;
import org.apache.seatunnel.api.source.SupportColumnProjection;
import org.apache.seatunnel.api.source.SupportParallelism;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.CatalogTableUtil;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;

import lombok.extern.slf4j.Slf4j;

import java.util.Collections;
import java.util.List;

@Slf4j
public class TypesenseSource
        implements SeaTunnelSource<SeaTunnelRow, TypesenseSourceSplit, TypesenseSourceState>,
                SupportParallelism,
                SupportColumnProjection {

    private final ReadonlyConfig config;

    private CatalogTable catalogTable;

    public TypesenseSource(ReadonlyConfig config) {
        this.config = config;
        if (config.getOptional(ConnectorCommonOptions.SCHEMA).isPresent()) {
            catalogTable = CatalogTableUtil.buildWithConfig(config);
        }
    }

    @Override
    public String getPluginName() {
        return "Typesense";
    }

    @Override
    public Boundedness getBoundedness() {
        return Boundedness.BOUNDED;
    }

    @Override
    public List<CatalogTable> getProducedCatalogTables() {
        return Collections.singletonList(catalogTable);
    }

    @Override
    public SourceReader<SeaTunnelRow, TypesenseSourceSplit> createReader(
            SourceReader.Context readerContext) throws Exception {
        return new TypesenseSourceReader(readerContext, config, catalogTable.getSeaTunnelRowType());
    }

    @Override
    public SourceSplitEnumerator<TypesenseSourceSplit, TypesenseSourceState> createEnumerator(
            SourceSplitEnumerator.Context<TypesenseSourceSplit> enumeratorContext) {
        return new TypesenseSourceSplitEnumerator(enumeratorContext, config);
    }

    @Override
    public SourceSplitEnumerator<TypesenseSourceSplit, TypesenseSourceState> restoreEnumerator(
            SourceSplitEnumerator.Context<TypesenseSourceSplit> enumeratorContext,
            TypesenseSourceState checkpointState) {
        return new TypesenseSourceSplitEnumerator(enumeratorContext, config);
    }
}
