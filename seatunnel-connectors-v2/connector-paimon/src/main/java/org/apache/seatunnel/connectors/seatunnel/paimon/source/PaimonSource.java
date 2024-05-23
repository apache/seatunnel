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

package org.apache.seatunnel.connectors.seatunnel.paimon.source;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.source.Boundedness;
import org.apache.seatunnel.api.source.SeaTunnelSource;
import org.apache.seatunnel.api.source.SourceReader;
import org.apache.seatunnel.api.source.SourceSplitEnumerator;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.connectors.seatunnel.paimon.catalog.PaimonCatalog;
import org.apache.seatunnel.connectors.seatunnel.paimon.config.PaimonSourceConfig;
import org.apache.seatunnel.connectors.seatunnel.paimon.source.converter.SqlToPaimonPredicateConverter;

import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.table.Table;

import java.util.Collections;
import java.util.List;

/** Paimon connector source class. */
public class PaimonSource
        implements SeaTunnelSource<SeaTunnelRow, PaimonSourceSplit, PaimonSourceState> {

    private static final long serialVersionUID = 1L;

    public static final String PLUGIN_NAME = "Paimon";

    private ReadonlyConfig readonlyConfig;

    private SeaTunnelRowType seaTunnelRowType;

    private Table paimonTable;

    private PaimonCatalog paimonCatalog;

    private Predicate predicate;

    private CatalogTable catalogTable;

    public PaimonSource(ReadonlyConfig readonlyConfig, PaimonCatalog paimonCatalog) {
        this.readonlyConfig = readonlyConfig;
        PaimonSourceConfig paimonSourceConfig = new PaimonSourceConfig(readonlyConfig);
        TablePath tablePath =
                TablePath.of(paimonSourceConfig.getNamespace(), paimonSourceConfig.getTable());
        this.paimonCatalog = paimonCatalog;
        this.catalogTable = paimonCatalog.getTable(tablePath);
        this.paimonTable = paimonCatalog.getPaimonTable(tablePath);
        this.seaTunnelRowType = catalogTable.getSeaTunnelRowType();
        // TODO: We can use this to realize the column projection feature later
        String filterSql = readonlyConfig.get(PaimonSourceConfig.FILTER_SQL);
        this.predicate =
                SqlToPaimonPredicateConverter.convertSqlWhereToPaimonPredicate(
                        this.paimonTable.rowType(), filterSql);
    }

    @Override
    public String getPluginName() {
        return PLUGIN_NAME;
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
    public SourceReader<SeaTunnelRow, PaimonSourceSplit> createReader(
            SourceReader.Context readerContext) throws Exception {

        return new PaimonSourceReader(
                readerContext, paimonTable, seaTunnelRowType, predicate, paimonCatalog);
    }

    @Override
    public SourceSplitEnumerator<PaimonSourceSplit, PaimonSourceState> createEnumerator(
            SourceSplitEnumerator.Context<PaimonSourceSplit> enumeratorContext) throws Exception {
        return new PaimonSourceSplitEnumerator(
                enumeratorContext, paimonTable, predicate, paimonCatalog);
    }

    @Override
    public SourceSplitEnumerator<PaimonSourceSplit, PaimonSourceState> restoreEnumerator(
            SourceSplitEnumerator.Context<PaimonSourceSplit> enumeratorContext,
            PaimonSourceState checkpointState)
            throws Exception {
        return new PaimonSourceSplitEnumerator(
                enumeratorContext, paimonTable, checkpointState, predicate, paimonCatalog);
    }
}
