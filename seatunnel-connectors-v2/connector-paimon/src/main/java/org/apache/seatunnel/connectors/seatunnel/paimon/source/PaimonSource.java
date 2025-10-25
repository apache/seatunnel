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

import org.apache.seatunnel.shade.com.google.common.collect.Lists;
import org.apache.seatunnel.shade.com.google.common.collect.Maps;

import org.apache.seatunnel.api.common.JobContext;
import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.source.Boundedness;
import org.apache.seatunnel.api.source.SeaTunnelSource;
import org.apache.seatunnel.api.source.SourceReader;
import org.apache.seatunnel.api.source.SourceSplitEnumerator;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.common.constants.JobMode;
import org.apache.seatunnel.connectors.seatunnel.paimon.catalog.PaimonCatalog;
import org.apache.seatunnel.connectors.seatunnel.paimon.config.PaimonSourceConfig;
import org.apache.seatunnel.connectors.seatunnel.paimon.source.converter.SqlToPaimonPredicateConverter;
import org.apache.seatunnel.connectors.seatunnel.paimon.source.enumerator.PaimonBatchSourceSplitEnumerator;
import org.apache.seatunnel.connectors.seatunnel.paimon.source.enumerator.PaimonStreamSourceSplitEnumerator;
import org.apache.seatunnel.connectors.seatunnel.paimon.utils.RowTypeConverter;

import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.source.ReadBuilder;
import org.apache.paimon.types.RowType;

import lombok.extern.slf4j.Slf4j;
import net.sf.jsqlparser.statement.select.PlainSelect;

import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static org.apache.seatunnel.connectors.seatunnel.paimon.source.converter.SqlToPaimonPredicateConverter.convertSqlSelectToPaimonProjectionIndex;
import static org.apache.seatunnel.connectors.seatunnel.paimon.source.converter.SqlToPaimonPredicateConverter.convertToPlainSelect;

/** Paimon connector source class. */
@Slf4j
public class PaimonSource
        implements SeaTunnelSource<SeaTunnelRow, PaimonSourceSplit, PaimonSourceState> {

    private static final long serialVersionUID = 1L;

    public static final String PLUGIN_NAME = "Paimon";

    private JobContext jobContext;

    private List<CatalogTable> catalogTables = Lists.newArrayList();
    private Map<String, FileStoreTable> paimonTables = Maps.newHashMap();
    private Map<String, SeaTunnelRowType> seaTunnelRowTypes = Maps.newHashMap();
    private Map<String, ReadBuilder> readBuilders = Maps.newHashMap();

    public PaimonSource(ReadonlyConfig readonlyConfig, PaimonCatalog paimonCatalog) {
        new PaimonSourceConfig(readonlyConfig)
                .getTableConfigList()
                .forEach(
                        tableConfig -> {
                            TablePath tablePath = tableConfig.getTablePath();
                            CatalogTable catalogTable = paimonCatalog.getTable(tablePath);
                            FileStoreTable paimonTable =
                                    (FileStoreTable) paimonCatalog.getPaimonTable(tablePath);
                            String query = tableConfig.getQuery();
                            Map<String, String> dynamicOptions =
                                    SqlToPaimonPredicateConverter.parseDynamicOptions(query);
                            if (!dynamicOptions.isEmpty()) {
                                paimonTable = paimonTable.copy(dynamicOptions);
                            }
                            RowType paimonRowType = paimonTable.rowType();
                            String[] filedNames =
                                    paimonRowType.getFieldNames().toArray(new String[0]);
                            PlainSelect plainSelect = convertToPlainSelect(query);
                            Predicate predicate = null;
                            int[] projectionIndex = null;
                            if (!Objects.isNull(plainSelect)) {
                                projectionIndex =
                                        convertSqlSelectToPaimonProjectionIndex(
                                                filedNames, plainSelect);
                                if (!Objects.isNull(projectionIndex)) {
                                    catalogTable =
                                            paimonCatalog.getTableWithProjection(
                                                    tablePath, projectionIndex);
                                }
                                predicate =
                                        SqlToPaimonPredicateConverter
                                                .convertSqlWhereToPaimonPredicate(
                                                        paimonRowType, plainSelect);
                            }
                            this.catalogTables.add(catalogTable);
                            String tableKey = tablePath.toString();
                            this.seaTunnelRowTypes.put(
                                    tableKey,
                                    RowTypeConverter.convert(paimonRowType, projectionIndex));
                            ReadBuilder readBuilder =
                                    paimonTable
                                            .newReadBuilder()
                                            .withProjection(projectionIndex)
                                            .withFilter(predicate);
                            this.paimonTables.put(tableKey, paimonTable);
                            this.readBuilders.put(tableKey, readBuilder);
                        });
    }

    @Override
    public String getPluginName() {
        return PLUGIN_NAME;
    }

    @Override
    public List<CatalogTable> getProducedCatalogTables() {
        return catalogTables;
    }

    @Override
    public void setJobContext(JobContext jobContext) {
        this.jobContext = jobContext;
    }

    @Override
    public Boundedness getBoundedness() {
        return JobMode.BATCH.equals(jobContext.getJobMode())
                ? Boundedness.BOUNDED
                : Boundedness.UNBOUNDED;
    }

    @Override
    public SourceReader<SeaTunnelRow, PaimonSourceSplit> createReader(
            SourceReader.Context readerContext) throws Exception {
        return new PaimonSourceReader(readerContext, paimonTables, seaTunnelRowTypes, readBuilders);
    }

    @Override
    public SourceSplitEnumerator<PaimonSourceSplit, PaimonSourceState> createEnumerator(
            SourceSplitEnumerator.Context<PaimonSourceSplit> enumeratorContext) throws Exception {
        if (getBoundedness() == Boundedness.BOUNDED) {
            return new PaimonBatchSourceSplitEnumerator(
                    enumeratorContext, new LinkedList<>(), null, readBuilders, 1);
        }
        return new PaimonStreamSourceSplitEnumerator(
                enumeratorContext, new LinkedList<>(), null, readBuilders, 1);
    }

    @Override
    public SourceSplitEnumerator<PaimonSourceSplit, PaimonSourceState> restoreEnumerator(
            SourceSplitEnumerator.Context<PaimonSourceSplit> enumeratorContext,
            PaimonSourceState checkpointState)
            throws Exception {
        if (getBoundedness() == Boundedness.BOUNDED) {
            return new PaimonBatchSourceSplitEnumerator(
                    enumeratorContext,
                    checkpointState.getAssignedSplits(),
                    checkpointState.getCurrentSnapshotId(),
                    readBuilders,
                    1);
        }
        return new PaimonStreamSourceSplitEnumerator(
                enumeratorContext,
                checkpointState.getAssignedSplits(),
                checkpointState.getCurrentSnapshotId(),
                readBuilders,
                1);
    }
}
