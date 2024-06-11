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


import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.IntStream;

import static org.apache.seatunnel.connectors.seatunnel.paimon.config.PaimonConfig.DATABASE;
import static org.apache.seatunnel.connectors.seatunnel.paimon.config.PaimonConfig.HDFS_SITE_PATH;
import static org.apache.seatunnel.connectors.seatunnel.paimon.config.PaimonConfig.READ_COLUMNS;
import static org.apache.seatunnel.connectors.seatunnel.paimon.config.PaimonConfig.TABLE;
import static org.apache.seatunnel.connectors.seatunnel.paimon.config.PaimonConfig.WAREHOUSE;

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

    private Predicate predicate;

    private CatalogTable catalogTable;

    public PaimonSource(ReadonlyConfig readonlyConfig, PaimonCatalog paimonCatalog) {
        this.readonlyConfig = readonlyConfig;
        PaimonSourceConfig paimonSourceConfig = new PaimonSourceConfig(readonlyConfig);
        TablePath tablePath =
                TablePath.of(paimonSourceConfig.getNamespace(), paimonSourceConfig.getTable());
        this.catalogTable = paimonCatalog.getTable(tablePath);
        this.paimonTable = paimonCatalog.getPaimonTable(tablePath);
        this.seaTunnelRowType = catalogTable.getSeaTunnelRowType();
        // TODO: We can use this to realize the column projection feature later
        String filterSql = readonlyConfig.get(PaimonSourceConfig.QUERY_SQL);
        this.predicate =
                SqlToPaimonPredicateConverter.convertSqlWhereToPaimonPredicate(
                        this.paimonTable.rowType(), filterSql);
    }

    private int[] projectionIndex = null;

    private String[] projectionFieldNames = null;

    @Override
    public String getPluginName() {
        return PLUGIN_NAME;
    }

    @Override
    public void prepare(Config pluginConfig) throws PrepareFailException {
        this.pluginConfig = pluginConfig;
        final CheckResult result =
                CheckConfigUtil.checkAllExists(
                        pluginConfig, WAREHOUSE.key(), DATABASE.key(), TABLE.key());
        if (!result.isSuccess()) {
            throw new PaimonConnectorException(
                    SeaTunnelAPIErrorCode.CONFIG_VALIDATION_FAILED,
                    String.format(
                            "PluginName: %s, PluginType: %s, Message: %s",
                            getPluginName(), PluginType.SOURCE, result.getMsg()));
        }
        // initialize paimon table
        final String warehouse = pluginConfig.getString(WAREHOUSE.key());
        final String database = pluginConfig.getString(DATABASE.key());
        final String table = pluginConfig.getString(TABLE.key());
        final Map<String, String> optionsMap = new HashMap<>();
        optionsMap.put(WAREHOUSE.key(), warehouse);
        final Options options = Options.fromMap(optionsMap);
        final Configuration hadoopConf = new Configuration();
        if (pluginConfig.hasPath(HDFS_SITE_PATH.key())) {
            hadoopConf.addResource(new Path(pluginConfig.getString(HDFS_SITE_PATH.key())));
        }
        final CatalogContext catalogContext = CatalogContext.create(options, hadoopConf);
        try (Catalog catalog = CatalogFactory.createCatalog(catalogContext)) {
            Identifier identifier = Identifier.create(database, table);
            this.table = catalog.getTable(identifier);
        } catch (Exception e) {
            String errorMsg =
                    String.format(
                            "Failed to get table [%s] from database [%s] on warehouse [%s]",
                            database, table, warehouse);
            throw new PaimonConnectorException(
                    PaimonConnectorErrorCode.GET_TABLE_FAILED, errorMsg, e);
        }
        if (pluginConfig.hasPath(READ_COLUMNS.key())) {
            String projectString = pluginConfig.getString(READ_COLUMNS.key());
            this.projectionFieldNames = projectString.split(",");
            String[] fieldNames = this.table.rowType().getFieldNames().toArray(new String[0]);
            this.projectionIndex =
                    IntStream.range(0, projectionFieldNames.length)
                            .map(
                                    i -> {
                                        String fieldName = projectionFieldNames[i];
                                        int index = Arrays.asList(fieldNames).indexOf(fieldName);
                                        if (index == -1) {
                                            throw new IllegalArgumentException(
                                                    "column " + fieldName + " does not exist.");
                                        }
                                        return index;
                                    })
                            .toArray();
        }

        seaTunnelRowType = RowTypeConverter.convert(this.table.rowType(), projectionIndex);
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
        // return new PaimonSourceReader(readerContext, table, seaTunnelRowType, projectionIndex);

        return new PaimonSourceReader(readerContext, paimonTable, seaTunnelRowType, predicate);
    }

    @Override
    public SourceSplitEnumerator<PaimonSourceSplit, PaimonSourceState> createEnumerator(
            SourceSplitEnumerator.Context<PaimonSourceSplit> enumeratorContext) throws Exception {
        return new PaimonSourceSplitEnumerator(enumeratorContext, paimonTable, predicate);
    }

    @Override
    public SourceSplitEnumerator<PaimonSourceSplit, PaimonSourceState> restoreEnumerator(
            SourceSplitEnumerator.Context<PaimonSourceSplit> enumeratorContext,
            PaimonSourceState checkpointState)
            throws Exception {
        return new PaimonSourceSplitEnumerator(
                enumeratorContext, paimonTable, checkpointState, predicate);
    }
}
