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

package org.apache.seatunnel.connectors.seatunnel.influxdb.source;

import org.apache.seatunnel.api.source.Boundedness;
import org.apache.seatunnel.api.source.SeaTunnelSource;
import org.apache.seatunnel.api.source.SourceReader;
import org.apache.seatunnel.api.source.SourceSplitEnumerator;
import org.apache.seatunnel.api.source.SupportColumnProjection;
import org.apache.seatunnel.api.source.SupportParallelism;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.connectors.seatunnel.influxdb.client.InfluxDBClient;
import org.apache.seatunnel.connectors.seatunnel.influxdb.config.SourceConfig;
import org.apache.seatunnel.connectors.seatunnel.influxdb.exception.InfluxdbConnectorErrorCode;
import org.apache.seatunnel.connectors.seatunnel.influxdb.exception.InfluxdbConnectorException;
import org.apache.seatunnel.connectors.seatunnel.influxdb.state.InfluxDBSourceState;

import org.influxdb.InfluxDB;
import org.influxdb.dto.Query;
import org.influxdb.dto.QueryResult;

import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

@Slf4j
public class InfluxDBSource
        implements SeaTunnelSource<SeaTunnelRow, InfluxDBSourceSplit, InfluxDBSourceState>,
                SupportParallelism,
                SupportColumnProjection {

    private final CatalogTable catalogTable;
    private final SourceConfig sourceConfig;

    private static final String QUERY_LIMIT = " limit 1";

    public InfluxDBSource(CatalogTable catalogTable, SourceConfig sourceConfig) {
        this.catalogTable = catalogTable;
        this.sourceConfig = sourceConfig;
    }

    @Override
    public String getPluginName() {
        return "InfluxDB";
    }

    @Override
    public Boundedness getBoundedness() {
        return Boundedness.BOUNDED;
    }

    @Override
    public SourceReader createReader(SourceReader.Context readerContext) throws Exception {
        List<Integer> columnsIndexList = initColumnsIndex(InfluxDBClient.getInfluxDB(sourceConfig));
        return new InfluxdbSourceReader(
                sourceConfig, readerContext, catalogTable.getSeaTunnelRowType(), columnsIndexList);
    }

    @Override
    public SourceSplitEnumerator createEnumerator(SourceSplitEnumerator.Context enumeratorContext)
            throws Exception {
        return new InfluxDBSourceSplitEnumerator(enumeratorContext, sourceConfig);
    }

    @Override
    public SourceSplitEnumerator<InfluxDBSourceSplit, InfluxDBSourceState> restoreEnumerator(
            SourceSplitEnumerator.Context<InfluxDBSourceSplit> enumeratorContext,
            InfluxDBSourceState checkpointState)
            throws Exception {
        return new InfluxDBSourceSplitEnumerator(enumeratorContext, checkpointState, sourceConfig);
    }

    @Override
    public List<CatalogTable> getProducedCatalogTables() {
        return Collections.singletonList(catalogTable);
    }

    private List<Integer> initColumnsIndex(InfluxDB influxdb) {
        // query one row to get column info
        String sql = sourceConfig.getSql();
        String query = sql + QUERY_LIMIT;
        // if sql contains tz(), can't be append QUERY_LIMIT at last . see bug #4231
        int start = containTzFunction(sql.toLowerCase());
        if (start > 0) {
            StringBuilder tmpSql = new StringBuilder(sql);
            tmpSql.insert(start - 1, QUERY_LIMIT).append(" ");
            query = tmpSql.toString();
        }

        try {
            QueryResult queryResult = influxdb.query(new Query(query, sourceConfig.getDatabase()));

            List<QueryResult.Series> serieList = queryResult.getResults().get(0).getSeries();
            List<String> fieldNames = new ArrayList<>(serieList.get(0).getColumns());

            return Arrays.stream(catalogTable.getSeaTunnelRowType().getFieldNames())
                    .map(fieldNames::indexOf)
                    .collect(Collectors.toList());
        } catch (Exception e) {
            throw new InfluxdbConnectorException(
                    InfluxdbConnectorErrorCode.GET_COLUMN_INDEX_FAILED,
                    "Get column index of query result exception",
                    e);
        }
    }

    private static int containTzFunction(String sql) {
        Pattern pattern = Pattern.compile("tz\\(.*\\)");
        Matcher matcher = pattern.matcher(sql);
        if (matcher.find()) {
            int start = matcher.start();
            return start;
        }
        return -1;
    }
}
