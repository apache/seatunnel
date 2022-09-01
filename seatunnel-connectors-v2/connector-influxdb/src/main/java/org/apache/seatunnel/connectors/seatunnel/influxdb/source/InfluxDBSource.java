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

import static org.apache.seatunnel.connectors.seatunnel.influxdb.config.InfluxDBConfig.QUERY_FIELD_SQL;
import static org.apache.seatunnel.connectors.seatunnel.influxdb.config.InfluxDBConfig.QUERY_TAG_SQL;
import static org.apache.seatunnel.connectors.seatunnel.influxdb.config.InfluxDBConfig.SQL;
import static org.apache.seatunnel.connectors.seatunnel.influxdb.config.InfluxDBConfig.URL;
import static org.apache.seatunnel.connectors.seatunnel.influxdb.converter.InfluxDBTypeMapper.INFLUXDB_BIGINT;

import org.apache.seatunnel.api.common.PrepareFailException;
import org.apache.seatunnel.api.source.Boundedness;
import org.apache.seatunnel.api.source.SeaTunnelSource;
import org.apache.seatunnel.api.source.SourceReader;
import org.apache.seatunnel.api.source.SourceSplitEnumerator;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.common.config.CheckConfigUtil;
import org.apache.seatunnel.common.config.CheckResult;
import org.apache.seatunnel.common.constants.PluginType;
import org.apache.seatunnel.connectors.seatunnel.influxdb.client.InfluxDBClient;
import org.apache.seatunnel.connectors.seatunnel.influxdb.config.InfluxDBConfig;
import org.apache.seatunnel.connectors.seatunnel.influxdb.converter.InfluxDBTypeMapper;
import org.apache.seatunnel.connectors.seatunnel.influxdb.state.InfluxDBSourceState;

import org.apache.seatunnel.shade.com.typesafe.config.Config;

import com.google.auto.service.AutoService;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.influxdb.InfluxDB;
import org.influxdb.dto.Query;
import org.influxdb.dto.QueryResult;

import java.util.ArrayList;
import java.util.List;

@Slf4j
@AutoService(SeaTunnelSource.class)
public class InfluxDBSource implements SeaTunnelSource<SeaTunnelRow, InfluxDBSourceSplit, InfluxDBSourceState>  {
    private SeaTunnelRowType seaTunnelRowType;
    private InfluxDBConfig influxDBConfig;

    private static final String INFLUXDB_TIME_KEY = "time";
    private static final String INFLUXDB_TIME_DATATYPE = INFLUXDB_BIGINT;

    @Override
    public String getPluginName() {
        return "InfluxDB";
    }

    @Override
    public void prepare(Config config) throws PrepareFailException {
        CheckResult result = CheckConfigUtil.checkAllExists(config, URL, SQL);
        if (!result.isSuccess()) {
            throw new PrepareFailException(getPluginName(), PluginType.SOURCE, result.getMsg());
        }
        try {
            this.influxDBConfig = new InfluxDBConfig(config);
            InfluxDB influxDB = InfluxDBClient.getInfluxDB(influxDBConfig);
            seaTunnelRowType = initTableField(influxDB, influxDBConfig);
        } catch (Exception e) {
            throw new PrepareFailException("InfluxDB", PluginType.SOURCE, e.toString());
        }
    }

    @Override
    public Boundedness getBoundedness() {
        return Boundedness.BOUNDED;
    }

    @Override
    public SeaTunnelDataType getProducedType() {
        return seaTunnelRowType;
    }

    @Override
    public SourceReader createReader(SourceReader.Context readerContext) throws Exception {
        return new InfluxdbSourceReader(influxDBConfig, readerContext, seaTunnelRowType);
    }

    @Override
    public SourceSplitEnumerator createEnumerator(SourceSplitEnumerator.Context enumeratorContext) throws Exception {
        return new InfluxDBSourceSplitEnumerator(enumeratorContext, influxDBConfig);
    }

    @Override
    public SourceSplitEnumerator<InfluxDBSourceSplit, InfluxDBSourceState> restoreEnumerator(SourceSplitEnumerator.Context<InfluxDBSourceSplit> enumeratorContext, InfluxDBSourceState checkpointState) throws Exception {
        return new InfluxDBSourceSplitEnumerator(enumeratorContext, checkpointState, influxDBConfig);
    }

    SeaTunnelRowType initTableField(InfluxDB influxDB, InfluxDBConfig config)  {
        ArrayList<SeaTunnelDataType<?>> seaTunnelDataTypes = new ArrayList<>();
        String query = config.getSql() + " limit 1";
        List<String> fieldNames = new ArrayList<>();
        try {
            QueryResult queryResult = influxDB.query(
                    new Query(query, config.getDatabase()));

            List<QueryResult.Series> serieList = queryResult.getResults().get(0).getSeries();
            Pair<List<String>, List<String>> resultSetMetaData = getTableMetadata(influxDB, config);

            fieldNames.addAll(serieList.get(0).getColumns());
            if (!CollectionUtils.isEmpty(serieList)) {
                for (int i = 0; i < fieldNames.size(); i++) {
                    seaTunnelDataTypes.add(InfluxDBTypeMapper.mapping(resultSetMetaData, fieldNames.get(i)));
                }
            }
        } catch (Exception e) {
            log.warn("get row type info exception", e);
        }
        return new SeaTunnelRowType(fieldNames.toArray(new String[fieldNames.size()]), seaTunnelDataTypes.toArray(new SeaTunnelDataType<?>[seaTunnelDataTypes.size()]));
    }

    private Pair<List<String>, List<String>> getTableMetadata(InfluxDB influxDB, InfluxDBConfig config) {
        List<String> columnNames = new ArrayList<>();
        List<String> columnTypes = new ArrayList<>();
        QueryResult queryResult =
                influxDB.query(
                        new Query(
                                QUERY_FIELD_SQL.replace("${measurement}", config.getMeasurement()),
                                config.getDatabase()));
        List<QueryResult.Series> serieList = queryResult.getResults().get(0).getSeries();
        if (!CollectionUtils.isEmpty(serieList)) {
            for (List<Object> value : serieList.get(0).getValues()) {
                columnNames.add(String.valueOf(value.get(0)));
                columnTypes.add(String.valueOf(value.get(1)));
            }
        }

        queryResult =
                influxDB.query(
                        new Query(
                                QUERY_TAG_SQL.replace("${measurement}", config.getMeasurement()),
                                config.getDatabase()));
        serieList = queryResult.getResults().get(0).getSeries();
        if (!CollectionUtils.isEmpty(serieList)) {
            for (List<Object> value : serieList.get(0).getValues()) {
                columnNames.add(String.valueOf(value.get(0)));
                // Tag keys and tag values are both strings.
                columnTypes.add("string");
            }
        }
        // add time field.
        columnNames.add(INFLUXDB_TIME_KEY);
        columnTypes.add(INFLUXDB_TIME_DATATYPE);
        return Pair.of(columnNames, columnTypes);
    }
}
