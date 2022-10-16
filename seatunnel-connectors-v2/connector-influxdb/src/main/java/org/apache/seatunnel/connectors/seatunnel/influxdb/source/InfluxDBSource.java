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

import static org.apache.seatunnel.connectors.seatunnel.influxdb.config.InfluxDBConfig.SQL;
import static org.apache.seatunnel.connectors.seatunnel.influxdb.config.InfluxDBConfig.URL;

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
import org.apache.seatunnel.connectors.common.schema.SeaTunnelSchema;
import org.apache.seatunnel.connectors.seatunnel.influxdb.client.InfluxDBClient;
import org.apache.seatunnel.connectors.seatunnel.influxdb.config.InfluxDBConfig;
import org.apache.seatunnel.connectors.seatunnel.influxdb.state.InfluxDBSourceState;

import org.apache.seatunnel.shade.com.typesafe.config.Config;

import com.google.auto.service.AutoService;
import lombok.extern.slf4j.Slf4j;
import org.influxdb.InfluxDB;
import org.influxdb.dto.Query;
import org.influxdb.dto.QueryResult;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

@Slf4j
@AutoService(SeaTunnelSource.class)
public class InfluxDBSource implements SeaTunnelSource<SeaTunnelRow, InfluxDBSourceSplit, InfluxDBSourceState>  {
    private SeaTunnelRowType typeInfo;
    private InfluxDBConfig influxDBConfig;

    private List<Integer> columnsIndexList;

    private static final String QUERY_LIMIT = " limit 1";

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
            SeaTunnelSchema seatunnelSchema = SeaTunnelSchema.buildWithConfig(config);
            this.typeInfo = seatunnelSchema.getSeaTunnelRowType();
            this.columnsIndexList = initColumnsIndex(InfluxDBClient.getInfluxDB(influxDBConfig));
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
        return typeInfo;
    }

    @Override
    public SourceReader createReader(SourceReader.Context readerContext) throws Exception {
        return new InfluxdbSourceReader(influxDBConfig, readerContext, typeInfo, columnsIndexList);
    }

    @Override
    public SourceSplitEnumerator createEnumerator(SourceSplitEnumerator.Context enumeratorContext) throws Exception {
        return new InfluxDBSourceSplitEnumerator(enumeratorContext, influxDBConfig);
    }

    @Override
    public SourceSplitEnumerator<InfluxDBSourceSplit, InfluxDBSourceState> restoreEnumerator(SourceSplitEnumerator.Context<InfluxDBSourceSplit> enumeratorContext, InfluxDBSourceState checkpointState) throws Exception {
        return new InfluxDBSourceSplitEnumerator(enumeratorContext, checkpointState, influxDBConfig);
    }

    private List<Integer> initColumnsIndex(InfluxDB influxDB)  {
        //query one row to get column info
        String query = influxDBConfig.getSql() + QUERY_LIMIT;
        List<String> fieldNames = new ArrayList<>();
        try {
            QueryResult queryResult = influxDB.query(
                    new Query(query, influxDBConfig.getDatabase()));

            List<QueryResult.Series> serieList = queryResult.getResults().get(0).getSeries();
            fieldNames.addAll(serieList.get(0).getColumns());

            return Arrays.stream(typeInfo.getFieldNames()).map(x -> fieldNames.indexOf(x)).collect(Collectors.toList());
        } catch (Exception e) {
            throw new RuntimeException("get column index of query result exception", e);
        }
    }
}
