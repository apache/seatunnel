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

import static org.apache.seatunnel.connectors.seatunnel.influxdb.config.SourceConfig.SQL;

import org.apache.seatunnel.api.common.PrepareFailException;
import org.apache.seatunnel.api.common.SeaTunnelAPIErrorCode;
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
import org.apache.seatunnel.connectors.seatunnel.common.schema.SeaTunnelSchema;
import org.apache.seatunnel.connectors.seatunnel.influxdb.client.InfluxDBClient;
import org.apache.seatunnel.connectors.seatunnel.influxdb.config.SourceConfig;
import org.apache.seatunnel.connectors.seatunnel.influxdb.exception.InfluxdbConnectorErrorCode;
import org.apache.seatunnel.connectors.seatunnel.influxdb.exception.InfluxdbConnectorException;
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
public class InfluxDBSource implements SeaTunnelSource<SeaTunnelRow, InfluxDBSourceSplit, InfluxDBSourceState> {
    private SeaTunnelRowType typeInfo;
    private SourceConfig sourceConfig;

    private List<Integer> columnsIndexList;

    private static final String QUERY_LIMIT = " limit 1";

    @Override
    public String getPluginName() {
        return "InfluxDB";
    }

    @Override
    public void prepare(Config config) throws PrepareFailException {
        CheckResult result = CheckConfigUtil.checkAllExists(config, SQL.key());
        if (!result.isSuccess()) {
            throw new InfluxdbConnectorException(SeaTunnelAPIErrorCode.CONFIG_VALIDATION_FAILED,
                String.format("PluginName: %s, PluginType: %s, Message: %s",
                    getPluginName(), PluginType.SOURCE,
                    result.getMsg()
                )
            );
        }
        try {
            this.sourceConfig = SourceConfig.loadConfig(config);
            SeaTunnelSchema seatunnelSchema = SeaTunnelSchema.buildWithConfig(config);
            this.typeInfo = seatunnelSchema.getSeaTunnelRowType();
            this.columnsIndexList = initColumnsIndex(InfluxDBClient.getInfluxDB(sourceConfig));
        } catch (Exception e) {
            throw new InfluxdbConnectorException(SeaTunnelAPIErrorCode.CONFIG_VALIDATION_FAILED,
                String.format("PluginName: %s, PluginType: %s, Message: %s",
                    getPluginName(), PluginType.SOURCE, e)
            );
        }
    }

    @Override
    public Boundedness getBoundedness() {
        return Boundedness.BOUNDED;
    }

    @Override
    public SeaTunnelDataType<SeaTunnelRow> getProducedType() {
        return typeInfo;
    }

    @Override
    public SourceReader createReader(SourceReader.Context readerContext) throws Exception {
        return new InfluxdbSourceReader(sourceConfig, readerContext, typeInfo, columnsIndexList);
    }

    @Override
    public SourceSplitEnumerator createEnumerator(SourceSplitEnumerator.Context enumeratorContext) throws Exception {
        return new InfluxDBSourceSplitEnumerator(enumeratorContext, sourceConfig);
    }

    @Override
    public SourceSplitEnumerator<InfluxDBSourceSplit, InfluxDBSourceState> restoreEnumerator(SourceSplitEnumerator.Context<InfluxDBSourceSplit> enumeratorContext, InfluxDBSourceState checkpointState) throws Exception {
        return new InfluxDBSourceSplitEnumerator(enumeratorContext, checkpointState, sourceConfig);
    }

    private List<Integer> initColumnsIndex(InfluxDB influxdb) {
        //query one row to get column info
        String query = sourceConfig.getSql() + QUERY_LIMIT;
        try {
            QueryResult queryResult = influxdb.query(
                new Query(query, sourceConfig.getDatabase()));

            List<QueryResult.Series> serieList = queryResult.getResults().get(0).getSeries();
            List<String> fieldNames = new ArrayList<>(serieList.get(0).getColumns());

            return Arrays.stream(typeInfo.getFieldNames()).map(fieldNames::indexOf).collect(Collectors.toList());
        } catch (Exception e) {
            throw new InfluxdbConnectorException(InfluxdbConnectorErrorCode.GET_COLUMN_INDEX_FAILED,
                "Get column index of query result exception", e);
        }
    }
}
