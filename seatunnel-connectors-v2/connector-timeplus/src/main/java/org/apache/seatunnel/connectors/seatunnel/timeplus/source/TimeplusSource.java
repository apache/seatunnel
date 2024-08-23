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

package org.apache.seatunnel.connectors.seatunnel.timeplus.source;

import org.apache.seatunnel.shade.com.typesafe.config.Config;
import org.apache.seatunnel.shade.com.typesafe.config.ConfigFactory;

import org.apache.seatunnel.api.common.PrepareFailException;
import org.apache.seatunnel.api.common.SeaTunnelAPIErrorCode;
import org.apache.seatunnel.api.source.Boundedness;
import org.apache.seatunnel.api.source.SeaTunnelSource;
import org.apache.seatunnel.api.source.SourceReader;
import org.apache.seatunnel.api.source.SourceSplitEnumerator;
import org.apache.seatunnel.api.source.SupportColumnProjection;
import org.apache.seatunnel.api.source.SupportParallelism;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.common.config.CheckConfigUtil;
import org.apache.seatunnel.common.config.CheckResult;
import org.apache.seatunnel.common.constants.PluginType;
import org.apache.seatunnel.connectors.seatunnel.timeplus.exception.TimeplusConnectorException;
import org.apache.seatunnel.connectors.seatunnel.timeplus.state.TimeplusSourceState;
import org.apache.seatunnel.connectors.seatunnel.timeplus.util.TimeplusUtil;
import org.apache.seatunnel.connectors.seatunnel.timeplus.util.TypeConvertUtil;

import com.google.auto.service.AutoService;
import com.google.common.collect.ImmutableMap;
import com.timeplus.proton.client.ProtonClient;
import com.timeplus.proton.client.ProtonException;
import com.timeplus.proton.client.ProtonFormat;
import com.timeplus.proton.client.ProtonNode;
import com.timeplus.proton.client.ProtonResponse;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Collectors;

import static org.apache.seatunnel.connectors.seatunnel.timeplus.config.TimeplusConfig.DATABASE;
import static org.apache.seatunnel.connectors.seatunnel.timeplus.config.TimeplusConfig.HOST;
import static org.apache.seatunnel.connectors.seatunnel.timeplus.config.TimeplusConfig.PASSWORD;
import static org.apache.seatunnel.connectors.seatunnel.timeplus.config.TimeplusConfig.SERVER_TIME_ZONE;
import static org.apache.seatunnel.connectors.seatunnel.timeplus.config.TimeplusConfig.SQL;
import static org.apache.seatunnel.connectors.seatunnel.timeplus.config.TimeplusConfig.TABLE;
import static org.apache.seatunnel.connectors.seatunnel.timeplus.config.TimeplusConfig.TIMEPLUS_CONFIG;
import static org.apache.seatunnel.connectors.seatunnel.timeplus.config.TimeplusConfig.USERNAME;

@AutoService(SeaTunnelSource.class)
public class TimeplusSource
        implements SeaTunnelSource<SeaTunnelRow, TimeplusSourceSplit, TimeplusSourceState>,
                SupportParallelism,
                SupportColumnProjection {

    private List<ProtonNode> servers;
    private SeaTunnelRowType rowTypeInfo;
    private String sql;

    @Override
    public String getPluginName() {
        return "Timeplus";
    }

    @Override
    public void prepare(Config config) throws PrepareFailException {
        CheckResult result = CheckConfigUtil.checkAllExists(config, SQL.key(), TABLE.key());
        if (!result.isSuccess()) {
            throw new TimeplusConnectorException(
                    SeaTunnelAPIErrorCode.CONFIG_VALIDATION_FAILED,
                    String.format(
                            "PluginName: %s, PluginType: %s, Message: %s",
                            getPluginName(), PluginType.SOURCE, result.getMsg()));
        }
        Map<String, Object> defaultConfig =
                ImmutableMap.<String, Object>builder()
                        .put(HOST.key(), HOST.defaultValue())
                        .put(DATABASE.key(), DATABASE.defaultValue())
                        .put(SERVER_TIME_ZONE.key(), SERVER_TIME_ZONE.defaultValue())
                        .put(USERNAME.key(), USERNAME.defaultValue())
                        .put(PASSWORD.key(), PASSWORD.defaultValue())
                        .build();

        config = config.withFallback(ConfigFactory.parseMap(defaultConfig));

        Map<String, String> customConfig = null;

        if (CheckConfigUtil.isValidParam(config, TIMEPLUS_CONFIG.key())) {
            customConfig =
                    config.getObject(TIMEPLUS_CONFIG.key()).entrySet().stream()
                            .collect(
                                    Collectors.toMap(
                                            Map.Entry::getKey,
                                            entrySet ->
                                                    entrySet.getValue().unwrapped().toString()));
        }

        servers =
                TimeplusUtil.createNodes(
                        config.getString(HOST.key()),
                        config.getString(DATABASE.key()),
                        config.getString(SERVER_TIME_ZONE.key()),
                        config.getString(USERNAME.key()),
                        config.getString(PASSWORD.key()),
                        customConfig);

        sql = config.getString(SQL.key());
        ProtonNode currentServer = servers.get(ThreadLocalRandom.current().nextInt(servers.size()));
        try (ProtonClient client = ProtonClient.newInstance(currentServer.getProtocol());
                ProtonResponse response =
                        client.connect(currentServer)
                                .format(ProtonFormat.RowBinaryWithNamesAndTypes)
                                .query(modifySQLToLimit1(config.getString(SQL.key())))
                                .executeAndWait()) {

            int columnSize = response.getColumns().size();
            String[] fieldNames = new String[columnSize];
            SeaTunnelDataType<?>[] seaTunnelDataTypes = new SeaTunnelDataType[columnSize];

            for (int i = 0; i < columnSize; i++) {
                fieldNames[i] = response.getColumns().get(i).getColumnName();
                seaTunnelDataTypes[i] = TypeConvertUtil.convert(response.getColumns().get(i));
            }

            this.rowTypeInfo = new SeaTunnelRowType(fieldNames, seaTunnelDataTypes);

        } catch (ProtonException e) {
            throw new TimeplusConnectorException(
                    SeaTunnelAPIErrorCode.CONFIG_VALIDATION_FAILED,
                    String.format(
                            "PluginName: %s, PluginType: %s, Message: %s",
                            getPluginName(), PluginType.SOURCE, e.getMessage()));
        }
    }

    private String modifySQLToLimit1(String sql) {
        return String.format("SELECT * FROM (%s) LIMIT 1", sql);
    }

    @Override
    public Boundedness getBoundedness() {
        return Boundedness.BOUNDED;
    }

    @Override
    public SeaTunnelRowType getProducedType() {
        return this.rowTypeInfo;
    }

    @Override
    public SourceReader<SeaTunnelRow, TimeplusSourceSplit> createReader(
            SourceReader.Context readerContext) throws Exception {
        return new TimeplusSourceReader(servers, readerContext, this.rowTypeInfo, sql);
    }

    @Override
    public SourceSplitEnumerator<TimeplusSourceSplit, TimeplusSourceState> createEnumerator(
            SourceSplitEnumerator.Context<TimeplusSourceSplit> enumeratorContext) throws Exception {
        return new TimeplusSourceSplitEnumerator(enumeratorContext);
    }

    @Override
    public SourceSplitEnumerator<TimeplusSourceSplit, TimeplusSourceState> restoreEnumerator(
            SourceSplitEnumerator.Context<TimeplusSourceSplit> enumeratorContext,
            TimeplusSourceState checkpointState)
            throws Exception {
        return new TimeplusSourceSplitEnumerator(enumeratorContext);
    }
}
