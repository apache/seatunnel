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

package org.apache.seatunnel.connectors.seatunnel.tdengine.source;

import org.apache.seatunnel.shade.com.typesafe.config.Config;

import org.apache.seatunnel.api.common.PrepareFailException;
import org.apache.seatunnel.api.common.SeaTunnelAPIErrorCode;
import org.apache.seatunnel.api.source.Boundedness;
import org.apache.seatunnel.api.source.SeaTunnelSource;
import org.apache.seatunnel.api.source.SourceReader;
import org.apache.seatunnel.api.source.SourceReader.Context;
import org.apache.seatunnel.api.source.SourceSplitEnumerator;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.common.config.CheckConfigUtil;
import org.apache.seatunnel.common.config.CheckResult;
import org.apache.seatunnel.connectors.seatunnel.tdengine.config.TDengineSourceConfig;
import org.apache.seatunnel.connectors.seatunnel.tdengine.exception.TDengineConnectorException;
import org.apache.seatunnel.connectors.seatunnel.tdengine.state.TDengineSourceState;
import org.apache.seatunnel.connectors.seatunnel.tdengine.typemapper.TDengineTypeMapper;

import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.StringUtils;

import com.google.auto.service.AutoService;
import com.google.common.collect.Lists;
import lombok.SneakyThrows;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.List;

import static org.apache.seatunnel.connectors.seatunnel.tdengine.config.TDengineSourceConfig.ConfigNames.DATABASE;
import static org.apache.seatunnel.connectors.seatunnel.tdengine.config.TDengineSourceConfig.ConfigNames.PASSWORD;
import static org.apache.seatunnel.connectors.seatunnel.tdengine.config.TDengineSourceConfig.ConfigNames.STABLE;
import static org.apache.seatunnel.connectors.seatunnel.tdengine.config.TDengineSourceConfig.ConfigNames.URL;
import static org.apache.seatunnel.connectors.seatunnel.tdengine.config.TDengineSourceConfig.ConfigNames.USERNAME;
import static org.apache.seatunnel.connectors.seatunnel.tdengine.config.TDengineSourceConfig.buildSourceConfig;

/**
 * TDengine source each split corresponds one subtable
 *
 * <p>TODO: wait for optimization 1. batch -> batch + stream 2. one item of data writing -> a batch
 * of data writing
 */
@AutoService(SeaTunnelSource.class)
public class TDengineSource
        implements SeaTunnelSource<SeaTunnelRow, TDengineSourceSplit, TDengineSourceState> {

    private SeaTunnelRowType seaTunnelRowType;
    private TDengineSourceConfig tdengineSourceConfig;

    @Override
    public String getPluginName() {
        return "TDengine";
    }

    @SneakyThrows
    @Override
    public void prepare(Config pluginConfig) throws PrepareFailException {
        CheckResult result =
                CheckConfigUtil.checkAllExists(
                        pluginConfig, URL, DATABASE, STABLE, USERNAME, PASSWORD);
        if (!result.isSuccess()) {
            throw new TDengineConnectorException(
                    SeaTunnelAPIErrorCode.CONFIG_VALIDATION_FAILED,
                    "TDengine connection require url/database/stable/username/password. All of these must not be empty.");
        }
        tdengineSourceConfig = buildSourceConfig(pluginConfig);

        // add subtable_name and tags to `seaTunnelRowType`
        SeaTunnelRowType originRowType = getSTableMetaInfo(tdengineSourceConfig);
        seaTunnelRowType = addHiddenAttribute(originRowType);
    }

    @SneakyThrows
    private SeaTunnelRowType getSTableMetaInfo(TDengineSourceConfig config) {
        String jdbcUrl =
                StringUtils.join(
                        config.getUrl(),
                        config.getDatabase(),
                        "?user=",
                        config.getUsername(),
                        "&password=",
                        config.getPassword());
        Connection conn = DriverManager.getConnection(jdbcUrl);
        List<String> fieldNames = Lists.newArrayList();
        List<SeaTunnelDataType<?>> fieldTypes = Lists.newArrayList();
        try (Statement statement = conn.createStatement()) {
            final ResultSet metaResultSet =
                    statement.executeQuery(
                            "desc " + config.getDatabase() + "." + config.getStable());
            while (metaResultSet.next()) {
                fieldNames.add(metaResultSet.getString(1));
                fieldTypes.add(TDengineTypeMapper.mapping(metaResultSet.getString(2)));
            }
        }
        return new SeaTunnelRowType(
                fieldNames.toArray(new String[0]), fieldTypes.toArray(new SeaTunnelDataType<?>[0]));
    }

    private SeaTunnelRowType addHiddenAttribute(SeaTunnelRowType originRowType) {
        // 0-subtable_name / 1-n field_names /
        String[] fieldNames = ArrayUtils.add(originRowType.getFieldNames(), 0, "subtable_name");
        // n+1-> tags
        SeaTunnelDataType<?>[] fieldTypes =
                ArrayUtils.add(originRowType.getFieldTypes(), 0, BasicType.STRING_TYPE);
        return new SeaTunnelRowType(fieldNames, fieldTypes);
    }

    @Override
    public Boundedness getBoundedness() {
        return Boundedness.BOUNDED;
    }

    @Override
    public SeaTunnelDataType<SeaTunnelRow> getProducedType() {
        return seaTunnelRowType;
    }

    @Override
    public SourceReader<SeaTunnelRow, TDengineSourceSplit> createReader(Context readerContext) {
        return new TDengineSourceReader(tdengineSourceConfig, readerContext);
    }

    @Override
    public SourceSplitEnumerator<TDengineSourceSplit, TDengineSourceState> createEnumerator(
            SourceSplitEnumerator.Context<TDengineSourceSplit> enumeratorContext) {
        return new TDengineSourceSplitEnumerator(
                seaTunnelRowType, tdengineSourceConfig, enumeratorContext);
    }

    @Override
    public SourceSplitEnumerator<TDengineSourceSplit, TDengineSourceState> restoreEnumerator(
            SourceSplitEnumerator.Context<TDengineSourceSplit> enumeratorContext,
            TDengineSourceState checkpointState) {
        return new TDengineSourceSplitEnumerator(
                seaTunnelRowType, tdengineSourceConfig, checkpointState, enumeratorContext);
    }
}
