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
import org.apache.seatunnel.connectors.seatunnel.tdengine.exception.TDengineConnectorErrorCode;
import org.apache.seatunnel.connectors.seatunnel.tdengine.exception.TDengineConnectorException;
import org.apache.seatunnel.connectors.seatunnel.tdengine.state.TDengineSourceState;
import org.apache.seatunnel.connectors.seatunnel.tdengine.typemapper.TDengineTypeMapper;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.ArrayUtils;

import com.google.auto.service.AutoService;
import com.taosdata.jdbc.TSDBDriver;
import lombok.SneakyThrows;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Properties;

import static org.apache.seatunnel.connectors.seatunnel.tdengine.config.TDengineSourceConfig.ConfigNames.DATABASE;
import static org.apache.seatunnel.connectors.seatunnel.tdengine.config.TDengineSourceConfig.ConfigNames.PASSWORD;
import static org.apache.seatunnel.connectors.seatunnel.tdengine.config.TDengineSourceConfig.ConfigNames.STABLE;
import static org.apache.seatunnel.connectors.seatunnel.tdengine.config.TDengineSourceConfig.ConfigNames.URL;
import static org.apache.seatunnel.connectors.seatunnel.tdengine.config.TDengineSourceConfig.ConfigNames.USERNAME;
import static org.apache.seatunnel.connectors.seatunnel.tdengine.config.TDengineSourceConfig.buildSourceConfig;
import static org.apache.seatunnel.connectors.seatunnel.tdengine.utils.TDengineUtil.checkDriverExist;

/**
 * TDengine source each split corresponds one subtable
 *
 * <p>TODO: wait for optimization 1. batch -> batch + stream 2. one item of data writing -> a batch
 * of data writing
 */
@AutoService(SeaTunnelSource.class)
public class TDengineSource
        implements SeaTunnelSource<SeaTunnelRow, TDengineSourceSplit, TDengineSourceState> {

    private StableMetadata stableMetadata;
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
        stableMetadata = getStableMetadata(tdengineSourceConfig);
    }

    @Override
    public Boundedness getBoundedness() {
        return Boundedness.BOUNDED;
    }

    @Override
    public SeaTunnelDataType<SeaTunnelRow> getProducedType() {
        return stableMetadata.getRowType();
    }

    @Override
    public SourceReader<SeaTunnelRow, TDengineSourceSplit> createReader(Context readerContext) {
        return new TDengineSourceReader(tdengineSourceConfig, readerContext);
    }

    @Override
    public SourceSplitEnumerator<TDengineSourceSplit, TDengineSourceState> createEnumerator(
            SourceSplitEnumerator.Context<TDengineSourceSplit> enumeratorContext) {
        return new TDengineSourceSplitEnumerator(
                stableMetadata, tdengineSourceConfig, enumeratorContext);
    }

    @Override
    public SourceSplitEnumerator<TDengineSourceSplit, TDengineSourceState> restoreEnumerator(
            SourceSplitEnumerator.Context<TDengineSourceSplit> enumeratorContext,
            TDengineSourceState checkpointState) {
        return new TDengineSourceSplitEnumerator(
                stableMetadata, tdengineSourceConfig, checkpointState, enumeratorContext);
    }

    private StableMetadata getStableMetadata(TDengineSourceConfig config) throws SQLException {
        String timestampFieldName = null;
        List<String> subTableNames = new ArrayList<>();
        List<String> fieldNames = new ArrayList<>();
        List<SeaTunnelDataType<?>> fieldTypes = new ArrayList<>();
        List<String> fields = config.getFields();

        String jdbcUrl = String.join("", config.getUrl(), config.getDatabase());

        // check td driver whether exist and if not, try to register
        checkDriverExist(jdbcUrl);

        Properties properties = new Properties();
        properties.put(TSDBDriver.PROPERTY_KEY_USER, config.getUsername());
        properties.put(TSDBDriver.PROPERTY_KEY_PASSWORD, config.getPassword());
        String metaSQL =
                String.format(
                        "select table_name from information_schema.ins_tables where db_name = '%s' and stable_name='%s'",
                        config.getDatabase(), config.getStable());
        try (Connection conn = DriverManager.getConnection(jdbcUrl, properties);
                Statement statement = conn.createStatement();
                ResultSet metaResultSet =
                        statement.executeQuery(
                                String.format(
                                        "desc %s.%s", config.getDatabase(), config.getStable()));
                ResultSet subTableNameResultSet = statement.executeQuery(metaSQL)) {
            while (metaResultSet.next()) {
                if (timestampFieldName == null) {
                    timestampFieldName = metaResultSet.getString(1);
                }
                String fieldName = metaResultSet.getString(1);
                if (CollectionUtils.isNotEmpty(fields)) {
                    if (fields.contains(fieldName)) {
                        fieldNames.add(fieldName);
                        fieldTypes.add(TDengineTypeMapper.mapping(metaResultSet.getString(2)));
                    }
                } else {
                    fieldNames.add(fieldName);
                    fieldTypes.add(TDengineTypeMapper.mapping(metaResultSet.getString(2)));
                }
            }

            while (subTableNameResultSet.next()) {
                String subTableName = subTableNameResultSet.getString(1);
                subTableNames.add(subTableName);
            }
        }

        SeaTunnelRowType rowType = addHiddenAttribute(fieldNames, fieldTypes);
        return new StableMetadata(rowType, timestampFieldName, subTableNames);
    }

    private SeaTunnelRowType addHiddenAttribute(
            List<String> fieldNames, List<SeaTunnelDataType<?>> fieldTypes) {
        // add subtable_name and tags to `seaTunnelRowType`
        // 0-subtable_name / 1-n field_names /
        String[] newFieldNames =
                ArrayUtils.add(fieldNames.toArray(new String[0]), 0, "subtable_name");
        // n+1-> tags
        SeaTunnelDataType<?>[] newFieldTypes =
                ArrayUtils.add(
                        fieldTypes.toArray(new SeaTunnelDataType[0]), 0, BasicType.STRING_TYPE);
        return new SeaTunnelRowType(newFieldNames, newFieldTypes);
    }
}
