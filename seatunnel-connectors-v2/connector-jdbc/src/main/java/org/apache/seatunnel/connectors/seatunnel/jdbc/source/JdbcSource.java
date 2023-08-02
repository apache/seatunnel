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

package org.apache.seatunnel.connectors.seatunnel.jdbc.source;

import org.apache.seatunnel.shade.com.typesafe.config.Config;

import org.apache.seatunnel.api.common.PrepareFailException;
import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.configuration.util.ConfigValidator;
import org.apache.seatunnel.api.serialization.Serializer;
import org.apache.seatunnel.api.source.Boundedness;
import org.apache.seatunnel.api.source.SeaTunnelSource;
import org.apache.seatunnel.api.source.SourceReader;
import org.apache.seatunnel.api.source.SourceSplitEnumerator;
import org.apache.seatunnel.api.source.SupportColumnProjection;
import org.apache.seatunnel.api.source.SupportParallelism;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.common.constants.PluginType;
import org.apache.seatunnel.connectors.seatunnel.jdbc.config.JdbcSourceConfig;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.JdbcInputFormat;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.connection.JdbcConnectionProvider;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.connection.SimpleJdbcConnectionProvider;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.JdbcDialect;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.JdbcDialectLoader;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.JdbcDialectTypeMapper;
import org.apache.seatunnel.connectors.seatunnel.jdbc.state.JdbcSourceState;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.auto.service.AutoService;
import lombok.NoArgsConstructor;

import java.sql.Connection;
import java.sql.ResultSetMetaData;
import java.util.ArrayList;

@AutoService(SeaTunnelSource.class)
@NoArgsConstructor
public class JdbcSource
        implements SeaTunnelSource<SeaTunnelRow, JdbcSourceSplit, JdbcSourceState>,
                SupportParallelism,
                SupportColumnProjection {
    protected static final Logger LOG = LoggerFactory.getLogger(JdbcSource.class);

    private JdbcSourceConfig jdbcSourceConfig;
    private SeaTunnelRowType typeInfo;

    private JdbcDialect jdbcDialect;
    private JdbcInputFormat inputFormat;
    private PartitionParameter partitionParameter;
    private JdbcConnectionProvider jdbcConnectionProvider;

    private String query;

    public JdbcSource(
            JdbcSourceConfig jdbcSourceConfig,
            SeaTunnelRowType typeInfo,
            JdbcDialect jdbcDialect,
            JdbcInputFormat inputFormat,
            PartitionParameter partitionParameter,
            JdbcConnectionProvider jdbcConnectionProvider,
            String query) {
        this.jdbcSourceConfig = jdbcSourceConfig;
        this.typeInfo = typeInfo;
        this.jdbcDialect = jdbcDialect;
        this.inputFormat = inputFormat;
        this.partitionParameter = partitionParameter;
        this.jdbcConnectionProvider = jdbcConnectionProvider;
        this.query = query;
    }

    @Override
    public String getPluginName() {
        return "Jdbc";
    }

    @Override
    public void prepare(Config pluginConfig) throws PrepareFailException {
        ReadonlyConfig config = ReadonlyConfig.fromConfig(pluginConfig);
        ConfigValidator.of(config).validate(new JdbcSourceFactory().optionRule());
        this.jdbcSourceConfig = JdbcSourceConfig.of(config);
        this.jdbcConnectionProvider =
                new SimpleJdbcConnectionProvider(jdbcSourceConfig.getJdbcConnectionConfig());
        this.query = jdbcSourceConfig.getQuery();
        this.jdbcDialect =
                JdbcDialectLoader.load(
                        jdbcSourceConfig.getJdbcConnectionConfig().getUrl(),
                        jdbcSourceConfig.getJdbcConnectionConfig().getCompatibleMode());
        try (Connection connection = jdbcConnectionProvider.getOrEstablishConnection()) {
            this.typeInfo = initTableField(connection);
            this.partitionParameter =
                    createPartitionParameter(jdbcConnectionProvider.getOrEstablishConnection());
        } catch (Exception e) {
            throw new PrepareFailException("jdbc", PluginType.SOURCE, e.toString());
        }

        if (partitionParameter != null) {
            this.query =
                    JdbcSourceFactory.obtainPartitionSql(
                            jdbcDialect, partitionParameter, jdbcSourceConfig.getQuery());
        }

        this.inputFormat =
                new JdbcInputFormat(
                        jdbcConnectionProvider,
                        jdbcDialect,
                        typeInfo,
                        query,
                        jdbcSourceConfig.getFetchSize(),
                        jdbcSourceConfig.getJdbcConnectionConfig().isAutoCommit());
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
    public SourceReader<SeaTunnelRow, JdbcSourceSplit> createReader(
            SourceReader.Context readerContext) throws Exception {
        return new JdbcSourceReader(inputFormat, readerContext);
    }

    @Override
    public Serializer<JdbcSourceSplit> getSplitSerializer() {
        return SeaTunnelSource.super.getSplitSerializer();
    }

    @Override
    public SourceSplitEnumerator<JdbcSourceSplit, JdbcSourceState> createEnumerator(
            SourceSplitEnumerator.Context<JdbcSourceSplit> enumeratorContext) throws Exception {
        return new JdbcSourceSplitEnumerator(
                enumeratorContext, jdbcSourceConfig, partitionParameter);
    }

    @Override
    public SourceSplitEnumerator<JdbcSourceSplit, JdbcSourceState> restoreEnumerator(
            SourceSplitEnumerator.Context<JdbcSourceSplit> enumeratorContext,
            JdbcSourceState checkpointState)
            throws Exception {
        return new JdbcSourceSplitEnumerator(
                enumeratorContext, jdbcSourceConfig, partitionParameter, checkpointState);
    }

    private SeaTunnelRowType initTableField(Connection conn) {
        JdbcDialectTypeMapper jdbcDialectTypeMapper = jdbcDialect.getJdbcDialectTypeMapper();
        ArrayList<SeaTunnelDataType<?>> seaTunnelDataTypes = new ArrayList<>();
        ArrayList<String> fieldNames = new ArrayList<>();
        try {
            ResultSetMetaData resultSetMetaData =
                    jdbcDialect.getResultSetMetaData(conn, jdbcSourceConfig);
            for (int i = 1; i <= resultSetMetaData.getColumnCount(); i++) {
                // Support AS syntax
                fieldNames.add(resultSetMetaData.getColumnLabel(i));
                seaTunnelDataTypes.add(jdbcDialectTypeMapper.mapping(resultSetMetaData, i));
            }
        } catch (Exception e) {
            LOG.warn("get row type info exception", e);
        }
        return new SeaTunnelRowType(
                fieldNames.toArray(new String[0]),
                seaTunnelDataTypes.toArray(new SeaTunnelDataType<?>[0]));
    }

    private PartitionParameter createPartitionParameter(Connection connection) {
        if (jdbcSourceConfig.getPartitionColumn().isPresent()) {
            String partitionColumn = jdbcSourceConfig.getPartitionColumn().get();
            SeaTunnelDataType<?> dataType =
                    JdbcSourceFactory.validationPartitionColumn(partitionColumn, typeInfo);
            return JdbcSourceFactory.createPartitionParameter(
                    jdbcSourceConfig, partitionColumn, dataType, connection);
        } else {
            LOG.info(
                    "The partition_column parameter is not configured, and the source parallelism is set to 1");
        }

        return null;
    }
}
