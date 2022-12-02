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

import org.apache.seatunnel.api.common.PrepareFailException;
import org.apache.seatunnel.api.serialization.Serializer;
import org.apache.seatunnel.api.source.Boundedness;
import org.apache.seatunnel.api.source.SeaTunnelSource;
import org.apache.seatunnel.api.source.SourceReader;
import org.apache.seatunnel.api.source.SourceSplitEnumerator;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.common.constants.PluginType;
import org.apache.seatunnel.connectors.seatunnel.jdbc.config.JdbcSourceOptions;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.JdbcInputFormat;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.connection.JdbcConnectionProvider;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.connection.SimpleJdbcConnectionProvider;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.JdbcDialect;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.JdbcDialectLoader;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.JdbcDialectTypeMapper;
import org.apache.seatunnel.connectors.seatunnel.jdbc.state.JdbcSourceState;

import org.apache.seatunnel.shade.com.typesafe.config.Config;

import com.google.auto.service.AutoService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

@AutoService(SeaTunnelSource.class)
public class JdbcSource implements SeaTunnelSource<SeaTunnelRow, JdbcSourceSplit, JdbcSourceState> {
    protected static final Logger LOG = LoggerFactory.getLogger(JdbcSource.class);

    private JdbcSourceOptions jdbcSourceOptions;
    private SeaTunnelRowType typeInfo;

    private JdbcDialect jdbcDialect;
    private JdbcInputFormat inputFormat;
    private PartitionParameter partitionParameter;
    private JdbcConnectionProvider jdbcConnectionProvider;

    private String query;

    @Override
    public String getPluginName() {
        return "Jdbc";
    }

    @Override
    public void prepare(Config pluginConfig) throws PrepareFailException {
        jdbcSourceOptions = new JdbcSourceOptions(pluginConfig);
        jdbcConnectionProvider = new SimpleJdbcConnectionProvider(jdbcSourceOptions.getJdbcConnectionOptions());
        query = jdbcSourceOptions.getQuery();
        jdbcDialect = JdbcDialectLoader.load(jdbcSourceOptions.getJdbcConnectionOptions().getUrl());
        try (Connection connection = jdbcConnectionProvider.getOrEstablishConnection()) {
            typeInfo = initTableField(connection);
            partitionParameter = initPartitionParameterAndExtendSql(jdbcConnectionProvider.getOrEstablishConnection());
        } catch (Exception e) {
            throw new PrepareFailException("jdbc", PluginType.SOURCE, e.toString());
        }

        inputFormat = new JdbcInputFormat(
            jdbcConnectionProvider,
            jdbcDialect,
            typeInfo,
            query,
            jdbcSourceOptions.getFetchSize(),
            jdbcSourceOptions.getJdbcConnectionOptions().isAutoCommit()
        );
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
    public SourceReader<SeaTunnelRow, JdbcSourceSplit> createReader(SourceReader.Context readerContext) throws Exception {
        return new JdbcSourceReader(inputFormat, readerContext);
    }

    @Override
    public Serializer<JdbcSourceSplit> getSplitSerializer() {
        return SeaTunnelSource.super.getSplitSerializer();
    }

    @Override
    public SourceSplitEnumerator<JdbcSourceSplit, JdbcSourceState> createEnumerator(SourceSplitEnumerator.Context<JdbcSourceSplit> enumeratorContext) throws Exception {
        return new JdbcSourceSplitEnumerator(enumeratorContext, jdbcSourceOptions, partitionParameter);
    }

    @Override
    public SourceSplitEnumerator<JdbcSourceSplit, JdbcSourceState> restoreEnumerator(SourceSplitEnumerator.Context<JdbcSourceSplit> enumeratorContext, JdbcSourceState checkpointState) throws Exception {
        return new JdbcSourceSplitEnumerator(enumeratorContext, jdbcSourceOptions, partitionParameter);
    }

    private SeaTunnelRowType initTableField(Connection conn) {
        JdbcDialectTypeMapper jdbcDialectTypeMapper = jdbcDialect.getJdbcDialectTypeMapper();
        ArrayList<SeaTunnelDataType<?>> seaTunnelDataTypes = new ArrayList<>();
        ArrayList<String> fieldNames = new ArrayList<>();
        try {
            ResultSetMetaData resultSetMetaData = jdbcDialect.getResultSetMetaData(conn, jdbcSourceOptions);
            for (int i = 1; i <= resultSetMetaData.getColumnCount(); i++) {
                fieldNames.add(resultSetMetaData.getColumnName(i));
                seaTunnelDataTypes.add(jdbcDialectTypeMapper.mapping(resultSetMetaData, i));
            }
        } catch (Exception e) {
            LOG.warn("get row type info exception", e);
        }
        return new SeaTunnelRowType(fieldNames.toArray(new String[0]), seaTunnelDataTypes.toArray(new SeaTunnelDataType<?>[0]));
    }

    private PartitionParameter initPartitionParameter(String columnName, Connection connection) throws SQLException {
        long max = Long.MAX_VALUE;
        long min = Long.MIN_VALUE;
        if (jdbcSourceOptions.getPartitionLowerBound().isPresent() && jdbcSourceOptions.getPartitionUpperBound().isPresent()) {
            max = jdbcSourceOptions.getPartitionUpperBound().get();
            min = jdbcSourceOptions.getPartitionLowerBound().get();
            return new PartitionParameter(columnName, min, max, jdbcSourceOptions.getPartitionNumber().orElse(null));
        }
        try (ResultSet rs = connection.createStatement().executeQuery(String.format("SELECT MAX(%s),MIN(%s) " +
            "FROM (%s) tt", columnName, columnName, query))) {
            if (rs.next()) {
                max = jdbcSourceOptions.getPartitionUpperBound().isPresent() ? jdbcSourceOptions.getPartitionUpperBound().get() :
                    Long.parseLong(rs.getString(1));
                min = jdbcSourceOptions.getPartitionLowerBound().isPresent() ? jdbcSourceOptions.getPartitionLowerBound().get() :
                    Long.parseLong(rs.getString(2));
            }
        }
        return new PartitionParameter(columnName, min, max, jdbcSourceOptions.getPartitionNumber().orElse(null));
    }

    private PartitionParameter initPartitionParameterAndExtendSql(Connection connection) throws SQLException {
        if (jdbcSourceOptions.getPartitionColumn().isPresent()) {
            String partitionColumn = jdbcSourceOptions.getPartitionColumn().get();
            Map<String, SeaTunnelDataType<?>> fieldTypes = new HashMap<>();
            for (int i = 0; i < typeInfo.getFieldNames().length; i++) {
                fieldTypes.put(typeInfo.getFieldName(i), typeInfo.getFieldType(i));
            }
            if (!fieldTypes.containsKey(partitionColumn)) {
                throw new IllegalArgumentException(String.format("field %s not contain in query %s",
                    partitionColumn, query));
            }
            SeaTunnelDataType<?> partitionColumnType = fieldTypes.get(partitionColumn);
            if (!isNumericType(partitionColumnType)) {
                throw new IllegalArgumentException(String.format("%s is not numeric type", partitionColumn));
            }
            PartitionParameter partitionParameter = initPartitionParameter(partitionColumn, connection);
            query = String.format("SELECT * FROM (%s) tt where " + partitionColumn + " >= ? AND " + partitionColumn + " <= ?", query);

            return partitionParameter;
        } else {
            LOG.info("The partition_column parameter is not configured, and the source parallelism is set to 1");
        }

        return null;
    }

    private boolean isNumericType(SeaTunnelDataType<?> type) {
        return type.equals(BasicType.INT_TYPE) || type.equals(BasicType.LONG_TYPE);
    }

}
