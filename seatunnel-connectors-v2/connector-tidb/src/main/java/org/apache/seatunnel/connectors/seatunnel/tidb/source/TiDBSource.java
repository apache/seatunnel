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

package org.apache.seatunnel.connectors.seatunnel.tidb.source;

import org.apache.seatunnel.api.common.PrepareFailException;
import org.apache.seatunnel.api.source.Boundedness;
import org.apache.seatunnel.api.source.SeaTunnelSource;
import org.apache.seatunnel.api.source.SourceReader;
import org.apache.seatunnel.api.source.SourceSplitEnumerator;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.common.constants.PluginType;
import org.apache.seatunnel.connectors.seatunnel.tidb.config.PartitionParameter;
import org.apache.seatunnel.connectors.seatunnel.tidb.config.TiDBSourceOptions;
import org.apache.seatunnel.connectors.seatunnel.tidb.connection.JdbcConnectionProvider;
import org.apache.seatunnel.connectors.seatunnel.tidb.connection.JdbcInputFormat;
import org.apache.seatunnel.connectors.seatunnel.tidb.connection.SimpleJdbcConnectionProvider;
import org.apache.seatunnel.connectors.seatunnel.tidb.converter.JdbcDialectTypeMapper;
import org.apache.seatunnel.connectors.seatunnel.tidb.converter.MySqlTypeMapper;
import org.apache.seatunnel.connectors.seatunnel.tidb.converter.MysqlJdbcRowConverter;
import org.apache.seatunnel.connectors.seatunnel.tidb.state.TiDBSourceState;

import org.apache.seatunnel.shade.com.typesafe.config.Config;

import com.google.auto.service.AutoService;
import com.google.common.collect.Lists;
import lombok.extern.slf4j.Slf4j;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

@AutoService(SeaTunnelSource.class)
@Slf4j
public class TiDBSource implements SeaTunnelSource<SeaTunnelRow, TiDBSourceSplit, TiDBSourceState> {
    private TiDBSourceOptions tidbSourceOptions;
    private JdbcConnectionProvider connectionProvider;
    private JdbcInputFormat inputFormat;

    private SeaTunnelRowType typeInfo;
    private PartitionParameter partitionParameter;
    private String query;

    @Override
    public String getPluginName() {
        return "TiDB";
    }

    @Override
    public void prepare(Config pluginConfig) throws PrepareFailException {
        tidbSourceOptions = new TiDBSourceOptions(pluginConfig);
        connectionProvider = new SimpleJdbcConnectionProvider(tidbSourceOptions.getJdbcConnectionOptions());
        try {
            //todo query/typeinfo/partition all of these should be initialized in READER , but HERE, I will change later
            query = tidbSourceOptions.getJdbcConnectionOptions().getQuery();
            typeInfo = initTableField(connectionProvider.getOrEstablishConnection());
            partitionParameter = initPartitionParameterAndExtendSql(connectionProvider.getOrEstablishConnection());
        } catch (Exception e) {
            throw new PrepareFailException(getPluginName(), PluginType.SOURCE, e.toString());
        }

        inputFormat = new JdbcInputFormat(
            connectionProvider,
            new MysqlJdbcRowConverter(),
            typeInfo,
            query,
            0,
            true
        );
    }

    @Override
    public Boundedness getBoundedness() {
        return Boundedness.BOUNDED;
    }

    @Override
    public SeaTunnelDataType<SeaTunnelRow> getProducedType() {
        Connection conn;
        try {
            conn = connectionProvider.getOrEstablishConnection();
            this.typeInfo = initTableField(conn);
        } catch (Exception e) {
            log.warn("get row type info exception", e);
        }
        return this.typeInfo;
    }

    @Override
    public SourceReader<SeaTunnelRow, TiDBSourceSplit> createReader(SourceReader.Context readerContext) throws Exception {
        return new TiDBSourceReader(inputFormat, readerContext);
    }

    @Override
    public SourceSplitEnumerator<TiDBSourceSplit, TiDBSourceState> createEnumerator(SourceSplitEnumerator.Context<TiDBSourceSplit> enumeratorContext) throws Exception {
        return new TiDBSourceSplitEnumerator(enumeratorContext, tidbSourceOptions, partitionParameter);
    }

    @Override
    public SourceSplitEnumerator<TiDBSourceSplit, TiDBSourceState> restoreEnumerator(SourceSplitEnumerator.Context<TiDBSourceSplit> enumeratorContext, TiDBSourceState checkpointState) throws Exception {
        return new TiDBSourceSplitEnumerator(enumeratorContext, tidbSourceOptions, partitionParameter);
    }

    private SeaTunnelRowType initTableField(Connection conn) {
        JdbcDialectTypeMapper typeMapper = new MySqlTypeMapper();
        try {
            PreparedStatement ps = conn.prepareStatement(tidbSourceOptions.getJdbcConnectionOptions().getQuery());
            ResultSetMetaData resultSetMetaData = ps.getMetaData();
            ArrayList<SeaTunnelDataType<?>> seaTunnelDataTypes = Lists.newArrayListWithExpectedSize(resultSetMetaData.getColumnCount());
            ArrayList<String> fieldNames = Lists.newArrayListWithExpectedSize(resultSetMetaData.getColumnCount());
            for (int i = 1; i <= resultSetMetaData.getColumnCount(); i++) {
                fieldNames.add(resultSetMetaData.getColumnName(i));
                seaTunnelDataTypes.add(typeMapper.mapping(resultSetMetaData, i));
            }
            return new SeaTunnelRowType(fieldNames.toArray(new String[0]), seaTunnelDataTypes.toArray(new SeaTunnelDataType<?>[0]));
        } catch (Exception e) {
            log.warn("get row type info exception", e);
        }
        return null;
    }

    private PartitionParameter initPartitionParameterAndExtendSql(Connection connection) throws SQLException {
        if (tidbSourceOptions.getPartitionColumn().isPresent()) {
            String partitionColumn = tidbSourceOptions.getPartitionColumn().get();
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
            log.info("The partition_column parameter is not configured, and the source parallelism is set to 1");
        }

        return null;
    }

    private PartitionParameter initPartitionParameter(String columnName, Connection connection) throws SQLException {
        long max = Long.MAX_VALUE;
        long min = Long.MIN_VALUE;
        if (tidbSourceOptions.getPartitionLowerBound().isPresent() && tidbSourceOptions.getPartitionUpperBound().isPresent()) {
            max = tidbSourceOptions.getPartitionUpperBound().get();
            min = tidbSourceOptions.getPartitionLowerBound().get();
            return new PartitionParameter(columnName, min, max);
        }
        try (ResultSet rs = connection.createStatement().executeQuery(String.format("SELECT MAX(%s),MIN(%s) " +
            "FROM (%s) tt", columnName, columnName, query))) {
            if (rs.next()) {
                max = tidbSourceOptions.getPartitionUpperBound().isPresent() ? tidbSourceOptions.getPartitionUpperBound().get() :
                    Long.parseLong(rs.getString(1));
                min = tidbSourceOptions.getPartitionLowerBound().isPresent() ? tidbSourceOptions.getPartitionLowerBound().get() :
                    Long.parseLong(rs.getString(2));
            }
        }
        return new PartitionParameter(columnName, min, max);
    }

    private boolean isNumericType(SeaTunnelDataType<?> type) {
        return type.equals(BasicType.INT_TYPE) || type.equals(BasicType.LONG_TYPE);
    }

}
