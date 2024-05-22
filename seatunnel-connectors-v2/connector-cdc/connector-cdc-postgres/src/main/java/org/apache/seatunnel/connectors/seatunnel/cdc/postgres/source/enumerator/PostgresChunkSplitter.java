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

package org.apache.seatunnel.connectors.seatunnel.cdc.postgres.source.enumerator;

import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.connectors.cdc.base.config.JdbcSourceConfig;
import org.apache.seatunnel.connectors.cdc.base.dialect.JdbcDataSourceDialect;
import org.apache.seatunnel.connectors.cdc.base.source.enumerator.splitter.AbstractJdbcSourceChunkSplitter;
import org.apache.seatunnel.connectors.seatunnel.cdc.postgres.utils.PostgresTypeUtils;
import org.apache.seatunnel.connectors.seatunnel.cdc.postgres.utils.PostgresUtils;

import io.debezium.jdbc.JdbcConnection;
import io.debezium.relational.Column;
import io.debezium.relational.Table;
import io.debezium.relational.TableId;
import lombok.extern.slf4j.Slf4j;

import java.sql.SQLException;

/** The {@code ChunkSplitter} used to split table into a set of chunks for JDBC data source. */
@Slf4j
public class PostgresChunkSplitter extends AbstractJdbcSourceChunkSplitter {

    public PostgresChunkSplitter(JdbcSourceConfig sourceConfig, JdbcDataSourceDialect dialect) {
        super(sourceConfig, dialect);
    }

    @Override
    public Object[] queryMinMax(JdbcConnection jdbc, TableId tableId, String columnName)
            throws SQLException {
        return PostgresUtils.queryMinMax(jdbc, tableId, columnName, null);
    }

    @Override
    public Object[] queryMinMax(JdbcConnection jdbc, TableId tableId, Column column)
            throws SQLException {
        return PostgresUtils.queryMinMax(jdbc, tableId, column.name(), column);
    }

    @Override
    public Object queryMin(
            JdbcConnection jdbc, TableId tableId, String columnName, Object excludedLowerBound)
            throws SQLException {
        return PostgresUtils.queryMin(jdbc, tableId, columnName, null, excludedLowerBound);
    }

    @Override
    public Object queryMin(
            JdbcConnection jdbc, TableId tableId, Column column, Object excludedLowerBound)
            throws SQLException {
        return PostgresUtils.queryMin(jdbc, tableId, column.name(), column, excludedLowerBound);
    }

    @Override
    public Object[] sampleDataFromColumn(
            JdbcConnection jdbc, TableId tableId, String columnName, int inverseSamplingRate)
            throws Exception {
        return PostgresUtils.skipReadAndSortSampleData(
                jdbc, tableId, columnName, null, inverseSamplingRate);
    }

    @Override
    public Object[] sampleDataFromColumn(
            JdbcConnection jdbc, TableId tableId, Column column, int inverseSamplingRate)
            throws Exception {
        return PostgresUtils.skipReadAndSortSampleData(
                jdbc, tableId, column.name(), column, inverseSamplingRate);
    }

    @Override
    public Object queryNextChunkMax(
            JdbcConnection jdbc,
            TableId tableId,
            String columnName,
            int chunkSize,
            Object includedLowerBound)
            throws SQLException {
        return PostgresUtils.queryNextChunkMax(
                jdbc, tableId, columnName, null, chunkSize, includedLowerBound);
    }

    @Override
    public Object queryNextChunkMax(
            JdbcConnection jdbc,
            TableId tableId,
            Column column,
            int chunkSize,
            Object includedLowerBound)
            throws SQLException {
        return PostgresUtils.queryNextChunkMax(
                jdbc, tableId, column.name(), column, chunkSize, includedLowerBound);
    }

    @Override
    public Long queryApproximateRowCnt(JdbcConnection jdbc, TableId tableId) throws SQLException {
        return PostgresUtils.queryApproximateRowCnt(jdbc, tableId);
    }

    @Override
    public String buildSplitScanQuery(
            Table table, SeaTunnelRowType splitKeyType, boolean isFirstSplit, boolean isLastSplit) {
        return PostgresUtils.buildSplitScanQuery(table, splitKeyType, isFirstSplit, isLastSplit);
    }

    @Override
    public SeaTunnelDataType<?> fromDbzColumn(Column splitColumn) {
        return PostgresTypeUtils.convertFromColumn(splitColumn);
    }
}
