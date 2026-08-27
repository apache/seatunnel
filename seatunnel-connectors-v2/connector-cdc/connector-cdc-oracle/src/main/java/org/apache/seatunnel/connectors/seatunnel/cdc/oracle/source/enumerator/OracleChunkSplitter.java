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

package org.apache.seatunnel.connectors.seatunnel.cdc.oracle.source.enumerator;

import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.connectors.cdc.base.config.JdbcSourceConfig;
import org.apache.seatunnel.connectors.cdc.base.dialect.JdbcDataSourceDialect;
import org.apache.seatunnel.connectors.cdc.base.source.enumerator.splitter.AbstractJdbcSourceChunkSplitter;
import org.apache.seatunnel.connectors.cdc.base.utils.ObjectUtils;
import org.apache.seatunnel.connectors.seatunnel.cdc.oracle.config.OracleSourceConfig;
import org.apache.seatunnel.connectors.seatunnel.cdc.oracle.utils.OracleTypeUtils;
import org.apache.seatunnel.connectors.seatunnel.cdc.oracle.utils.OracleUtils;

import io.debezium.jdbc.JdbcConnection;
import io.debezium.relational.Column;
import io.debezium.relational.Table;
import io.debezium.relational.TableId;
import lombok.extern.slf4j.Slf4j;
import oracle.sql.ROWID;

import java.sql.SQLException;

/**
 * The {@code ChunkSplitter} used to split Oracle table into a set of chunks for JDBC data source.
 */
@Slf4j
public class OracleChunkSplitter extends AbstractJdbcSourceChunkSplitter {

    private final OracleSourceConfig oracleSourceConfig;

    public OracleChunkSplitter(JdbcSourceConfig sourceConfig, JdbcDataSourceDialect dialect) {
        super(sourceConfig, dialect);
        this.oracleSourceConfig = (OracleSourceConfig) sourceConfig;
    }

    @Override
    public Object[] queryMinMax(JdbcConnection jdbc, TableId tableId, String columnName)
            throws SQLException {
        return OracleUtils.queryMinMax(jdbc, tableId, columnName);
    }

    @Override
    public Object queryMin(
            JdbcConnection jdbc, TableId tableId, String columnName, Object excludedLowerBound)
            throws SQLException {
        return OracleUtils.queryMin(jdbc, tableId, columnName, excludedLowerBound);
    }

    @Override
    public Object[] sampleDataFromColumn(
            JdbcConnection jdbc, TableId tableId, String columnName, int inverseSamplingRate)
            throws Exception {
        return OracleUtils.skipReadAndSortSampleData(
                jdbc, tableId, columnName, inverseSamplingRate);
    }

    @Override
    public Object queryNextChunkMax(
            JdbcConnection jdbc,
            TableId tableId,
            String columnName,
            int chunkSize,
            Object includedLowerBound)
            throws SQLException {
        return OracleUtils.queryNextChunkMax(
                jdbc, tableId, columnName, chunkSize, includedLowerBound);
    }

    @Override
    public Long queryApproximateRowCnt(JdbcConnection jdbc, TableId tableId) throws SQLException {
        return OracleUtils.queryApproximateRowCnt(oracleSourceConfig, jdbc, tableId);
    }

    @Override
    public String buildSplitScanQuery(
            Table table, SeaTunnelRowType splitKeyType, boolean isFirstSplit, boolean isLastSplit) {
        return OracleUtils.buildSplitScanQuery(table.id(), splitKeyType, isFirstSplit, isLastSplit);
    }

    @Override
    public SeaTunnelDataType<?> fromDbzColumn(Column splitColumn) {
        return OracleTypeUtils.convertFromColumn(splitColumn);
    }

    protected int ObjectCompare(Object obj1, Object obj2) {
        if (obj1 instanceof ROWID && obj2 instanceof ROWID) {
            return ROWID.compareBytes(((ROWID) obj1).getBytes(), ((ROWID) obj2).getBytes());
        } else {
            return ObjectUtils.compare(obj1, obj2);
        }
    }
}
