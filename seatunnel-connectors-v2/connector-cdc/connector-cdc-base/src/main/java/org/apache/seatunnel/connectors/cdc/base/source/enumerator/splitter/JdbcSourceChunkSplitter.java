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

package org.apache.seatunnel.connectors.cdc.base.source.enumerator.splitter;

import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.connectors.cdc.base.source.split.SnapshotSplit;

import io.debezium.jdbc.JdbcConnection;
import io.debezium.relational.Column;
import io.debezium.relational.Table;
import io.debezium.relational.TableId;

import java.sql.SQLException;
import java.util.Collection;

/** The {@code ChunkSplitter} used to split table into a set of chunks for JDBC data source. */
public interface JdbcSourceChunkSplitter extends ChunkSplitter {

    /** Generates all snapshot splits (chunks) for the give table path. */
    @Override
    Collection<SnapshotSplit> generateSplits(TableId tableId);

    /** @deprecated instead by {@link this#queryMinMax(JdbcConnection, TableId, Column)} */
    @Deprecated
    Object[] queryMinMax(JdbcConnection jdbc, TableId tableId, String columnName)
            throws SQLException;

    /**
     * Query the maximum and minimum value of the column in the table. e.g. query string <code>
     * SELECT MIN(%s) FROM %s WHERE %s > ?</code>
     *
     * @param jdbc JDBC connection.
     * @param tableId table identity.
     * @param column column.
     * @return maximum and minimum value.
     */
    default Object[] queryMinMax(JdbcConnection jdbc, TableId tableId, Column column)
            throws SQLException {
        return queryMinMax(jdbc, tableId, column.name());
    }

    /** @deprecated instead by {@link this#queryMin(JdbcConnection, TableId, Column, Object)} */
    @Deprecated
    Object queryMin(
            JdbcConnection jdbc, TableId tableId, String columnName, Object excludedLowerBound)
            throws SQLException;

    /**
     * Query the minimum value of the column in the table, and the minimum value must greater than
     * the excludedLowerBound value. e.g. prepare query string <code>
     * SELECT MIN(%s) FROM %s WHERE %s > ?</code>
     *
     * @param jdbc JDBC connection.
     * @param tableId table identity.
     * @param column column.
     * @param excludedLowerBound the minimum value should be greater than this value.
     * @return minimum value.
     */
    default Object queryMin(
            JdbcConnection jdbc, TableId tableId, Column column, Object excludedLowerBound)
            throws SQLException {
        return queryMin(jdbc, tableId, column.name(), excludedLowerBound);
    }

    @Deprecated
    Object[] sampleDataFromColumn(
            JdbcConnection jdbc, TableId tableId, String columnName, int samplingRate)
            throws SQLException;

    /**
     * Performs a sampling operation on the specified column of a table in a JDBC-connected
     * database.
     *
     * @param jdbc The JDBC connection object used to connect to the database.
     * @param tableId The ID of the table in which the column resides.
     * @param column The column to be sampled.
     * @param samplingRate samplingRate The inverse of the fraction of the data to be sampled from
     *     the column. For example, a value of 1000 would mean 1/1000 of the data will be sampled.
     * @return Returns a List of sampled data from the specified column.
     * @throws SQLException If an SQL error occurs during the sampling operation.
     */
    default Object[] sampleDataFromColumn(
            JdbcConnection jdbc, TableId tableId, Column column, int samplingRate)
            throws SQLException {
        return sampleDataFromColumn(jdbc, tableId, column.name(), samplingRate);
    }

    /**
     * @deprecated instead by {@link this#queryNextChunkMax(JdbcConnection, TableId, Column, int,
     *     Object)}
     */
    @Deprecated
    Object queryNextChunkMax(
            JdbcConnection jdbc,
            TableId tableId,
            String columnName,
            int chunkSize,
            Object includedLowerBound)
            throws SQLException;

    /**
     * Query the maximum value of the next chunk, and the next chunk must be greater than or equal
     * to <code>includedLowerBound</code> value [min_1, max_1), [min_2, max_2),... [min_n, null).
     * Each time this method is called it will return max1, max2...
     *
     * @param jdbc JDBC connection.
     * @param tableId table identity.
     * @param column column.
     * @param chunkSize chunk size.
     * @param includedLowerBound the previous chunk end value.
     * @return next chunk end value.
     */
    default Object queryNextChunkMax(
            JdbcConnection jdbc,
            TableId tableId,
            Column column,
            int chunkSize,
            Object includedLowerBound)
            throws SQLException {
        return queryNextChunkMax(jdbc, tableId, column.name(), chunkSize, includedLowerBound);
    }

    /**
     * Approximate total number of entries in the lookup table.
     *
     * @param jdbc JDBC connection.
     * @param tableId table identity.
     * @return approximate row count.
     */
    Long queryApproximateRowCnt(JdbcConnection jdbc, TableId tableId) throws SQLException;

    /**
     * Build the scan query sql of the {@link SnapshotSplit}.
     *
     * @param table table.
     * @param splitKeyType primary key type.
     * @param isFirstSplit whether the first split.
     * @param isLastSplit whether the last split.
     * @return query sql.
     */
    String buildSplitScanQuery(
            Table table, SeaTunnelRowType splitKeyType, boolean isFirstSplit, boolean isLastSplit);

    /**
     * Checks whether split column is evenly distributed across its range.
     *
     * @param splitColumn split column.
     * @return true that means split column with type BIGINT, INT, DECIMAL.
     */
    default boolean isEvenlySplitColumn(Column splitColumn) {
        // currently, we only support these types.
        switch (fromDbzColumn(splitColumn).getSqlType()) {
            case TINYINT:
            case SMALLINT:
            case INT:
            case BIGINT:
            case DECIMAL:
            case STRING:
                return true;
            default:
                return false;
        }
    }

    /**
     * Get a corresponding SeaTunnel data type from a debezium {@link Column}.
     *
     * @param splitColumn dbz split column.
     * @return SeaTunnel data type
     */
    SeaTunnelDataType<?> fromDbzColumn(Column splitColumn);

    /**
     * convert dbz column to SeaTunnel row type.
     *
     * @param splitColumn split column.
     * @return SeaTunnel row type.
     */
    default SeaTunnelRowType getSplitType(Column splitColumn) {
        return new SeaTunnelRowType(
                new String[] {splitColumn.name()},
                new SeaTunnelDataType[] {fromDbzColumn(splitColumn)});
    }
}
