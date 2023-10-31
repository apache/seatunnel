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

import org.apache.seatunnel.api.table.type.DecimalType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.api.table.type.SqlType;
import org.apache.seatunnel.common.exception.CommonErrorCode;
import org.apache.seatunnel.connectors.seatunnel.jdbc.config.JdbcSourceConfig;
import org.apache.seatunnel.connectors.seatunnel.jdbc.exception.JdbcConnectorException;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.split.JdbcNumericBetweenParametersProvider;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;

import lombok.extern.slf4j.Slf4j;

import java.io.Serializable;
import java.math.BigDecimal;
import java.sql.Array;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

@Slf4j
public class FixedChunkSplitter extends ChunkSplitter {

    public FixedChunkSplitter(JdbcSourceConfig config) {
        super(config);
    }

    @Override
    protected Collection<JdbcSourceSplit> createSplits(
            JdbcSourceTable table, SeaTunnelRowType splitKey) throws SQLException {

        String splitKeyName = splitKey.getFieldNames()[0];
        SeaTunnelDataType splitKeyType = splitKey.getFieldType(0);
        if (splitKeyType instanceof DecimalType) {
            int scale = ((DecimalType) splitKeyType).getScale();
            if (scale != 0) {
                throw new JdbcConnectorException(
                        CommonErrorCode.ILLEGAL_ARGUMENT,
                        String.format(
                                "The current field is DecimalType containing decimals: %d Unable to support",
                                scale));
            }
        }
        if (SqlType.STRING.equals(splitKeyType.getSqlType())) {
            return createStringColumnSplits(table, splitKeyName, splitKeyType);
        }

        BigDecimal partitionStart = table.getPartitionStart();
        BigDecimal partitionEnd = table.getPartitionEnd();
        if (partitionStart == null || partitionEnd == null) {
            Pair<BigDecimal, BigDecimal> range = findSplitColumnRange(table, splitKeyName);
            partitionStart = range.getLeft();
            partitionEnd = range.getRight();
        }
        if (partitionStart == null || partitionEnd == null) {
            JdbcSourceSplit spilt = createSingleSplit(table);
            return Collections.singletonList(spilt);
        }

        return createNumberColumnSplits(
                table, splitKeyName, splitKeyType, partitionStart, partitionEnd);
    }

    @Override
    protected PreparedStatement createSplitStatement(JdbcSourceSplit split) throws SQLException {
        if (SqlType.STRING.equals(split.getSplitKeyType().getSqlType())) {
            return createStringColumnSplitStatement(split);
        }
        if (split.getSplitStart() == null && split.getSplitEnd() == null) {
            return createSingleSplitStatement(split);
        }

        return createNumberColumnSplitStatement(split);
    }

    private Collection<JdbcSourceSplit> createStringColumnSplits(
            JdbcSourceTable table, String splitKeyName, SeaTunnelDataType splitKeyType) {
        List<JdbcSourceSplit> splits = new ArrayList<>(table.getPartitionNumber());
        for (int i = 0; i < table.getPartitionNumber(); i++) {
            String splitQuery;
            if (StringUtils.isNotBlank(table.getQuery())) {
                splitQuery =
                        String.format(
                                "SELECT * FROM (%s) st_jdbc_splitter WHERE %s = ?",
                                table.getQuery(),
                                jdbcDialect.hashModForField(
                                        splitKeyName, table.getPartitionNumber()));
            } else {
                splitQuery =
                        String.format(
                                "SELECT * FROM %s WHERE %s = ?",
                                jdbcDialect.tableIdentifier(table.getTablePath()),
                                jdbcDialect.hashModForField(
                                        splitKeyName, table.getPartitionNumber()));
            }

            JdbcSourceSplit split =
                    new JdbcSourceSplit(
                            table.getTablePath(),
                            createSplitId(table.getTablePath(), i),
                            splitQuery,
                            splitKeyName,
                            splitKeyType,
                            i,
                            null);
            splits.add(split);
        }
        return splits;
    }

    private PreparedStatement createStringColumnSplitStatement(JdbcSourceSplit split)
            throws SQLException {
        PreparedStatement statement = createPreparedStatement(split.getSplitQuery());
        statement.setInt(1, (Integer) split.getSplitStart());
        return statement;
    }

    private Collection<JdbcSourceSplit> createNumberColumnSplits(
            JdbcSourceTable table,
            String splitKeyName,
            SeaTunnelDataType splitKeyType,
            BigDecimal partitionStart,
            BigDecimal partitionEnd) {
        JdbcNumericBetweenParametersProvider jdbcNumericBetweenParametersProvider =
                new JdbcNumericBetweenParametersProvider(partitionStart, partitionEnd)
                        .ofBatchNum(table.getPartitionNumber());
        Serializable[][] parameterValues =
                jdbcNumericBetweenParametersProvider.getParameterValues();
        List<JdbcSourceSplit> splits = new ArrayList<>(table.getPartitionNumber());
        for (int i = 0; i < parameterValues.length; i++) {
            JdbcSourceSplit split =
                    new JdbcSourceSplit(
                            table.getTablePath(),
                            createSplitId(table.getTablePath(), i),
                            table.getQuery(),
                            splitKeyName,
                            splitKeyType,
                            parameterValues[i][0],
                            parameterValues[i][1]);
            splits.add(split);
        }
        return splits;
    }

    private PreparedStatement createNumberColumnSplitStatement(JdbcSourceSplit split)
            throws SQLException {
        String splitQuery;
        String splitKeyName = jdbcDialect.quoteIdentifier(split.getSplitKeyName());
        if (StringUtils.isNotBlank(split.getSplitQuery())) {
            splitQuery =
                    String.format(
                            "SELECT * FROM (%s) st_jdbc_splitter WHERE %s >= ? AND %s <= ?",
                            split.getSplitQuery(), splitKeyName, splitKeyName);
        } else {
            splitQuery =
                    String.format(
                            "SELECT * FROM %s WHERE %s >= ? AND %s <= ?",
                            jdbcDialect.tableIdentifier(split.getTablePath()),
                            splitKeyName,
                            splitKeyName);
        }
        PreparedStatement statement = createPreparedStatement(splitQuery);

        Object[] parameterValues = new Object[] {split.getSplitStart(), split.getSplitEnd()};
        for (int i = 0; i < parameterValues.length; i++) {
            Object param = parameterValues[i];
            if (param instanceof String) {
                statement.setString(i + 1, (String) param);
            } else if (param instanceof Long) {
                statement.setLong(i + 1, (Long) param);
            } else if (param instanceof Integer) {
                statement.setInt(i + 1, (Integer) param);
            } else if (param instanceof Double) {
                statement.setDouble(i + 1, (Double) param);
            } else if (param instanceof Boolean) {
                statement.setBoolean(i + 1, (Boolean) param);
            } else if (param instanceof Float) {
                statement.setFloat(i + 1, (Float) param);
            } else if (param instanceof BigDecimal) {
                statement.setBigDecimal(i + 1, (BigDecimal) param);
            } else if (param instanceof Byte) {
                statement.setByte(i + 1, (Byte) param);
            } else if (param instanceof Short) {
                statement.setShort(i + 1, (Short) param);
            } else if (param instanceof Date) {
                statement.setDate(i + 1, (Date) param);
            } else if (param instanceof Time) {
                statement.setTime(i + 1, (Time) param);
            } else if (param instanceof Timestamp) {
                statement.setTimestamp(i + 1, (Timestamp) param);
            } else if (param instanceof Array) {
                statement.setArray(i + 1, (Array) param);
            } else {
                // extends with other types if needed
                throw new JdbcConnectorException(
                        CommonErrorCode.UNSUPPORTED_DATA_TYPE,
                        "open() failed. Parameter "
                                + i
                                + " of type "
                                + param.getClass()
                                + " is not handled (yet).");
            }
        }

        return statement;
    }

    private Pair<BigDecimal, BigDecimal> findSplitColumnRange(
            JdbcSourceTable table, String columnName) throws SQLException {
        Pair<Object, Object> splitColumnRange = queryMinMax(table, columnName);
        Object min = splitColumnRange.getLeft();
        Object max = splitColumnRange.getRight();
        if (min != null) {
            min = convertToBigDecimal(min);
        }
        if (max != null) {
            max = convertToBigDecimal(max);
        }
        return Pair.of(((BigDecimal) min), ((BigDecimal) max));
    }

    private BigDecimal convertToBigDecimal(Object o) {
        if (o instanceof BigDecimal) {
            return (BigDecimal) o;
        } else if (o instanceof Long) {
            return BigDecimal.valueOf((Long) o);
        } else if (o instanceof Integer) {
            return BigDecimal.valueOf((Integer) o);
        } else if (o instanceof Double) {
            return BigDecimal.valueOf((Double) o);
        } else if (o instanceof Boolean) {
            return BigDecimal.valueOf((Boolean) o ? 1 : 0);
        } else if (o instanceof Float) {
            return BigDecimal.valueOf((Float) o);
        } else if (o instanceof Byte) {
            return BigDecimal.valueOf((Byte) o);
        } else if (o instanceof Short) {
            return BigDecimal.valueOf((Short) o);
        } else if (o instanceof Date) {
            return BigDecimal.valueOf(((Date) o).getTime());
        } else if (o instanceof Time) {
            return BigDecimal.valueOf(((Time) o).getTime());
        } else if (o instanceof Timestamp) {
            return BigDecimal.valueOf(((Timestamp) o).getTime());
        } else {
            throw new JdbcConnectorException(
                    CommonErrorCode.UNSUPPORTED_DATA_TYPE,
                    "convert failed. Column "
                            + o.getClass()
                            + " of type "
                            + o.getClass()
                            + " is not handled (yet).");
        }
    }
}
