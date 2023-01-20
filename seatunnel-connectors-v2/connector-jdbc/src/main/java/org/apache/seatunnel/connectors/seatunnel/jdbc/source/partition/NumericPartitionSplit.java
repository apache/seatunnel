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

package org.apache.seatunnel.connectors.seatunnel.jdbc.source.partition;

import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.common.exception.CommonErrorCode;
import org.apache.seatunnel.common.utils.SeaTunnelException;
import org.apache.seatunnel.connectors.seatunnel.jdbc.config.JdbcSourceOptions;
import org.apache.seatunnel.connectors.seatunnel.jdbc.exception.JdbcConnectorException;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.connection.JdbcConnectionProvider;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.split.JdbcNumericBetweenParametersProvider;
import org.apache.seatunnel.connectors.seatunnel.jdbc.source.JdbcSourceSplit;
import org.apache.seatunnel.connectors.seatunnel.jdbc.source.PartitionParameter;

import lombok.Builder;
import lombok.extern.slf4j.Slf4j;

import java.io.Serializable;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

@Builder
@Slf4j
public class NumericPartitionSplit implements PartitionSplit<Long> {

    private final JdbcSourceOptions jdbcSourceOptions;

    private final String columnName;

    private String query;

    private final SeaTunnelRowType rowType;

    private final JdbcConnectionProvider jdbcConnectionProvider;

    @Override
    public boolean checkType(SeaTunnelDataType<?> type) {
        return type.equals(BasicType.INT_TYPE) || type.equals(BasicType.LONG_TYPE);
    }

    @Override
    public PartitionParameter<Long> getPartitionParameter() throws SQLException {
        if (jdbcSourceOptions.getPartitionColumn().isPresent()) {
            String partitionColumn = jdbcSourceOptions.getPartitionColumn().get();
            Map<String, SeaTunnelDataType<?>> fieldTypes = new HashMap<>();
            for (int i = 0; i < rowType.getFieldNames().length; i++) {
                fieldTypes.put(rowType.getFieldName(i), rowType.getFieldType(i));
            }
            if (!fieldTypes.containsKey(partitionColumn)) {
                throw new JdbcConnectorException(CommonErrorCode.ILLEGAL_ARGUMENT,
                    String.format("field %s not contain in query %s",
                        partitionColumn, query));
            }
            if (!checkType(fieldTypes.get(partitionColumn))) {
                throw new JdbcConnectorException(CommonErrorCode.ILLEGAL_ARGUMENT,
                    String.format("%s is not numeric type", partitionColumn));
            }
            long max = Long.MAX_VALUE;
            long min = Long.MIN_VALUE;
            if (jdbcSourceOptions.getPartitionLowerBound().isPresent() &&
                jdbcSourceOptions.getPartitionUpperBound().isPresent()) {
                max = jdbcSourceOptions.getPartitionUpperBound().get();
                min = jdbcSourceOptions.getPartitionLowerBound().get();
                return new PartitionParameter<>(columnName, min, max, jdbcSourceOptions.getPartitionNumber().orElse(null));
            }
            try (ResultSet rs = jdbcConnectionProvider.getOrEstablishConnection().createStatement().executeQuery(String.format("SELECT MAX(%s),MIN(%s) " +
                "FROM (%s) tt", columnName, columnName, query))) {
                if (rs.next()) {
                    max = jdbcSourceOptions.getPartitionUpperBound().isPresent() ?
                        jdbcSourceOptions.getPartitionUpperBound().get() :
                        Long.parseLong(rs.getString(1));
                    min = jdbcSourceOptions.getPartitionLowerBound().isPresent() ?
                        jdbcSourceOptions.getPartitionLowerBound().get() :
                        Long.parseLong(rs.getString(2));
                }
            } catch (ClassNotFoundException e) {
                throw new SeaTunnelException(e);
            }


            return new PartitionParameter<>(columnName, min, max, jdbcSourceOptions.getPartitionNumber().orElse(null));
        } else {
            log.info("The partition_column parameter is not configured, and the source parallelism is set to 1");
        }
        return null;

    }

    @Override
    public Set<JdbcSourceSplit> getSplit() throws SQLException {
        Set<JdbcSourceSplit> allSplit = new HashSet<>();
        log.info("Starting to calculate splits.");
        PartitionParameter<Long> partitionParameter = getPartitionParameter();
        if (null != partitionParameter) {
            JdbcNumericBetweenParametersProvider jdbcNumericBetweenParametersProvider =
                new JdbcNumericBetweenParametersProvider(partitionParameter.getMinValue(), partitionParameter.getMaxValue())
                    .ofBatchNum(partitionParameter.getPartitionNumber());
            Serializable[][] parameterValues = jdbcNumericBetweenParametersProvider.getParameterValues();
            for (int i = 0; i < parameterValues.length; i++) {
                allSplit.add(new JdbcSourceSplit(parameterValues[i], i));
            }
        } else {
            allSplit.add(new JdbcSourceSplit(null, 0));
        }
        return allSplit;

    }

}
