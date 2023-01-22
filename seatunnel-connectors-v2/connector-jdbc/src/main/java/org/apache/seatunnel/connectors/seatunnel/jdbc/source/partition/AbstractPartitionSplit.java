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

import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.common.exception.CommonErrorCode;
import org.apache.seatunnel.connectors.seatunnel.jdbc.config.JdbcSourceOptions;
import org.apache.seatunnel.connectors.seatunnel.jdbc.exception.JdbcConnectorException;
import org.apache.seatunnel.connectors.seatunnel.jdbc.source.PartitionParameter;

import lombok.Builder;
import lombok.extern.slf4j.Slf4j;

import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

@Builder
@Slf4j
public abstract class AbstractPartitionSplit<T> implements PartitionSplit<T> {

    protected final JdbcSourceOptions jdbcSourceOptions;

    protected final SeaTunnelRowType rowType;

    protected final String query;

    public PartitionParameter<T> checkAndGetPartitionColumn() throws SQLException {

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
            return getPartitionParameter(fieldTypes, partitionColumn);
        } else {
            log.info("The partition_column parameter is not configured, and the source parallelism is set to 1");
        }
        return null;
    }

}
