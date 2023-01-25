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

import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.connectors.seatunnel.jdbc.config.JdbcSourceOptions;
import org.apache.seatunnel.connectors.seatunnel.jdbc.exception.JdbcConnectorErrorCode;
import org.apache.seatunnel.connectors.seatunnel.jdbc.exception.JdbcConnectorException;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.connection.JdbcConnectionProvider;

public class PartitionSplitFactory {

    public static PartitionSplit<?> getPartitionSplit(JdbcSourceOptions jdbcSourceOptions,
                                                      SeaTunnelRowType rowType,
                                                      JdbcConnectionProvider jdbcConnectionProvider) {
        switch (jdbcSourceOptions.getSplitType()) {
            case NumericType:
                return new NumericPartitionSplit(jdbcConnectionProvider, jdbcSourceOptions, rowType);
            case DateType:
                return new DatePartitionSplit(jdbcConnectionProvider, jdbcSourceOptions, rowType);
            case StrNumericType:
                return StrNumericPartitionSplit.builder().build();
            case StrDateType:
                return new StrDatePartitionSplit(jdbcConnectionProvider, jdbcSourceOptions, rowType);
            case StrPrefixNumericType:
                return StrPrefixNumericPartitionSplit.builder().build();
            case StrPrefixDateType:
                return StrPrefixDatePartitionSplit.builder().build();
            default:
                throw new JdbcConnectorException(JdbcConnectorErrorCode.NO_SUITABLE_PARTITION_SPLIT,
                    JdbcConnectorErrorCode.NO_SUITABLE_PARTITION_SPLIT.getErrorMessage());
        }
    }
}
