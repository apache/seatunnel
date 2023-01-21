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
import org.apache.seatunnel.connectors.seatunnel.jdbc.source.JdbcSourceSplit;
import org.apache.seatunnel.connectors.seatunnel.jdbc.source.PartitionParameter;

import lombok.Builder;
import lombok.extern.slf4j.Slf4j;

import java.sql.SQLException;
import java.util.Set;

@Builder
@Slf4j
public class StrPrefixNumericPartitionSplit implements PartitionSplit<String> {
    @Override
    public boolean checkType(SeaTunnelDataType<?> type) {
        return false;
    }

    @Override
    public PartitionParameter<String> getPartitionParameter() throws SQLException {
        return null;
    }

    @Override
    public Set<JdbcSourceSplit> getSplit(int currentParallelism) throws SQLException {
        return null;
    }
}
