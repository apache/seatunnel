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

package org.apache.seatunnel.connectors.seatunnel.clickhouse.sink.client;

import org.apache.seatunnel.connectors.seatunnel.clickhouse.tool.IntHolder;

import com.clickhouse.jdbc.internal.ClickHouseConnectionImpl;

import java.sql.PreparedStatement;

public class ClickhouseBatchStatement {

    private final ClickHouseConnectionImpl clickHouseConnection;
    private final PreparedStatement preparedStatement;
    private final IntHolder intHolder;

    public ClickhouseBatchStatement(ClickHouseConnectionImpl clickHouseConnection,
                                    PreparedStatement preparedStatement,
                                    IntHolder intHolder) {
        this.clickHouseConnection = clickHouseConnection;
        this.preparedStatement = preparedStatement;
        this.intHolder = intHolder;
    }

    public ClickHouseConnectionImpl getClickHouseConnection() {
        return clickHouseConnection;
    }

    public PreparedStatement getPreparedStatement() {
        return preparedStatement;
    }

    public IntHolder getIntHolder() {
        return intHolder;
    }

}
