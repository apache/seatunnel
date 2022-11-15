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

package org.apache.seatunnel.connectors.seatunnel.cassandra.exception;

import org.apache.seatunnel.common.exception.SeaTunnelErrorCode;

public enum CassandraConnectorErrorCode implements SeaTunnelErrorCode {
    FIELD_NOT_IN_TABLE("CASSANDRA-01", "Field is not existed in target table"),
    ADD_BATCH_DATA_FAILED("CASSANDRA-02", "Add batch SeaTunnelRow data into a batch failed"),
    CLOSE_CQL_SESSION_FAILED("CASSANDRA-03", "Close cql session of cassandra failed"),
    NO_DATA_IN_SOURCE_TABLE("CASSANDRA-04", "No data in source table"),
    PARSE_IP_ADDRESS_FAILED("CASSANDRA-05", "Parse ip address from string field");

    private final String code;
    private final String description;

    CassandraConnectorErrorCode(String code, String description) {
        this.code = code;
        this.description = description;
    }

    @Override
    public String getCode() {
        return code;
    }

    @Override
    public String getDescription() {
        return description;
    }
}
