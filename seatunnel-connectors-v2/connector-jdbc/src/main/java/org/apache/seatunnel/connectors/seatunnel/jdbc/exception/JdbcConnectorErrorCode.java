/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.seatunnel.connectors.seatunnel.jdbc.exception;

import org.apache.seatunnel.common.exception.SeaTunnelErrorCode;

public enum JdbcConnectorErrorCode implements SeaTunnelErrorCode {

    CREATE_DRIVER_FAILED("JDBC-01", "Fail to create driver of class"),
    NO_SUITABLE_DRIVER("JDBC-02", "No suitable driver found"),
    XA_OPERATION_FAILED("JDBC-03", "Xa operation failed, such as (commit, rollback) etc.."),
    CONNECT_DATABASE_FAILED("JDBC-04", "Connector database failed"),
    TRANSACTION_OPERATION_FAILED("JDBC-05", "transaction operation failed, such as (commit, rollback) etc.."),
    NO_SUITABLE_DIALECT_FACTORY("JDBC-06", "No suitable dialect factory found");


    private final String code;

    private final String description;

    JdbcConnectorErrorCode(String code, String description) {
        this.code = code;
        this.description = description;
    }

    @Override
    public String getCode() {
        return this.code;
    }

    @Override
    public String getDescription() {
        return this.description;
    }

}
