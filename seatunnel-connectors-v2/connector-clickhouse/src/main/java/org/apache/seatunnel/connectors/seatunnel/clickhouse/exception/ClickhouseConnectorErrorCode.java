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

package org.apache.seatunnel.connectors.seatunnel.clickhouse.exception;

import org.apache.seatunnel.common.exception.SeaTunnelErrorCode;

public enum ClickhouseConnectorErrorCode implements SeaTunnelErrorCode {

    FIELD_NOT_IN_TABLE("CLICKHOUSE-01", "Field is not existed in target table"),
    PASSWORD_NOT_FOUND_IN_SHARD_NODE("CLICKHOUSE-02", "Can’t find password of shard node"),
    DELETE_DIRECTORY_FIELD("CLICKHOUSE-03", "Can’t delete directory"),
    SSH_OPERATION_FAILED("CLICKHOUSE-04", "Ssh operation failed, such as (login,connect,authentication,close) etc..."),
    CLUSTER_LIST_GET_FAILED("CLICKHOUSE-05", "Get cluster list from clickhouse failed"),
    SHARD_KEY_NOT_FOUND("CLICKHOUSE-06", "Shard key not found in table"),
    FILE_NOT_EXISTS("CLICKHOUSE-07", "Clickhouse local file not exists");

    private final String code;
    private final String description;

    ClickhouseConnectorErrorCode(String code, String description) {
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
