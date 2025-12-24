/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.seatunnel.connectors.seatunnel.hbase.exception;

import org.apache.seatunnel.common.exception.SeaTunnelErrorCode;

public enum HbaseConnectorErrorCode implements SeaTunnelErrorCode {
    CONNECTION_FAILED("Hbase-01", "Build Hbase connection failed"),
    CONNECTION_FAILED_FOR_ADMIN("Hbase-02", "Build Hbase Admin failed"),
    DATABASE_QUERY_EXCEPTION("Hbase-03", "Hbase namespace query failed"),
    TABLE_QUERY_EXCEPTION("Hbase-04", "Hbase table query failed"),
    TABLE_CREATE_EXCEPTION("Hbase-05", "Hbase table create failed"),
    TABLE_DELETE_EXCEPTION("Hbase-06", "Hbase table delete failed"),
    TABLE_EXISTS_EXCEPTION("Hbase-07", "Hbase table exists failed"),
    NAMESPACE_CREATE_EXCEPTION("Hbase-08", "Hbase namespace create failed"),
    NAMESPACE_DELETE_EXCEPTION("Hbase-09", "Hbase namespace delete failed"),
    TABLE_TRUNCATE_EXCEPTION("Hbase-10", "Hbase table truncate failed"),
    ;
    private final String code;
    private final String description;

    HbaseConnectorErrorCode(String code, String description) {
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
