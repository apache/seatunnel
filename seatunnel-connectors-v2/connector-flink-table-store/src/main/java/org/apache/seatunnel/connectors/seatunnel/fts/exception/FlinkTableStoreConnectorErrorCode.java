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

package org.apache.seatunnel.connectors.seatunnel.fts.exception;

import org.apache.seatunnel.common.exception.SeaTunnelErrorCode;

public enum FlinkTableStoreConnectorErrorCode implements SeaTunnelErrorCode {
    TABLE_WRITE_COMMIT_FAILED("FTS-01", "Flink table store write commit failed"),
    TABLE_WRITE_RECORD_FAILED("FTS-02", "Record write to flink table store failed"),
    TABLE_PRE_COMMIT_FAILED("FTS-03", "Flink table store pre commit failed");

    private final String code;
    private final String description;

    FlinkTableStoreConnectorErrorCode(String code, String description) {
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
