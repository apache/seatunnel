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

package org.apache.seatunnel.connectors.bigquery.exception;

import org.apache.seatunnel.common.exception.SeaTunnelErrorCode;

public enum BigQueryConnectorErrorCode implements SeaTunnelErrorCode {
    WRITER_CREATE_FAILED("Bigquery-01", "create writer failed"),
    APPEND_ROWS_FAILED("Bigquery-02", "append rows to bigquery failed"),
    COMMIT_FAILED("Bigquery-03", "commit stream write failed"),
    STREAM_FINALIZE_FAILED("Bigquery-04", "finalize stream write failed"),
    CLIENT_CREATE_FAILED("Bigquery-05", "create bigquery client failed"),
    BAD_CREDENTIALS("Bigquery-06", "bad credentials for bigquery client"),
    TABLE_NOT_FOUND("Bigquery-07", "the specified table is not found in bigquery"),
    ;

    private final String code;
    private final String description;

    BigQueryConnectorErrorCode(String code, String description) {
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
