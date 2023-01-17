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

package org.apache.seatunnel.connector.selectdb.exception;

import org.apache.seatunnel.common.exception.SeaTunnelErrorCode;

public enum SelectDBConnectorErrorCode implements SeaTunnelErrorCode {
    UPLOAD_FAILED("SelectDB-01", "upload file error"),
    COMMIT_FAILED("SelectDB-02", "commit error"),
    CLOSE_HTTP_FAILED("SelectDB-03", "Closing httpClient failed"),
    REDIRECTED_FAILED("SelectDB-04", "Get the redirected s3 address filed"),
    WHILE_LOADING_FAILED("SelectDB-05", "error while loading data");

    private final String code;
    private final String description;

    SelectDBConnectorErrorCode(String code, String description) {
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
